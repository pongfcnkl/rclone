package _139

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingReader struct {
	io.Reader
	total *atomic.Int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.total.Add(int64(n))
	return n, err
}

type memoryUploadProgressStore struct {
	mu     sync.Mutex
	states map[string]*uploadProgress
}

func newMemoryUploadProgressStore() *memoryUploadProgressStore {
	return &memoryUploadProgressStore{states: make(map[string]*uploadProgress)}
}

func cloneUploadProgress(state *uploadProgress) *uploadProgress {
	if state == nil {
		return nil
	}
	clone := *state
	clone.Pending = append([]int(nil), state.Pending...)
	return &clone
}

func (s *memoryUploadProgressStore) Load(key string) (*uploadProgress, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneUploadProgress(s.states[key]), nil
}

func (s *memoryUploadProgressStore) Save(key string, state *uploadProgress) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state == nil {
		delete(s.states, key)
	} else {
		s.states[key] = cloneUploadProgress(state)
	}
	return nil
}

func (s *memoryUploadProgressStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.states)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type rangeOpenObject struct {
	*object.StaticObjectInfo
	data   []byte
	mu     sync.Mutex
	ranges [][2]int64
}

func (o *rangeOpenObject) SetModTime(context.Context, time.Time) error {
	return fs.ErrorCantSetModTime
}

func (o *rangeOpenObject) Update(context.Context, io.Reader, fs.ObjectInfo, ...fs.OpenOption) error {
	return io.ErrClosedPipe
}

func (o *rangeOpenObject) Remove(context.Context) error {
	return io.ErrClosedPipe
}

func (o *rangeOpenObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	start := int64(0)
	limit := int64(len(o.data))
	for _, option := range options {
		if rangeOption, ok := option.(*fs.RangeOption); ok {
			start, limit = rangeOption.Decode(int64(len(o.data)))
			if limit < 0 {
				limit = int64(len(o.data)) - start
			}
		}
	}
	o.mu.Lock()
	o.ranges = append(o.ranges, [2]int64{start, limit})
	o.mu.Unlock()
	return io.NopCloser(bytes.NewReader(o.data[start : start+limit])), nil
}

type unwrapOnlyUploadInfo struct {
	*object.StaticObjectInfo
	object      fs.Object
	unwrapCalls atomic.Int64
}

func (o *unwrapOnlyUploadInfo) UnWrap() fs.Object {
	o.unwrapCalls.Add(1)
	return o.object
}

func newUploadTestFs(client *http.Client, retries int) (context.Context, *Fs) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = retries
	f := &Fs{
		srv: client,
		pacer: fs.NewPacer(ctx, pacer.NewDefault(
			pacer.MinSleep(time.Microsecond),
			pacer.MaxSleep(time.Millisecond),
			pacer.DecayConstant(2),
		)),
	}
	f.dirCache = dircache.New("", "root", f)
	return ctx, f
}

func TestUploadResumesFailedPart(t *testing.T) {
	const partSize = int64(4)
	data := []byte("abcdefghijkl")

	var (
		mu            sync.Mutex
		serverURL     string
		createCalls   int
		completeCalls int
		partCalls     = map[int]int{}
		partBodies    = map[int][][]byte{}
		requestOrder  []int
		createRequest struct {
			ContentHash string       `json:"contentHash"`
			Size        int64        `json:"size"`
			PartInfos   []uploadPart `json:"partInfos"`
		}
		completeRequest struct {
			FileID   string `json:"fileId"`
			UploadID string `json:"uploadId"`
		}
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/file/create":
			mu.Lock()
			createCalls++
			decodeErr := json.NewDecoder(r.Body).Decode(&createRequest)
			mu.Unlock()
			if decodeErr != nil {
				http.Error(w, decodeErr.Error(), http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data": map[string]any{
					"fileId":   "file-1",
					"uploadId": "upload-1",
					"partInfos": []personalPartInfo{
						{PartNumber: 3, UploadURL: serverURL + "/upload/3"},
						{PartNumber: 1, UploadURL: serverURL + "/upload/1"},
						{PartNumber: 2, UploadURL: serverURL + "/upload/2"},
					},
				},
			})
		case r.URL.Path == "/file/complete":
			mu.Lock()
			completeCalls++
			decodeErr := json.NewDecoder(r.Body).Decode(&completeRequest)
			mu.Unlock()
			if decodeErr != nil {
				http.Error(w, decodeErr.Error(), http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		case strings.HasPrefix(r.URL.Path, "/upload/"):
			partNumber, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/upload/"))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			partCalls[partNumber]++
			attempt := partCalls[partNumber]
			partBodies[partNumber] = append(partBodies[partNumber], body)
			requestOrder = append(requestOrder, partNumber)
			mu.Unlock()
			if partNumber == 2 && attempt == 1 {
				http.Error(w, "retry this part", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverURL = server.URL

	ctx, f := newUploadTestFs(server.Client(), 3)
	f.personalCloudHost = server.URL
	f.opt.CustomUploadPartSize = partSize
	o := &Object{fs: f, remote: "photo.bin"}
	src := object.NewStaticObjectInfo("photo.bin", time.Now(), int64(len(data)), true, nil, nil)

	var accounted atomic.Int64
	wrap := func(r io.Reader) io.Reader {
		return &countingReader{Reader: r, total: &accounted}
	}
	err := o.upload(ctx, bytes.NewReader(data), wrap, src, "photo.bin", "parent-1", nil)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, createCalls)
	assert.Equal(t, 1, completeCalls)
	assert.Equal(t, map[int]int{1: 1, 2: 2, 3: 1}, partCalls)
	assert.Equal(t, []int{1, 2, 2, 3}, requestOrder)
	assert.Equal(t, [][]byte{data[0:4]}, partBodies[1])
	assert.Equal(t, [][]byte{data[4:8], data[4:8]}, partBodies[2])
	assert.Equal(t, [][]byte{data[8:12]}, partBodies[3])
	assert.Equal(t, int64(len(data)), accounted.Load(), "retried bytes must not inflate transfer progress")

	sum := sha256.Sum256(data)
	assert.Equal(t, hex.EncodeToString(sum[:]), createRequest.ContentHash)
	assert.Equal(t, int64(len(data)), createRequest.Size)
	require.Len(t, createRequest.PartInfos, 3)
	for i, part := range createRequest.PartInfos {
		assert.Equal(t, i+1, part.PartNumber)
		assert.Equal(t, partSize, part.PartSize)
		assert.Equal(t, int64(i)*partSize, part.ParallelHashCtx.PartOffset)
	}
	assert.Equal(t, "file-1", completeRequest.FileID)
	assert.Equal(t, "upload-1", completeRequest.UploadID)
}

func TestUploadResumesAcrossPutAttempts(t *testing.T) {
	const partSize = int64(4)
	data := []byte("abcdefghijkl")
	store := newMemoryUploadProgressStore()
	var (
		mu            sync.Mutex
		serverURL     string
		createCalls   int
		getURLCalls   int
		completeCalls int
		partCalls     = map[int]int{}
		failPart2     atomic.Bool
		getURLRequest struct {
			FileID            string       `json:"fileId"`
			UploadID          string       `json:"uploadId"`
			PartInfos         []uploadPart `json:"partInfos"`
			CommonAccountInfo struct {
				Account     string `json:"account"`
				AccountType int    `json:"accountType"`
			} `json:"commonAccountInfo"`
		}
	)
	failPart2.Store(true)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/file/create":
			mu.Lock()
			createCalls++
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data": map[string]any{
					"fileId":   "file-resume",
					"uploadId": "upload-resume",
					"partInfos": []personalPartInfo{
						{PartNumber: 1, UploadURL: serverURL + "/upload/1"},
						{PartNumber: 2, UploadURL: serverURL + "/upload/2"},
						{PartNumber: 3, UploadURL: serverURL + "/upload/3"},
					},
				},
			})
		case r.URL.Path == "/file/getUploadUrl":
			mu.Lock()
			getURLCalls++
			decodeErr := json.NewDecoder(r.Body).Decode(&getURLRequest)
			mu.Unlock()
			if decodeErr != nil {
				http.Error(w, decodeErr.Error(), http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data": map[string]any{
					"fileId":   "file-resume",
					"uploadId": "upload-resume",
					"partInfos": []personalPartInfo{
						{PartNumber: 3, UploadURL: serverURL + "/upload/3"},
						{PartNumber: 2, UploadURL: serverURL + "/upload/2"},
					},
				},
			})
		case r.URL.Path == "/file/complete":
			mu.Lock()
			completeCalls++
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		case strings.HasPrefix(r.URL.Path, "/upload/"):
			partNumber, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/upload/"))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			_, _ = io.Copy(io.Discard, r.Body)
			mu.Lock()
			partCalls[partNumber]++
			mu.Unlock()
			if partNumber == 2 && failPart2.Load() {
				http.Error(w, "temporary failure", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverURL = server.URL

	ctx, f := newUploadTestFs(server.Client(), 2)
	f.personalCloudHost = server.URL
	f.opt.CustomUploadPartSize = partSize
	f.account = "account-1"
	f.uploadState = store
	o := &Object{fs: f, remote: "resume.bin"}
	src := object.NewStaticObjectInfo("resume.bin", time.Now(), int64(len(data)), true, nil, nil)

	err := o.upload(ctx, bytes.NewReader(data), nil, src, "resume.bin", "parent-1", nil)
	require.Error(t, err)
	assert.True(t, fserrors.IsRetryError(err))
	assert.Equal(t, 1, store.Len())
	mu.Lock()
	assert.Equal(t, 1, createCalls)
	assert.Equal(t, 0, getURLCalls)
	assert.Equal(t, 0, completeCalls)
	assert.Equal(t, map[int]int{1: 1, 2: 2}, partCalls)
	mu.Unlock()

	failPart2.Store(false)
	resumeCtx, resumeFs := newUploadTestFs(server.Client(), 2)
	resumeFs.personalCloudHost = server.URL
	resumeFs.opt.CustomUploadPartSize = partSize
	resumeFs.account = "account-1"
	resumeFs.uploadState = store
	resumeObject := &Object{fs: resumeFs, remote: "resume.bin"}
	err = resumeObject.upload(resumeCtx, bytes.NewReader(data), nil, src, "resume.bin", "parent-1", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, store.Len())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, createCalls, "resumed upload must reuse the existing session")
	assert.Equal(t, 1, getURLCalls)
	assert.Equal(t, 1, completeCalls)
	assert.Equal(t, map[int]int{1: 1, 2: 3, 3: 1}, partCalls)
	assert.Equal(t, "file-resume", getURLRequest.FileID)
	assert.Equal(t, "upload-resume", getURLRequest.UploadID)
	assert.Equal(t, "account-1", getURLRequest.CommonAccountInfo.Account)
	assert.Equal(t, 1, getURLRequest.CommonAccountInfo.AccountType)
	require.Len(t, getURLRequest.PartInfos, 2)
	assert.Equal(t, 2, getURLRequest.PartInfos[0].PartNumber)
	assert.Equal(t, 3, getURLRequest.PartInfos[1].PartNumber)
}

func TestUploadResumesCompleteAcrossPutAttempts(t *testing.T) {
	data := []byte("part")
	store := newMemoryUploadProgressStore()
	var (
		serverURL      string
		createCalls    atomic.Int64
		uploadCalls    atomic.Int64
		getURLCalls    atomic.Int64
		completeCalls  atomic.Int64
		failComplete   atomic.Bool
		completeMu     sync.Mutex
		completeBodies [][]byte
	)
	failComplete.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file/create":
			createCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data": map[string]any{
					"fileId":   "file-complete",
					"uploadId": "upload-complete",
					"partInfos": []personalPartInfo{{
						PartNumber: 1,
						UploadURL:  serverURL + "/upload/1",
					}},
				},
			})
		case "/file/getUploadUrl":
			getURLCalls.Add(1)
			http.Error(w, "unexpected URL refresh", http.StatusBadRequest)
		case "/file/complete":
			completeCalls.Add(1)
			body, _ := io.ReadAll(r.Body)
			completeMu.Lock()
			completeBodies = append(completeBodies, body)
			completeMu.Unlock()
			if failComplete.Load() {
				http.Error(w, "complete later", http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		case "/upload/1":
			uploadCalls.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverURL = server.URL

	ctx, f := newUploadTestFs(server.Client(), 2)
	f.personalCloudHost = server.URL
	f.opt.CustomUploadPartSize = int64(len(data))
	f.uploadState = store
	o := &Object{fs: f, remote: "complete.bin"}
	src := object.NewStaticObjectInfo("complete.bin", time.Now(), int64(len(data)), true, nil, nil)

	err := o.upload(ctx, bytes.NewReader(data), nil, src, "complete.bin", "parent-1", nil)
	require.Error(t, err)
	assert.True(t, fserrors.IsRetryError(err))
	assert.Equal(t, 1, store.Len())
	assert.Equal(t, int64(1), createCalls.Load())
	assert.Equal(t, int64(1), uploadCalls.Load())
	assert.Equal(t, int64(2), completeCalls.Load())

	failComplete.Store(false)
	resumeCtx, resumeFs := newUploadTestFs(server.Client(), 2)
	resumeFs.personalCloudHost = server.URL
	resumeFs.opt.CustomUploadPartSize = int64(len(data))
	resumeFs.uploadState = store
	resumeObject := &Object{fs: resumeFs, remote: "complete.bin"}
	err = resumeObject.upload(resumeCtx, bytes.NewReader(data), nil, src, "complete.bin", "parent-1", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, store.Len())
	assert.Equal(t, int64(1), createCalls.Load())
	assert.Equal(t, int64(1), uploadCalls.Load())
	assert.Equal(t, int64(0), getURLCalls.Load())
	assert.Equal(t, int64(3), completeCalls.Load())

	completeMu.Lock()
	defer completeMu.Unlock()
	require.Len(t, completeBodies, 3)
	assert.Equal(t, completeBodies[0], completeBodies[1])
	assert.Equal(t, completeBodies[0], completeBodies[2])
}

func TestUploadKeepsExistingTargetAfterLostCompleteResponse(t *testing.T) {
	data := []byte("part")
	var httpCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpCalls.Add(1)
		http.Error(w, "existing target must not be touched", http.StatusInternalServerError)
	}))
	defer server.Close()

	ctx, f := newUploadTestFs(server.Client(), 2)
	f.personalCloudHost = server.URL
	f.opt.CustomUploadPartSize = int64(len(data))
	store := newMemoryUploadProgressStore()
	f.uploadState = store
	sum := sha256.Sum256(data)
	contentHash := hex.EncodeToString(sum[:])
	progressKey := f.uploadProgressKey("parent-1", "complete.bin", contentHash)
	require.NoError(t, store.Save(progressKey, &uploadProgress{
		Version:     uploadStateVersion,
		FileID:      "file-complete",
		UploadID:    "upload-complete",
		Pending:     nil,
		PartSize:    int64(len(data)),
		Size:        int64(len(data)),
		ContentHash: contentHash,
		ParentID:    "parent-1",
		EncodedName: "complete.bin",
		Account:     f.account,
		UpdatedAt:   time.Now().Unix(),
	}))

	candidate := &Object{fs: f, remote: "complete.bin"}
	existing := &Object{fs: f, remote: "complete.bin", id: "file-complete", size: int64(len(data)), hasMetaData: true}
	src := object.NewStaticObjectInfo("complete.bin", time.Now(), int64(len(data)), true, nil, nil)
	err := candidate.upload(ctx, bytes.NewReader(data), nil, src, "complete.bin", "parent-1", existing)
	require.NoError(t, err)
	assert.Equal(t, int64(0), httpCalls.Load())
	assert.Equal(t, 0, store.Len())
}

func TestUploadReplacesExistingTargetAfterCheckpointExpires(t *testing.T) {
	data := []byte("part")
	store := newMemoryUploadProgressStore()
	var (
		serverURL     string
		removeCalls   atomic.Int64
		createCalls   atomic.Int64
		uploadCalls   atomic.Int64
		completeCalls atomic.Int64
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/recyclebin/batchTrash":
			removeCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		case "/file/create":
			createCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data": map[string]any{
					"fileId":   "file-new",
					"uploadId": "upload-new",
					"partInfos": []personalPartInfo{{
						PartNumber: 1,
						UploadURL:  serverURL + "/upload/1",
					}},
				},
			})
		case "/upload/1":
			uploadCalls.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil || !bytes.Equal(body, data) {
				http.Error(w, "wrong body", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
		case "/file/complete":
			completeCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverURL = server.URL

	ctx, f := newUploadTestFs(server.Client(), 2)
	f.personalCloudHost = server.URL
	f.opt.CustomUploadPartSize = int64(len(data))
	f.account = "account-1"
	f.uploadState = store
	sum := sha256.Sum256(data)
	contentHash := hex.EncodeToString(sum[:])
	progressKey := f.uploadProgressKey("parent-1", "replace.bin", contentHash)
	require.NoError(t, store.Save(progressKey, &uploadProgress{
		Version:     uploadStateVersion,
		FileID:      "file-stale",
		UploadID:    "upload-stale",
		Pending:     []int{1},
		PartSize:    int64(len(data)),
		Size:        int64(len(data)),
		ContentHash: contentHash,
		ParentID:    "parent-1",
		EncodedName: "replace.bin",
		Account:     f.account,
		UpdatedAt:   time.Now().Add(-uploadStateTTL - time.Hour).Unix(),
	}))

	candidate := &Object{fs: f, remote: "replace.bin"}
	existing := &Object{fs: f, remote: "replace.bin", id: "file-existing", size: int64(len(data)), hasMetaData: true}
	src := object.NewStaticObjectInfo("replace.bin", time.Now(), int64(len(data)), true, nil, nil)
	err := candidate.upload(ctx, bytes.NewReader(data), nil, src, "replace.bin", "parent-1", existing)
	require.NoError(t, err)
	assert.Equal(t, int64(1), removeCalls.Load())
	assert.Equal(t, int64(1), createCalls.Load())
	assert.Equal(t, int64(1), uploadCalls.Load())
	assert.Equal(t, int64(1), completeCalls.Load())
	assert.Equal(t, 0, store.Len())
}

func TestUploadPreservesUnknownSessionErrorUntilCheckpointExpires(t *testing.T) {
	data := []byte("part")
	store := newMemoryUploadProgressStore()
	var (
		serverURL     string
		getURLCalls   atomic.Int64
		createCalls   atomic.Int64
		uploadCalls   atomic.Int64
		completeCalls atomic.Int64
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file/getUploadUrl":
			getURLCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": false,
				"code":    "CONTROL_PLANE_BUSY",
				"message": "temporary control-plane failure",
			})
		case "/file/create":
			createCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data": map[string]any{
					"fileId":   "file-new",
					"uploadId": "upload-new",
					"partInfos": []personalPartInfo{{
						PartNumber: 1,
						UploadURL:  serverURL + "/upload/1",
					}},
				},
			})
		case "/upload/1":
			uploadCalls.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		case "/file/complete":
			completeCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverURL = server.URL

	ctx, f := newUploadTestFs(server.Client(), 2)
	f.personalCloudHost = server.URL
	f.opt.CustomUploadPartSize = int64(len(data))
	f.account = "account-1"
	f.uploadState = store
	sum := sha256.Sum256(data)
	contentHash := hex.EncodeToString(sum[:])
	progressKey := f.uploadProgressKey("parent-1", "recreate.bin", contentHash)
	require.NoError(t, store.Save(progressKey, &uploadProgress{
		Version:     uploadStateVersion,
		FileID:      "file-expired",
		UploadID:    "upload-expired",
		Pending:     []int{1},
		PartSize:    int64(len(data)),
		Size:        int64(len(data)),
		ContentHash: contentHash,
		ParentID:    "parent-1",
		EncodedName: "recreate.bin",
		Account:     f.account,
		UpdatedAt:   time.Now().Unix(),
	}))
	src := object.NewStaticObjectInfo("recreate.bin", time.Now(), int64(len(data)), true, nil, nil)
	o := &Object{fs: f, remote: "recreate.bin"}

	err := o.upload(ctx, bytes.NewReader(data), nil, src, "recreate.bin", "parent-1", nil)
	require.Error(t, err)
	var apiErr *personalAPIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "CONTROL_PLANE_BUSY", apiErr.Code)
	assert.Equal(t, 1, store.Len())
	assert.Equal(t, int64(1), getURLCalls.Load())
	assert.Equal(t, int64(0), createCalls.Load())

	stale, err := store.Load(progressKey)
	require.NoError(t, err)
	require.NotNil(t, stale)
	stale.UpdatedAt = time.Now().Add(-uploadStateTTL - time.Hour).Unix()
	require.NoError(t, store.Save(progressKey, stale))

	resumeCtx, resumeFs := newUploadTestFs(server.Client(), 2)
	resumeFs.personalCloudHost = server.URL
	resumeFs.opt.CustomUploadPartSize = int64(len(data))
	resumeFs.account = "account-1"
	resumeFs.uploadState = store
	resumeObject := &Object{fs: resumeFs, remote: "recreate.bin"}
	err = resumeObject.upload(resumeCtx, bytes.NewReader(data), nil, src, "recreate.bin", "parent-1", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), createCalls.Load())
	assert.Equal(t, int64(1), uploadCalls.Load())
	assert.Equal(t, int64(1), completeCalls.Load())
}

func TestUploadRefreshesExpiredPartURL(t *testing.T) {
	data := []byte("part")
	var (
		serverURL   string
		createCalls atomic.Int64
		getURLCalls atomic.Int64
		expiredPUT  atomic.Int64
		freshPUT    atomic.Int64
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file/create":
			createCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data": map[string]any{
					"fileId":   "file-refresh",
					"uploadId": "upload-refresh",
					"partInfos": []personalPartInfo{{
						PartNumber: 1,
						UploadURL:  serverURL + "/expired",
					}},
				},
			})
		case "/file/getUploadUrl":
			getURLCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data": map[string]any{
					"fileId":   "file-refresh",
					"uploadId": "upload-refresh",
					"partInfos": []personalPartInfo{{
						PartNumber: 1,
						UploadURL:  serverURL + "/fresh",
					}},
				},
			})
		case "/file/complete":
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		case "/expired":
			expiredPUT.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			http.Error(w, "signature expired", http.StatusForbidden)
		case "/fresh":
			freshPUT.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil || !bytes.Equal(body, data) {
				http.Error(w, "wrong body", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverURL = server.URL

	ctx, f := newUploadTestFs(server.Client(), 2)
	f.personalCloudHost = server.URL
	f.opt.CustomUploadPartSize = int64(len(data))
	o := &Object{fs: f, remote: "refresh.bin"}
	src := object.NewStaticObjectInfo("refresh.bin", time.Now(), int64(len(data)), true, nil, nil)

	err := o.upload(ctx, bytes.NewReader(data), nil, src, "refresh.bin", "parent-1", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), createCalls.Load())
	assert.Equal(t, int64(1), getURLCalls.Load())
	assert.Equal(t, int64(1), expiredPUT.Load())
	assert.Equal(t, int64(1), freshPUT.Load())
}

func TestUploadRecreatesSessionAfterFreshURLIsRejected(t *testing.T) {
	data := []byte("part")
	store := newMemoryUploadProgressStore()
	var (
		serverURL     string
		createCalls   atomic.Int64
		getURLCalls   atomic.Int64
		rejectedPUTs  atomic.Int64
		workingPUTs   atomic.Int64
		completeCalls atomic.Int64
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file/create":
			createAttempt := createCalls.Add(1)
			uploadURL := serverURL + "/rejected"
			if createAttempt > 1 {
				uploadURL = serverURL + "/working"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data": map[string]any{
					"fileId":   "file-" + strconv.FormatInt(createAttempt, 10),
					"uploadId": "upload-" + strconv.FormatInt(createAttempt, 10),
					"partInfos": []personalPartInfo{{
						PartNumber: 1,
						UploadURL:  uploadURL,
					}},
				},
			})
		case "/file/getUploadUrl":
			getURLCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data": map[string]any{
					"fileId":   "file-1",
					"uploadId": "upload-1",
					"partInfos": []personalPartInfo{{
						PartNumber: 1,
						UploadURL:  serverURL + "/rejected",
					}},
				},
			})
		case "/rejected":
			rejectedPUTs.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			http.Error(w, "session rejected", http.StatusForbidden)
		case "/working":
			workingPUTs.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil || !bytes.Equal(body, data) {
				http.Error(w, "wrong body", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
		case "/file/complete":
			completeCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverURL = server.URL

	ctx, f := newUploadTestFs(server.Client(), 1)
	f.personalCloudHost = server.URL
	f.opt.CustomUploadPartSize = int64(len(data))
	f.account = "account-1"
	f.uploadState = store
	src := object.NewStaticObjectInfo("recreate-session.bin", time.Now(), int64(len(data)), true, nil, nil)
	o := &Object{fs: f, remote: "recreate-session.bin"}

	err := o.upload(ctx, bytes.NewReader(data), nil, src, "recreate-session.bin", "parent-1", nil)
	require.Error(t, err)
	assert.True(t, fserrors.IsRetryError(err))
	assert.Equal(t, 0, store.Len())
	assert.Equal(t, int64(1), createCalls.Load())
	assert.Equal(t, int64(1), getURLCalls.Load())
	assert.Equal(t, int64(2), rejectedPUTs.Load())

	err = o.upload(ctx, bytes.NewReader(data), nil, src, "recreate-session.bin", "parent-1", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), createCalls.Load())
	assert.Equal(t, int64(1), workingPUTs.Load())
	assert.Equal(t, int64(1), completeCalls.Load())
	assert.Equal(t, 0, store.Len())
}

func TestPrepareUploadSourceUsesObjectRanges(t *testing.T) {
	data := []byte("abcdefghijkl")
	src := &rangeOpenObject{
		StaticObjectInfo: object.NewStaticObjectInfo("range.bin", time.Now(), int64(len(data)), true, nil, nil),
		data:             data,
	}
	o := &Object{}
	hash, source, cleanup, err := o.prepareUploadSource(context.Background(), bytes.NewReader(nil), src)
	require.NoError(t, err)
	defer cleanup()

	sum := sha256.Sum256(data)
	assert.Equal(t, hex.EncodeToString(sum[:]), hash)
	for _, test := range []struct {
		offset int64
		size   int64
		want   []byte
	}{
		{offset: 4, size: 4, want: data[4:8]},
		{offset: 8, size: 4, want: data[8:12]},
	} {
		rc, err := source.OpenRange(context.Background(), test.offset, test.size)
		require.NoError(t, err)
		got, err := io.ReadAll(rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
		assert.Equal(t, test.want, got)
	}

	src.mu.Lock()
	defer src.mu.Unlock()
	assert.Equal(t, [][2]int64{{0, int64(len(data))}, {4, 4}, {8, 4}}, src.ranges)
}

func TestPrepareUploadSourceDoesNotUnwrapTransformingObjectInfo(t *testing.T) {
	transformed := []byte("ciphertext")
	underlying := &rangeOpenObject{
		StaticObjectInfo: object.NewStaticObjectInfo("plain.bin", time.Now(), 5, true, nil, nil),
		data:             []byte("plain"),
	}
	src := &unwrapOnlyUploadInfo{
		StaticObjectInfo: object.NewStaticObjectInfo("cipher.bin", time.Now(), int64(len(transformed)), true, nil, nil),
		object:           underlying,
	}
	o := &Object{}
	hash, source, cleanup, err := o.prepareUploadSource(context.Background(), bytes.NewReader(transformed), src)
	require.NoError(t, err)
	defer cleanup()

	sum := sha256.Sum256(transformed)
	assert.Equal(t, hex.EncodeToString(sum[:]), hash)
	rc, err := source.OpenRange(context.Background(), 2, 5)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	assert.Equal(t, transformed[2:7], got)
	assert.Equal(t, int64(0), src.unwrapCalls.Load())
	underlying.mu.Lock()
	defer underlying.mu.Unlock()
	assert.Empty(t, underlying.ranges)
}

func TestUploadPartPermanentFailureIsNotRetried(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	ctx, f := newUploadTestFs(server.Client(), 4)
	o := &Object{fs: f, remote: "photo.bin"}
	data := []byte("part")
	source := &uploadSource{openRange: func(ctx context.Context, offset, size int64) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data[offset : offset+size])), nil
	}}
	part := uploadPart{PartNumber: 1, PartSize: int64(len(data))}

	err := o.uploadPartFromSource(ctx, source, nil, part, personalPartInfo{
		PartNumber: 1,
		UploadURL:  server.URL,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "http 400")
	assert.Equal(t, int64(1), calls.Load())
	assert.False(t, fserrors.IsRetryError(err))
}

func TestUploadPartResumesAfterPartialBodyRead(t *testing.T) {
	data := []byte("part")
	var (
		calls      atomic.Int64
		firstBody  []byte
		secondBody []byte
	)
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			firstBody = make([]byte, 2)
			_, _ = io.ReadFull(req.Body, firstBody)
			return nil, io.ErrUnexpectedEOF
		}
		secondBody, _ = io.ReadAll(req.Body)
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    req,
		}, nil
	})}

	ctx, f := newUploadTestFs(client, 2)
	o := &Object{fs: f, remote: "partial.bin"}
	source := &uploadSource{openRange: func(ctx context.Context, offset, size int64) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data[offset : offset+size])), nil
	}}
	part := uploadPart{PartNumber: 1, PartSize: int64(len(data))}
	var accounted atomic.Int64
	wrap := func(r io.Reader) io.Reader {
		return &countingReader{Reader: r, total: &accounted}
	}

	err := o.uploadPartFromSource(ctx, source, wrap, part, personalPartInfo{
		PartNumber: 1,
		UploadURL:  "https://upload.invalid/part",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), calls.Load())
	assert.Equal(t, data[:2], firstBody)
	assert.Equal(t, data, secondBody)
	assert.Equal(t, int64(len(data)), accounted.Load())
}

func TestDoReplaysRequestBodyOnRetry(t *testing.T) {
	var (
		mu     sync.Mutex
		bodies [][]byte
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		bodies = append(bodies, body)
		attempt := len(bodies)
		mu.Unlock()
		if attempt == 1 {
			http.Error(w, "retry", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, f := newUploadTestFs(server.Client(), 2)
	payload := []byte(`{"fileId":"file-1","uploadId":"upload-1"}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(payload))
	require.NoError(t, err)

	resp, err := f.do(req, shouldRetry)
	require.NoError(t, err)
	require.NotNil(t, resp)
	_ = resp.Body.Close()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, [][]byte{payload, payload}, bodies)
}
