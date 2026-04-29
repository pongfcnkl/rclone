package quarkuc

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
)

type readCloserWithReader struct {
	io.Reader
	io.Closer
}

type reopenableSource struct {
	open func(context.Context, ...fs.OpenOption) (io.ReadCloser, error)
}

type hashInfo struct {
	md5  string
	sha1 string
}

type baseResp struct {
	Status  int    `json:"status"`
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type sortResp struct {
	baseResp
	Data struct {
		List []fileItem `json:"list"`
	} `json:"data"`
	Metadata struct {
		Total int `json:"_total"`
	} `json:"metadata"`
}

type memberResp struct {
	baseResp
	Data struct {
		UseCapacity   int64 `json:"use_capacity"`
		TotalCapacity int64 `json:"total_capacity"`
	} `json:"data"`
}

type downloadResp struct {
	baseResp
	Data []struct {
		DownloadURL string `json:"download_url"`
	} `json:"data"`
}

type transcodingResp struct {
	baseResp
	Data struct {
		VideoList []struct {
			VideoInfo struct {
				URL  string `json:"url"`
				Size int64  `json:"size"`
			} `json:"video_info"`
		} `json:"video_list"`
	} `json:"data"`
}

type upPreResp struct {
	baseResp
	Data struct {
		TaskID    string `json:"task_id"`
		UploadID  string `json:"upload_id"`
		ObjKey    string `json:"obj_key"`
		UploadURL string `json:"upload_url"`
		Bucket    string `json:"bucket"`
		AuthInfo  string `json:"auth_info"`
		Callback  struct {
			CallbackURL  string `json:"callbackUrl"`
			CallbackBody string `json:"callbackBody"`
		} `json:"callback"`
	} `json:"data"`
	Metadata struct {
		PartSize int64 `json:"part_size"`
	} `json:"metadata"`
}

type hashResp struct {
	baseResp
	Data struct {
		Finish bool `json:"finish"`
	} `json:"data"`
}

type upAuthResp struct {
	baseResp
	Data struct {
		AuthKey string `json:"auth_key"`
	} `json:"data"`
}

func newReopenableSource(src fs.ObjectInfo) (*reopenableSource, error) {
	if src.Size() < 0 {
		return nil, errors.New("quark_uc upload requires known size")
	}
	type openable interface {
		Open(context.Context, ...fs.OpenOption) (io.ReadCloser, error)
	}
	if obj, ok := src.(openable); ok {
		return &reopenableSource{open: obj.Open}, nil
	}
	type unwrapObject interface {
		UnWrap() fs.Object
	}
	if wrapped, ok := src.(unwrapObject); ok {
		if obj := wrapped.UnWrap(); obj != nil {
			return &reopenableSource{open: obj.Open}, nil
		}
	}
	if obj := fs.UnWrapObjectInfo(src); obj != nil {
		return &reopenableSource{open: obj.Open}, nil
	}
	type fsRemote interface {
		Fs() fs.Info
		Remote() string
	}
	if item, ok := src.(fsRemote); ok {
		if srcFs, ok := item.Fs().(fs.Fs); ok {
			obj, err := srcFs.NewObject(context.Background(), item.Remote())
			if err == nil {
				return &reopenableSource{open: obj.Open}, nil
			}
		}
	}
	return nil, fmt.Errorf("quark_uc upload requires reopenable source object; streaming readers without local cache are unsupported (src=%T remote=%q)", src, src.Remote())
}

func (s *reopenableSource) OpenRange(ctx context.Context, offset, size int64) (io.ReadCloser, error) {
	if size < 0 {
		return s.open(ctx, &fs.RangeOption{Start: offset, End: -1})
	}
	return s.open(ctx, &fs.RangeOption{Start: offset, End: offset + size - 1})
}

func (f *Fs) upload(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	baseIn, acc := accounting.UnWrapAccounting(in)
	var transfer *accounting.Transfer
	if acc == nil {
		var srcFs fs.Fs
		if item, ok := src.(interface{ Fs() fs.Info }); ok {
			if v, ok := item.Fs().(fs.Fs); ok {
				srcFs = v
			}
		}
		transfer = accounting.Stats(ctx).NewTransferRemoteSize(src.Remote(), src.Size(), srcFs, f)
		defer func() {
			transfer.Done(ctx, err)
		}()
	}
	parentRemote := path.Dir(src.Remote())
	if parentRemote == "." {
		parentRemote = ""
	}
	if err = f.ensureDir(ctx, parentRemote); err != nil {
		return err
	}
	parent, err := f.findDirByRemote(ctx, parentRemote)
	if err != nil {
		return err
	}
	source, sourceErr := newReopenableSource(src)
	if sourceErr == nil {
		return f.uploadFromSource(ctx, source, acc, transfer, src, parent.id, options...)
	}
	return f.uploadFromCache(ctx, baseIn, acc, transfer, src, parent.id, options...)
}

func (f *Fs) uploadFromCache(ctx context.Context, in io.Reader, acc *accounting.Account, transfer *accounting.Transfer, src fs.ObjectInfo, parentID string, options ...fs.OpenOption) error {
	cachePath, err := cacheFilePath("upload-*")
	if err != nil {
		return err
	}
	tempFile, err := os.Create(cachePath)
	if err != nil {
		return err
	}
	tempName := tempFile.Name()
	defer func() {
		_ = tempFile.Close()
		_ = os.Remove(tempName)
	}()
	written, err := io.Copy(tempFile, in)
	if err != nil {
		return err
	}
	if written != src.Size() {
		return fmt.Errorf("quark_uc: expected %d bytes, got %d", src.Size(), written)
	}
	if err = tempFile.Close(); err != nil {
		return err
	}
	return f.uploadFromSource(ctx, &reopenableSource{
		open: func(_ context.Context, opts ...fs.OpenOption) (io.ReadCloser, error) {
			file, err := os.Open(tempName)
			if err != nil {
				return nil, err
			}
			var start, end int64
			start, end = 0, -1
			for _, opt := range opts {
				switch x := opt.(type) {
				case *fs.RangeOption:
					start, end = x.Start, x.End
				case *fs.SeekOption:
					start, end = x.Offset, -1
				}
			}
			if _, err = file.Seek(start, io.SeekStart); err != nil {
				_ = file.Close()
				return nil, err
			}
			reader := io.Reader(file)
			if end >= start {
				reader = io.LimitReader(file, end-start+1)
			}
			return readCloserWithReader{Reader: reader, Closer: file}, nil
		},
	}, acc, transfer, src, parentID, options...)
}

func (f *Fs) uploadFromSource(ctx context.Context, source *reopenableSource, acc *accounting.Account, transfer *accounting.Transfer, src fs.ObjectInfo, parentID string, options ...fs.OpenOption) error {
	hashes, err := computeHashes(ctx, source)
	if err != nil {
		return err
	}
	pre, err := f.upPre(ctx, src, parentID)
	if err != nil {
		return err
	}
	finished, err := f.upHash(ctx, hashes.md5, hashes.sha1, pre.Data.TaskID)
	if err != nil {
		return err
	}
	if finished {
		return nil
	}
	partSize := pre.Metadata.PartSize
	if partSize <= 0 {
		return errors.New("quark_uc: invalid part size")
	}
	partCount := int((src.Size() + partSize - 1) / partSize)
	etags := make([]string, 0, partCount)
	var progressAcc *accounting.Account
	for partIndex := 0; partIndex < partCount; partIndex++ {
		offset := int64(partIndex) * partSize
		size := partSize
		if remain := src.Size() - offset; remain < size {
			size = remain
		}
		rc, err := source.OpenRange(ctx, offset, size)
		if err != nil {
			return err
		}
		body := wrapUploadReader(ctx, rc, acc, transfer, &progressAcc)
		etag, err := f.upPart(ctx, pre, fs.MimeType(ctx, src), partIndex+1, body, size, options)
		_ = body.Close()
		if err != nil {
			return err
		}
		etags = append(etags, etag)
	}
	if err := f.upCommit(ctx, pre, etags); err != nil {
		return err
	}
	return f.upFinish(ctx, pre)
}

func computeHashes(ctx context.Context, source *reopenableSource) (*hashInfo, error) {
	rc, err := source.OpenRange(ctx, 0, -1)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	md5Hash := md5.New()
	sha1Hash := sha1.New()
	if _, err = io.Copy(io.MultiWriter(md5Hash, sha1Hash), rc); err != nil {
		return nil, err
	}
	return &hashInfo{
		md5:  hex.EncodeToString(md5Hash.Sum(nil)),
		sha1: hex.EncodeToString(sha1Hash.Sum(nil)),
	}, nil
}

func wrapUploadReader(ctx context.Context, rc io.ReadCloser, acc *accounting.Account, transfer *accounting.Transfer, progressAcc **accounting.Account) io.ReadCloser {
	if acc != nil {
		return readCloserWithReader{
			Reader: acc.WrapStream(rc),
			Closer: rc,
		}
	}
	if transfer != nil {
		if progressAcc != nil && *progressAcc != nil {
			return readCloserWithReader{
				Reader: (*progressAcc).WrapStream(rc),
				Closer: rc,
			}
		}
		wrapped := transfer.Account(ctx, rc)
		if progressAcc != nil {
			*progressAcc = wrapped
		}
		return wrapped
	}
	return rc
}

func (f *Fs) upPre(ctx context.Context, src fs.ObjectInfo, parentID string) (*upPreResp, error) {
	now := time.Now().UnixMilli()
	body := map[string]any{
		"ccp_hash_update": true,
		"dir_name":        "",
		"file_name":       f.opt.Enc.FromStandardName(path.Base(src.Remote())),
		"format_type":     fs.MimeType(ctx, src),
		"l_created_at":    now,
		"l_updated_at":    now,
		"pdir_fid":        parentID,
		"size":            src.Size(),
	}
	var resp upPreResp
	_, err := f.call(ctx, http.MethodPost, "/file/upload/pre", nil, body, nil, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (f *Fs) upHash(ctx context.Context, md5Str, sha1Str, taskID string) (bool, error) {
	body := map[string]any{
		"md5":     md5Str,
		"sha1":    sha1Str,
		"task_id": taskID,
	}
	var resp hashResp
	_, err := f.call(ctx, http.MethodPost, "/file/update/hash", nil, body, nil, &resp)
	if err != nil {
		return false, err
	}
	return resp.Data.Finish, nil
}

func (f *Fs) upPart(ctx context.Context, pre *upPreResp, mimeType string, partNumber int, body io.ReadCloser, size int64, options []fs.OpenOption) (string, error) {
	timeStr := time.Now().UTC().Format(http.TimeFormat)
	authMeta := fmt.Sprintf(`PUT

%s
%s
x-oss-date:%s
x-oss-user-agent:aliyun-sdk-js/6.6.1 Chrome 98.0.4758.80 on Windows 10 64-bit
/%s/%s?partNumber=%d&uploadId=%s`,
		mimeType, timeStr, timeStr, pre.Data.Bucket, pre.Data.ObjKey, partNumber, pre.Data.UploadID)
	var authResp upAuthResp
	_, err := f.call(ctx, http.MethodPost, "/file/upload/auth", nil, map[string]any{
		"auth_info": pre.Data.AuthInfo,
		"auth_meta": authMeta,
		"task_id":   pre.Data.TaskID,
	}, nil, &authResp)
	if err != nil {
		return "", err
	}
	u := fmt.Sprintf("https://%s.%s/%s", pre.Data.Bucket, normalizeUploadHost(pre.Data.UploadURL), pre.Data.ObjKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, struct{ io.Reader }{body})
	if err != nil {
		return "", err
	}
	req.ContentLength = size
	req.Header.Set("Authorization", authResp.Data.AuthKey)
	req.Header.Set("Content-Type", mimeType)
	req.Header.Set("Referer", "https://pan.quark.cn/")
	req.Header.Set("x-oss-date", timeStr)
	req.Header.Set("x-oss-user-agent", "aliyun-sdk-js/6.6.1 Chrome 98.0.4758.80 on Windows 10 64-bit")
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	q := req.URL.Query()
	q.Set("partNumber", strconv.Itoa(partNumber))
	q.Set("uploadId", pre.Data.UploadID)
	req.URL.RawQuery = q.Encode()
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("quark_uc: upload part http %d: %s", resp.StatusCode, string(data))
	}
	return resp.Header.Get("Etag"), nil
}

func (f *Fs) upCommit(ctx context.Context, pre *upPreResp, etags []string) error {
	timeStr := time.Now().UTC().Format(http.TimeFormat)
	var bodyBuilder strings.Builder
	bodyBuilder.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUpload>\n")
	for i, etag := range etags {
		bodyBuilder.WriteString(fmt.Sprintf("<Part>\n<PartNumber>%d</PartNumber>\n<ETag>%s</ETag>\n</Part>\n", i+1, etag))
	}
	bodyBuilder.WriteString("</CompleteMultipartUpload>")
	xmlBody := bodyBuilder.String()
	sum := md5.Sum([]byte(xmlBody))
	contentMD5 := base64.StdEncoding.EncodeToString(sum[:])
	callbackBytes, err := json.Marshal(pre.Data.Callback)
	if err != nil {
		return err
	}
	callbackBase64 := base64.StdEncoding.EncodeToString(callbackBytes)
	authMeta := fmt.Sprintf(`POST
%s
application/xml
%s
x-oss-callback:%s
x-oss-date:%s
x-oss-user-agent:aliyun-sdk-js/6.6.1 Chrome 98.0.4758.80 on Windows 10 64-bit
/%s/%s?uploadId=%s`,
		contentMD5, timeStr, callbackBase64, timeStr, pre.Data.Bucket, pre.Data.ObjKey, pre.Data.UploadID)
	var authResp upAuthResp
	_, err = f.call(ctx, http.MethodPost, "/file/upload/auth", nil, map[string]any{
		"auth_info": pre.Data.AuthInfo,
		"auth_meta": authMeta,
		"task_id":   pre.Data.TaskID,
	}, nil, &authResp)
	if err != nil {
		return err
	}
	u := fmt.Sprintf("https://%s.%s/%s", pre.Data.Bucket, normalizeUploadHost(pre.Data.UploadURL), pre.Data.ObjKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u+"?uploadId="+urlQueryEscape(pre.Data.UploadID), strings.NewReader(xmlBody))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", authResp.Data.AuthKey)
	req.Header.Set("Content-MD5", contentMD5)
	req.Header.Set("Content-Type", "application/xml")
	req.Header.Set("Referer", "https://pan.quark.cn/")
	req.Header.Set("x-oss-callback", callbackBase64)
	req.Header.Set("x-oss-date", timeStr)
	req.Header.Set("x-oss-user-agent", "aliyun-sdk-js/6.6.1 Chrome 98.0.4758.80 on Windows 10 64-bit")
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("quark_uc: commit http %d: %s", resp.StatusCode, string(data))
	}
	return nil
}

func (f *Fs) upFinish(ctx context.Context, pre *upPreResp) error {
	_, err := f.call(ctx, http.MethodPost, "/file/upload/finish", nil, map[string]any{
		"obj_key": pre.Data.ObjKey,
		"task_id": pre.Data.TaskID,
	}, nil, nil)
	if err == nil {
		time.Sleep(time.Second)
	}
	return err
}

func (f *Fs) downloadLink(ctx context.Context, obj *Object) (string, http.Header, error) {
	if f.opt.UseTranscoding && f.service.name == "quark" && obj.category == 1 && obj.size > 0 {
		var resp transcodingResp
		_, err := f.call(ctx, http.MethodPost, "/file/v2/play/project", nil, map[string]any{
			"fid":         obj.id,
			"resolutions": "low,normal,high,super,2k,4k",
			"supports":    "fmp4_av,m3u8,dolby_vision",
		}, nil, &resp)
		if err == nil {
			for _, item := range resp.Data.VideoList {
				if item.VideoInfo.URL != "" {
					return item.VideoInfo.URL, http.Header{}, nil
				}
			}
		}
	}
	var resp downloadResp
	_, err := f.call(ctx, http.MethodPost, "/file/download", nil, map[string]any{
		"fids": []string{obj.id},
	}, map[string]string{
		"User-Agent": f.service.ua,
	}, &resp)
	if err != nil {
		return "", nil, err
	}
	if len(resp.Data) == 0 || resp.Data[0].DownloadURL == "" {
		return "", nil, errors.New("quark_uc: empty download url")
	}
	return resp.Data[0].DownloadURL, http.Header{
		"Cookie":     []string{f.opt.Cookie},
		"Referer":    []string{f.service.referer},
		"User-Agent": []string{f.service.ua},
	}, nil
}

func urlQueryEscape(v string) string {
	return strings.ReplaceAll(strings.ReplaceAll(v, "+", "%2B"), "/", "%2F")
}

func normalizeUploadHost(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "https://")
	raw = strings.TrimPrefix(raw, "http://")
	if strings.HasPrefix(raw, "http//") || strings.HasPrefix(raw, "https//") {
		raw = strings.TrimPrefix(raw, "http//")
		raw = strings.TrimPrefix(raw, "https//")
	}
	if !strings.Contains(raw, "://") {
		if parsed, err := url.Parse("https://" + raw); err == nil && parsed.Host != "" {
			return strings.TrimSuffix(parsed.Host, "/")
		}
		return strings.Trim(raw, "/")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return strings.Trim(raw, "/")
	}
	return strings.TrimSuffix(parsed.Host, "/")
}
