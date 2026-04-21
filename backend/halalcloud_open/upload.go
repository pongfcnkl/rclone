package halalcloudopen

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"

	sdkUserFile "github.com/halalcloud/golang-sdk-lite/halalcloud/services/userfile"
	"github.com/ipfs/go-cid"
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

type uploadedFile struct {
	Identity        string `json:"identity"`
	UserIdentity    string `json:"user_identity"`
	Path            string `json:"path"`
	Size            int64  `json:"size"`
	ContentIdentity string `json:"content_identity"`
}

func newReopenableSource(src fs.ObjectInfo) (*reopenableSource, error) {
	if src.Size() < 0 {
		return nil, errors.New("halalcloud_open upload requires known size")
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
	return nil, fmt.Errorf("halalcloud_open upload requires reopenable source object; streaming readers without temp cache are unsupported (src=%T remote=%q)", src, src.Remote())
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

	source, sourceErr := newReopenableSource(src)
	if sourceErr == nil {
		return f.uploadFromSource(ctx, source, acc, transfer, src, options...)
	}
	return f.uploadFromTempFile(ctx, baseIn, acc, transfer, src, options...)
}

func (f *Fs) uploadFromSource(ctx context.Context, source *reopenableSource, acc *accounting.Account, transfer *accounting.Transfer, src fs.ObjectInfo, options ...fs.OpenOption) error {
	newPath := f.fullPath(src.Remote())
	task, err := f.fileService.CreateUploadTask(ctx, &sdkUserFile.File{Path: newPath, Size: src.Size()})
	if err != nil {
		return err
	}
	if task.Created {
		return nil
	}
	prefix := cid.Prefix{
		Codec:    defaultCodec(task.BlockCodec),
		MhLength: -1,
		MhType:   defaultHashType(task.BlockHashType),
		Version:  1,
	}
	blockSize := task.BlockSize
	if blockSize <= 0 {
		return errors.New("halalcloud_open: invalid block size")
	}
	partCount := (src.Size() + blockSize - 1) / blockSize
	slices := make([]string, 0, partCount)
	var progressAcc *accounting.Account
	for partIndex := int64(0); partIndex < partCount; partIndex++ {
		offset := partIndex * blockSize
		size := blockSize
		if remain := src.Size() - offset; remain < size {
			size = remain
		}
		rc, err := source.OpenRange(ctx, offset, size)
		if err != nil {
			return err
		}
		chunk, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			return err
		}
		newCID, err := prefix.Sum(chunk)
		if err != nil {
			return err
		}
		if err = f.postFileSlice(ctx, task.Task, task.UploadAddress, newCID.String(), bytes.NewReader(chunk), acc, transfer, &progressAcc, options); err != nil {
			return err
		}
		slices = append(slices, newCID.String())
	}
	_, err = f.makeFile(task.Task, task.UploadAddress, slices)
	return err
}

func (f *Fs) uploadFromTempFile(ctx context.Context, in io.Reader, acc *accounting.Account, transfer *accounting.Transfer, src fs.ObjectInfo, options ...fs.OpenOption) error {
	tempFile, err := os.CreateTemp("", "rclone-halalcloud-*")
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
		return fmt.Errorf("halalcloud_open: expected %d bytes, got %d", src.Size(), written)
	}
	if _, err = tempFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return f.uploadFromSource(ctx, &reopenableSource{
		open: func(_ context.Context, opts ...fs.OpenOption) (io.ReadCloser, error) {
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
			if _, err := tempFile.Seek(start, io.SeekStart); err != nil {
				return nil, err
			}
			reader := io.Reader(tempFile)
			if end >= start {
				reader = io.LimitReader(tempFile, end-start+1)
			}
			return io.NopCloser(reader), nil
		},
	}, acc, transfer, src, options...)
}

func defaultCodec(v int64) uint64 {
	if v > 0 {
		return uint64(v)
	}
	return 0x55
}

func defaultHashType(v int64) uint64 {
	if v > 0 {
		return uint64(v)
	}
	return 0x12
}

func (f *Fs) postFileSlice(ctx context.Context, taskID, uploadAddress, sliceCID string, section io.Reader, acc *accounting.Account, transfer *accounting.Transfer, progressAcc **accounting.Account, options []fs.OpenOption) error {
	accessURL := strings.TrimRight(uploadAddress, "/") + "/" + taskID + "/" + sliceCID
	checkReq, err := http.NewRequestWithContext(ctx, http.MethodGet, accessURL, nil)
	if err != nil {
		return err
	}
	checkReq.Header.Set("Accept", "application/json")
	checkResp, err := f.httpClient.Do(checkReq)
	if err != nil {
		return err
	}
	checkBody, err := io.ReadAll(checkResp.Body)
	_ = checkResp.Body.Close()
	if err != nil {
		return err
	}
	if checkResp.StatusCode != http.StatusOK {
		return fmt.Errorf("halalcloud_open: upload slice check http %d", checkResp.StatusCode)
	}
	var exists bool
	if err = json.Unmarshal(checkBody, &exists); err != nil {
		return err
	}
	if exists {
		return nil
	}

	body := io.NopCloser(section)
	if acc != nil {
		body = readCloserWithReader{Reader: acc.WrapStream(body), Closer: body}
	} else if transfer != nil {
		if progressAcc != nil && *progressAcc != nil {
			body = readCloserWithReader{Reader: (*progressAcc).WrapStream(body), Closer: body}
		} else {
			wrapped := transfer.Account(ctx, body)
			if progressAcc != nil {
				*progressAcc = wrapped
			}
			body = wrapped
		}
	}
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, accessURL, body)
	if err != nil {
		return err
	}
	postReq.Header.Set("Accept", "application/json")
	postReq.Header.Set("Content-Type", "application/octet-stream")
	fs.OpenOptionAddHTTPHeaders(postReq.Header, options)
	resp, err := f.httpClient.Do(postReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("halalcloud_open: upload slice http %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func (f *Fs) makeFile(taskID, uploadAddress string, slices []string) (*uploadedFile, error) {
	accessURL := strings.TrimRight(uploadAddress, "/") + "/" + taskID
	payload, err := json.Marshal(slices)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, accessURL, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("halalcloud_open: make file http %d: %s", resp.StatusCode, string(body))
	}
	out := new(uploadedFile)
	if err = json.Unmarshal(body, out); err != nil {
		return nil, err
	}
	return out, nil
}
