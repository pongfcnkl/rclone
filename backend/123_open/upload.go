package open123

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
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
	"github.com/rclone/rclone/lib/rest"
)

type readCloserWithReader struct {
	io.Reader
	io.Closer
}

type reopenableSource struct {
	open func(context.Context, ...fs.OpenOption) (io.ReadCloser, error)
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
	filename := f.opt.Enc.FromStandardName(path.Base(src.Remote()))

	source, err := newReopenableSource(src)
	if err == nil {
		return f.uploadFromSource(ctx, source, acc, transfer, src, parent.FileID, filename, options...)
	}

	return f.uploadFromTempFile(ctx, baseIn, acc, transfer, src, parent.FileID, filename, options...)
}

func newReopenableSource(src fs.ObjectInfo) (*reopenableSource, error) {
	if src.Size() < 0 {
		return nil, errors.New("123_open upload requires known size")
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
	return nil, fmt.Errorf("123_open upload requires reopenable source object; streaming readers without temp cache are unsupported (src=%T remote=%q)", src, src.Remote())
}

func (s *reopenableSource) OpenRange(ctx context.Context, offset, size int64) (io.ReadCloser, error) {
	if size < 0 {
		return s.open(ctx, &fs.RangeOption{Start: offset, End: -1})
	}
	return s.open(ctx, &fs.RangeOption{Start: offset, End: offset + size - 1})
}

func (f *Fs) uploadFromSource(ctx context.Context, source *reopenableSource, acc *accounting.Account, transfer *accounting.Transfer, src fs.ObjectInfo, parentFileID int64, filename string, options ...fs.OpenOption) error {
	fileMD5, fileSHA1, written, err := f.hashFromSource(ctx, source)
	if err != nil {
		return err
	}
	if written != src.Size() {
		return fmt.Errorf("123_open: expected %d bytes, got %d", src.Size(), written)
	}

	if reuseResp, err := f.apiSHA1Reuse(ctx, parentFileID, filename, fileSHA1, src.Size(), 2); err == nil && reuseResp.Data.Reuse {
		return nil
	}

	createResp, err := f.apiCreateFile(ctx, parentFileID, filename, fileMD5, src.Size(), 2, false)
	if err != nil {
		return err
	}
	if createResp.Data.Reuse {
		return nil
	}
	if createResp.Data.PreuploadID == "" || len(createResp.Data.Servers) == 0 {
		return errors.New("123_open: upload precreate returned no upload server")
	}

	chunkSize := createResp.Data.SliceSize
	if chunkSize <= 0 {
		return errors.New("123_open: invalid slice size")
	}

	partCount := (src.Size() + chunkSize - 1) / chunkSize
	var progressAcc *accounting.Account
	for partIndex := int64(0); partIndex < partCount; partIndex++ {
		offset := partIndex * chunkSize
		size := chunkSize
		if remain := src.Size() - offset; remain < size {
			size = remain
		}
		rc, err := source.OpenRange(ctx, offset, size)
		if err != nil {
			return err
		}
		sliceMD5, err := hashReadCloserMD5(rc)
		if err != nil {
			_ = rc.Close()
			return err
		}
		_ = rc.Close()

		rc, err = source.OpenRange(ctx, offset, size)
		if err != nil {
			return err
		}
		err = f.uploadSlice(ctx, createResp.Data.Servers[0], createResp.Data.PreuploadID, partIndex+1, src.Remote(), rc, acc, transfer, &progressAcc, size, sliceMD5, options)
		_ = rc.Close()
		if err != nil {
			return err
		}
	}

	return f.waitUploadComplete(ctx, createResp.Data.PreuploadID)
}

func (f *Fs) uploadFromTempFile(ctx context.Context, in io.Reader, acc *accounting.Account, transfer *accounting.Transfer, src fs.ObjectInfo, parentFileID int64, filename string, options ...fs.OpenOption) error {
	tempFile, err := os.CreateTemp("", "rclone-123open-*")
	if err != nil {
		return err
	}
	tempName := tempFile.Name()
	defer func() {
		_ = tempFile.Close()
		_ = os.Remove(tempName)
	}()

	md5Hash := md5.New()
	sha1Hash := sha1.New()
	written, err := io.Copy(io.MultiWriter(tempFile, md5Hash, sha1Hash), in)
	if err != nil {
		return err
	}
	if written != src.Size() {
		return fmt.Errorf("123_open: expected %d bytes, got %d", src.Size(), written)
	}

	fileMD5 := hex.EncodeToString(md5Hash.Sum(nil))
	fileSHA1 := hex.EncodeToString(sha1Hash.Sum(nil))

	if reuseResp, err := f.apiSHA1Reuse(ctx, parentFileID, filename, fileSHA1, src.Size(), 2); err == nil && reuseResp.Data.Reuse {
		return nil
	}

	createResp, err := f.apiCreateFile(ctx, parentFileID, filename, fileMD5, src.Size(), 2, false)
	if err != nil {
		return err
	}
	if createResp.Data.Reuse {
		return nil
	}
	if createResp.Data.PreuploadID == "" || len(createResp.Data.Servers) == 0 {
		return errors.New("123_open: upload precreate returned no upload server")
	}

	chunkSize := createResp.Data.SliceSize
	if chunkSize <= 0 {
		return errors.New("123_open: invalid slice size")
	}

	if _, err = tempFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	partCount := (src.Size() + chunkSize - 1) / chunkSize
	buffer := make([]byte, chunkSize)
	var progressAcc *accounting.Account
	for partIndex := int64(0); partIndex < partCount; partIndex++ {
		partSize := chunkSize
		remaining := src.Size() - partIndex*chunkSize
		if remaining < partSize {
			partSize = remaining
		}
		n, err := io.ReadFull(tempFile, buffer[:partSize])
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return err
		}
		chunk := buffer[:n]
		sliceMD5 := md5.Sum(chunk)
		if err = f.uploadSlice(ctx, createResp.Data.Servers[0], createResp.Data.PreuploadID, partIndex+1, src.Remote(), bytes.NewReader(chunk), acc, transfer, &progressAcc, int64(len(chunk)), hex.EncodeToString(sliceMD5[:]), options); err != nil {
			return err
		}
	}

	return f.waitUploadComplete(ctx, createResp.Data.PreuploadID)
}

func (f *Fs) waitUploadComplete(ctx context.Context, preuploadID string) error {
	for i := 0; i < 60; i++ {
		completeResp, err := f.apiCompleteUpload(ctx, preuploadID)
		if err == nil && completeResp.Data.Completed && completeResp.Data.FileID != 0 {
			return nil
		}
		time.Sleep(time.Second)
	}
	return errors.New("123_open: upload complete timeout")
}

func (f *Fs) hashFromSource(ctx context.Context, source *reopenableSource) (fileMD5, fileSHA1 string, written int64, err error) {
	rc, err := source.OpenRange(ctx, 0, -1)
	if err != nil {
		return "", "", 0, err
	}
	defer rc.Close()

	md5Hash := md5.New()
	sha1Hash := sha1.New()
	written, err = io.Copy(io.MultiWriter(md5Hash, sha1Hash), rc)
	if err != nil {
		return "", "", 0, err
	}
	fileMD5 = hex.EncodeToString(md5Hash.Sum(nil))
	fileSHA1 = hex.EncodeToString(sha1Hash.Sum(nil))
	return fileMD5, fileSHA1, written, nil
}

func hashReadCloserMD5(rc io.Reader) (string, error) {
	sum := md5.New()
	_, err := io.Copy(sum, rc)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(sum.Sum(nil)), nil
}

func (f *Fs) uploadSlice(ctx context.Context, serverURL, preuploadID string, partNumber int64, remote string, section io.Reader, acc *accounting.Account, transfer *accounting.Transfer, progressAcc **accounting.Account, chunkSize int64, sliceMD5 string, options []fs.OpenOption) error {
	token, err := f.getAccessToken(ctx, false)
	if err != nil {
		return err
	}
	params := url.Values{}
	params.Set("preuploadID", preuploadID)
	params.Set("sliceNo", strconv.FormatInt(partNumber, 10))
	params.Set("sliceMD5", sliceMD5)
	body, contentType, overhead, err := rest.MultipartUpload(ctx, section, params, "slice", path.Base(remote))
	if err != nil {
		return err
	}
	if acc != nil {
		body = readCloserWithReader{
			Reader: acc.WrapStream(body),
			Closer: body,
		}
	} else if transfer != nil {
		if progressAcc != nil && *progressAcc != nil {
			body = readCloserWithReader{
				Reader: (*progressAcc).WrapStream(body),
				Closer: body,
			}
		} else {
			wrapped := transfer.Account(ctx, body)
			if progressAcc != nil {
				*progressAcc = wrapped
			}
			body = wrapped
		}
	}
	contentLength := chunkSize + overhead
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(serverURL, "/")+"/upload/v2/file/slice", body)
	if err != nil {
		return err
	}
	req.ContentLength = contentLength
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Platform", "open_platform")
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("123_open: upload http %d: %s", resp.StatusCode, string(data))
	}
	var baseResp BaseResp
	if err = json.Unmarshal(data, &baseResp); err != nil {
		return err
	}
	if baseResp.Code != 0 {
		return fmt.Errorf("123_open: upload slice %d failed: %s", partNumber, baseResp.Message)
	}
	return nil
}
