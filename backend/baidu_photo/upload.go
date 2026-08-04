package baiduphoto

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/lib/rest"
)

const (
	uploadPartSize = int64(1 << 22)
	sliceMD5Size   = int64(1 << 18)
)

type readCloserWithReader struct {
	io.Reader
	io.Closer
}

type reopenableSource struct {
	open func(context.Context, ...fs.OpenOption) (io.ReadCloser, error)
}

type uploadHashes struct {
	contentMD5   string
	sliceMD5     string
	blockList    []string
	blockListStr string
}

func newReopenableSource(src fs.ObjectInfo) (*reopenableSource, error) {
	if src.Size() < 0 {
		return nil, errors.New("baidu_photo upload requires known size")
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
	return nil, fmt.Errorf("baidu_photo upload requires reopenable source object; streaming readers without temp cache are unsupported (src=%T remote=%q)", src, src.Remote())
}

func (s *reopenableSource) OpenRange(ctx context.Context, offset, size int64) (io.ReadCloser, error) {
	if size < 0 {
		return s.open(ctx, &fs.RangeOption{Start: offset, End: -1})
	}
	return s.open(ctx, &fs.RangeOption{Start: offset, End: offset + size - 1})
}

func (f *Fs) upload(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	if src.Size() == 0 {
		return errors.New("baidu_photo does not allow empty files")
	}
	_, acc := accounting.UnWrapAccounting(in)
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
	source, err := newReopenableSource(src)
	if err != nil {
		return err
	}
	hashes, err := computeUploadHashes(ctx, source, src.Size())
	if err != nil {
		return err
	}
	remote := strings.Trim(src.Remote(), "/")
	if f.root != "" {
		remote = path.Join(f.root, remote)
	}
	targetAlbum, filename, err := f.uploadTarget(ctx, remote)
	if err != nil {
		return err
	}
	if targetAlbum != nil {
		fs.Debugf(src, "baidu_photo: uploading to album %q (%s) as %q", targetAlbum.Title, targetAlbum.AlbumID, filename)
	} else {
		fs.Debugf(src, "baidu_photo: uploading to root as %q", filename)
	}
	rootPath := "/" + f.opt.Enc.FromStandardName(filename)
	modTime := src.ModTime(ctx).Unix()
	precreate, err := f.apiPrecreate(ctx, rootPath, src.Size(), hashes, modTime, modTime)
	if err != nil {
		return err
	}
	switch precreate.ReturnType {
	case 1:
		if err = f.uploadMissingBlocks(ctx, source, acc, transfer, src, rootPath, precreate, hashes, options...); err != nil {
			return err
		}
		fallthrough
	case 2:
		if err = f.apiCreate(ctx, rootPath, src.Size(), precreate.UploadID, hashes.blockListStr, modTime, modTime, precreate); err != nil {
			return err
		}
		fallthrough
	case 3:
		rootFile := precreate.Data.toFile()
		if rootFile.Fsid == 0 && precreate.Data.FsID == 0 {
			if found, findErr := f.findRootFileByName(ctx, filename); findErr == nil {
				rootFile = found
			}
		}
		if targetAlbum != nil {
			if rootFile == nil || rootFile.Fsid == 0 {
				return errors.New("baidu_photo: uploaded root file is missing fsid")
			}
			_, err = f.apiAddAlbumFile(ctx, targetAlbum, *rootFile)
		}
		return err
	default:
		return fmt.Errorf("baidu_photo: unsupported precreate return_type %d", precreate.ReturnType)
	}
}

func (f *Fs) uploadTarget(ctx context.Context, remote string) (*album, string, error) {
	if f.rootAlbum != nil {
		return f.rootAlbum, path.Base(remote), nil
	}
	dir, leaf := path.Split(remote)
	dir = strings.Trim(dir, "/")
	if dir == "" {
		return nil, leaf, nil
	}
	if strings.Contains(dir, "/") {
		return nil, "", fs.ErrorCantDirMove
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	a, err := f.findAlbumByName(ctx, dir)
	if err != nil {
		if !errors.Is(err, fs.ErrorDirNotFound) {
			return nil, "", err
		}
		a, err = f.apiCreateAlbum(ctx, f.opt.Enc.FromStandardName(dir))
		if err != nil {
			if found, findErr := f.findAlbumByName(ctx, dir); findErr == nil {
				return found, leaf, nil
			}
			return nil, "", fmt.Errorf("baidu_photo: failed to create upload album %q: %w", dir, err)
		}
	}
	return a, leaf, nil
}

func computeUploadHashes(ctx context.Context, source *reopenableSource, size int64) (*uploadHashes, error) {
	rc, err := source.OpenRange(ctx, 0, -1)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	count := int64(1)
	if size > uploadPartSize {
		count = (size + uploadPartSize - 1) / uploadPartSize
	}
	fileMD5 := md5.New()
	firstSliceMD5 := md5.New()
	blocks := make([]string, 0, count)
	firstRemain := sliceMD5Size
	buf := make([]byte, 256*1024)
	for part := int64(0); part < count; part++ {
		partMD5 := md5.New()
		remain := uploadPartSize
		if left := size - part*uploadPartSize; left < remain {
			remain = left
		}
		for remain > 0 {
			readLen := int64(len(buf))
			if readLen > remain {
				readLen = remain
			}
			n, err := io.ReadFull(rc, buf[:readLen])
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, err
			}
			if n == 0 {
				return nil, io.ErrUnexpectedEOF
			}
			chunk := buf[:n]
			_, _ = fileMD5.Write(chunk)
			_, _ = partMD5.Write(chunk)
			if firstRemain > 0 {
				nn := int64(len(chunk))
				if nn > firstRemain {
					nn = firstRemain
				}
				_, _ = firstSliceMD5.Write(chunk[:nn])
				firstRemain -= nn
			}
			remain -= int64(n)
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
		}
		blocks = append(blocks, hex.EncodeToString(partMD5.Sum(nil)))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}
	blockJSON, err := json.Marshal(blocks)
	if err != nil {
		return nil, err
	}
	return &uploadHashes{
		contentMD5:   hex.EncodeToString(fileMD5.Sum(nil)),
		sliceMD5:     hex.EncodeToString(firstSliceMD5.Sum(nil)),
		blockList:    blocks,
		blockListStr: string(blockJSON),
	}, nil
}

func (f *Fs) apiPrecreate(ctx context.Context, rootPath string, size int64, hashes *uploadHashes, mtime, ctime int64) (*precreateResp, error) {
	form := url.Values{
		"autoinit":    {"1"},
		"isdir":       {"0"},
		"rtype":       {"3"},
		"ctype":       {"11"},
		"path":        {rootPath},
		"size":        {strconv.FormatInt(size, 10)},
		"slice-md5":   {hashes.sliceMD5},
		"content-md5": {hashes.contentMD5},
		"block_list":  {hashes.blockListStr},
	}
	if mtime > 0 {
		form.Set("local_mtime", strconv.FormatInt(mtime, 10))
		form.Set("local_ctime", strconv.FormatInt(ctime, 10))
	}
	fs.Debugf(f, "baidu_photo precreate request: path=%q size=%d content_md5=%q slice_md5=%q block_list=%s", rootPath, size, hashes.contentMD5, hashes.sliceMD5, hashes.blockListStr)
	var resp precreateResp
	_, err := f.call(ctx, http.MethodPost, fileAPIURLV1+"/precreate", url.Values{"bdstoken": {f.bdstoken}}, form, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (f *Fs) apiCreate(ctx context.Context, rootPath string, size int64, uploadID, blockList string, mtime, ctime int64, out *precreateResp) error {
	form := url.Values{
		"autoinit":   {"1"},
		"isdir":      {"0"},
		"rtype":      {"3"},
		"ctype":      {"11"},
		"path":       {rootPath},
		"size":       {strconv.FormatInt(size, 10)},
		"block_list": {blockList},
	}
	if uploadID != "" {
		form.Set("uploadid", uploadID)
	}
	if mtime > 0 {
		form.Set("local_mtime", strconv.FormatInt(mtime, 10))
		form.Set("local_ctime", strconv.FormatInt(ctime, 10))
	}
	if out == nil {
		out = &precreateResp{}
	}
	fs.Debugf(f, "baidu_photo create request: path=%q size=%d uploadid=%q block_list=%s", rootPath, size, uploadID, blockList)
	_, err := f.call(ctx, http.MethodPost, fileAPIURLV1+"/create", url.Values{"bdstoken": {f.bdstoken}}, form, out)
	return err
}

func (f *Fs) uploadMissingBlocks(ctx context.Context, source *reopenableSource, acc *accounting.Account, transfer *accounting.Transfer, src fs.ObjectInfo, rootPath string, precreate *precreateResp, hashes *uploadHashes, options ...fs.OpenOption) error {
	var progressAcc *accounting.Account
	for _, part := range precreate.BlockList {
		offset := int64(part) * uploadPartSize
		size := uploadPartSize
		if remain := src.Size() - offset; remain < size {
			size = remain
		}
		rc, err := source.OpenRange(ctx, offset, size)
		if err != nil {
			return err
		}
		bodyReader := io.Reader(rc)
		closeReader := io.Closer(rc)
		if acc != nil {
			bodyReader = acc.WrapStream(bodyReader)
		} else if transfer != nil {
			if progressAcc != nil {
				bodyReader = progressAcc.WrapStream(bodyReader)
			} else {
				progressAcc = transfer.Account(ctx, rc)
				bodyReader = progressAcc
			}
		}
		if closer, ok := bodyReader.(io.Closer); ok {
			closeReader = closer
		}
		md5sum, err := f.uploadSlice(ctx, rootPath, precreate.UploadID, part, path.Base(rootPath), bodyReader, size, options)
		_ = closeReader.Close()
		if err != nil {
			return err
		}
		if md5sum != "" && part >= 0 && part < len(hashes.blockList) {
			hashes.blockList[part] = md5sum
			if blockJSON, err := json.Marshal(hashes.blockList); err == nil {
				hashes.blockListStr = string(blockJSON)
			}
		}
	}
	return nil
}

func (f *Fs) uploadSlice(ctx context.Context, rootPath, uploadID string, partSeq int, fileName string, section io.Reader, size int64, options []fs.OpenOption) (string, error) {
	reqBody, contentType, overhead, err := rest.MultipartUpload(ctx, section, nil, "file", fileName)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://c3.pcs.baidu.com/rest/2.0/pcs/superfile2", reqBody)
	if err != nil {
		return "", err
	}
	q := req.URL.Query()
	q.Set("method", "upload")
	q.Set("path", rootPath)
	q.Set("partseq", strconv.Itoa(partSeq))
	q.Set("uploadid", uploadID)
	q.Set("app_id", "16051585")
	req.URL.RawQuery = q.Encode()
	req.ContentLength = overhead + size
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Cookie", f.opt.Cookie)
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Referer", "https://photo.baidu.com/")
	req.Header.Set("Origin", "https://photo.baidu.com")
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", fmt.Errorf("baidu_photo: upload http %d: %s", resp.StatusCode, string(data))
	}
	var out uploadSliceResp
	if err = json.Unmarshal(data, &out); err != nil {
		return "", err
	}
	if out.ErrorCode != 0 || out.Errno != 0 {
		return "", fmt.Errorf("baidu_photo: upload failed: %s", string(data))
	}
	return out.MD5, nil
}

func (f uploadFile) toFile() *file {
	return &file{
		Fsid:  f.FsID,
		Path:  f.Path,
		Size:  f.Size,
		Ctime: f.Ctime,
		Mtime: f.Mtime,
		MD5:   f.MD5,
	}
}
