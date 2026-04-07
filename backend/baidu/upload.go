package baidu

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/lib/kv"
)

var errUploadIDExpired = errors.New("uploadid expired")

type uploadProgress struct {
	UploadID   string
	UploadURL  string
	Pending    []int
	RemotePath string
	BlockList  []string
}

type putProgress struct {
	key string
	val *uploadProgress
}

func (op *putProgress) Do(ctx context.Context, b kv.Bucket) error {
	if op.val == nil {
		return b.Delete([]byte(op.key))
	}
	data, err := json.Marshal(op.val)
	if err != nil {
		return err
	}
	return b.Put([]byte(op.key), data)
}

type getProgress struct {
	key string
	out **uploadProgress
}

func (op *getProgress) Do(ctx context.Context, b kv.Bucket) error {
	data := b.Get([]byte(op.key))
	if len(data) == 0 {
		return nil
	}
	var state uploadProgress
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	*op.out = &state
	return nil
}

type reopenableSource struct {
	open func(context.Context, ...fs.OpenOption) (io.ReadCloser, error)
}

func newReopenableSource(src fs.ObjectInfo) (*reopenableSource, error) {
	if src.Size() < 0 {
		return nil, errors.New("baidu upload requires known size")
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
	return nil, fmt.Errorf("baidu upload requires reopenable source object; streaming readers without temp cache are unsupported (src=%T remote=%q fs=%T unwrap=%T)", src, src.Remote(), src.Fs(), fs.UnWrapObjectInfo(src))
}

func (s *reopenableSource) OpenRange(ctx context.Context, offset, size int64) (io.ReadCloser, error) {
	if size < 0 {
		return s.open(ctx, &fs.RangeOption{Start: offset, End: -1})
	}
	return s.open(ctx, &fs.RangeOption{Start: offset, End: offset + size - 1})
}

func removePendingPart(parts []int, target int) []int {
	out := make([]int, 0, len(parts))
	removed := false
	for _, part := range parts {
		if !removed && part == target {
			removed = true
			continue
		}
		out = append(out, part)
	}
	return out
}

func (f *Fs) progressKey(fullPath, contentMD5 string) string {
	return f.originalName + ":" + fullPath + ":" + contentMD5
}

func (f *Fs) loadProgress(key string) (*uploadProgress, error) {
	if f.progressDB == nil {
		return nil, nil
	}
	var out *uploadProgress
	err := f.progressDB.Do(false, &getProgress{key: key, out: &out})
	if err == kv.ErrEmpty {
		return nil, nil
	}
	return out, err
}

func (f *Fs) saveProgress(key string, state *uploadProgress) error {
	if f.progressDB == nil {
		return nil
	}
	return f.progressDB.Do(true, &putProgress{key: key, val: state})
}

type hashInfo struct {
	contentMD5   string
	sliceMD5     string
	blockList    []string
	blockListStr string
}

func (f *Fs) computeHashes(ctx context.Context, src fs.ObjectInfo) (*hashInfo, error) {
	source, err := newReopenableSource(src)
	if err != nil {
		return nil, err
	}
	rc, err := source.OpenRange(ctx, 0, -1)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	size := src.Size()
	partSize := f.getSliceSize(size)
	partCount := 1
	if size > partSize {
		partCount = int((size + partSize - 1) / partSize)
	}

	fileMD5 := md5.New()
	sliceMD5 := md5.New()
	first256kRemain := int64(256 * 1024)
	blocks := make([]string, 0, partCount)
	buf := make([]byte, 256*1024)

	for part := 0; part < partCount; part++ {
		partHasher := md5.New()
		remain := partSize
		if last := size - int64(part)*partSize; last < remain {
			remain = last
		}
		for remain > 0 {
			readLen := int64(len(buf))
			if readLen > remain {
				readLen = remain
			}
			n, err := io.ReadFull(rc, buf[:readLen])
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				return nil, err
			}
			chunk := buf[:n]
			if len(chunk) == 0 {
				return nil, io.ErrUnexpectedEOF
			}
			_, _ = fileMD5.Write(chunk)
			_, _ = partHasher.Write(chunk)
			if first256kRemain > 0 {
				nn := int64(len(chunk))
				if nn > first256kRemain {
					nn = first256kRemain
				}
				_, _ = sliceMD5.Write(chunk[:nn])
				first256kRemain -= nn
			}
			remain -= int64(n)
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
		}
		blocks = append(blocks, hex.EncodeToString(partHasher.Sum(nil)))
	}
	blockJSON, err := json.Marshal(blocks)
	if err != nil {
		return nil, err
	}
	return &hashInfo{
		contentMD5:   hex.EncodeToString(fileMD5.Sum(nil)),
		sliceMD5:     hex.EncodeToString(sliceMD5.Sum(nil)),
		blockList:    blocks,
		blockListStr: string(blockJSON),
	}, nil
}

func (f *Fs) upload(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	if src.Size() < 1 {
		return errors.New("baidu does not allow empty files")
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
			if transfer != nil {
				transfer.Done(ctx, nil)
			}
		}()
	}
	source, err := newReopenableSource(src)
	if err != nil {
		if transfer != nil {
			transfer.Done(ctx, err)
			transfer = nil
		}
		return err
	}
	hashes, err := f.computeHashes(ctx, src)
	if err != nil {
		if transfer != nil {
			transfer.Done(ctx, err)
			transfer = nil
		}
		return err
	}
	parent := path.Dir(src.Remote())
	if parent == "." {
		parent = ""
	}
	if err = f.ensureDir(ctx, parent); err != nil {
		if transfer != nil {
			transfer.Done(ctx, err)
			transfer = nil
		}
		return err
	}
	fullPath := f.fullPath(src.Remote())
	modTime := src.ModTime(ctx).Unix()
	if modTime == 0 {
		modTime = time.Now().Unix()
	}
	progressKey := f.progressKey(fullPath, hashes.contentMD5)
	state, err := f.loadProgress(progressKey)
	if err != nil {
		if transfer != nil {
			transfer.Done(ctx, err)
			transfer = nil
		}
		return err
	}

	precreate, err := f.apiPrecreate(ctx, fullPath, src.Size(), hashes.blockListStr, hashes.contentMD5, hashes.sliceMD5, modTime, modTime)
	if err != nil {
		if transfer != nil {
			transfer.Done(ctx, err)
			transfer = nil
		}
		return err
	}
	if precreate.ReturnType == 2 {
		_ = f.saveProgress(progressKey, nil)
		return nil
	}
	if state != nil && state.UploadID != "" && state.RemotePath == fullPath {
		precreate.UploadID = state.UploadID
		precreate.UploadURL = state.UploadURL
		precreate.BlockList = append([]int(nil), state.Pending...)
		if len(state.BlockList) == len(hashes.blockList) {
			hashes.blockList = append([]string(nil), state.BlockList...)
			if blockJSON, err := json.Marshal(hashes.blockList); err == nil {
				hashes.blockListStr = string(blockJSON)
			}
		}
	}
	if precreate.UploadURL == "" {
		precreate.UploadURL, _ = f.getUploadURL(ctx, fullPath, precreate.UploadID)
	}

	uploadOnce := func() error {
		pending := append([]int(nil), precreate.BlockList...)
		for _, part := range pending {
			offset := int64(part) * f.getSliceSize(src.Size())
			size := f.getSliceSize(src.Size())
			if remain := src.Size() - offset; remain < size {
				size = remain
			}
			rc, err := source.OpenRange(ctx, offset, size)
			if err != nil {
				if transfer != nil {
					transfer.Done(ctx, err)
					transfer = nil
				}
				return err
			}
			section := io.Reader(rc)
			if acc != nil {
				section = acc.WrapStream(section)
			} else if transfer != nil {
				section = transfer.Account(ctx, rc)
			}
			md5sum, err := f.uploadSlice(ctx, precreate.UploadURL, fullPath, precreate.UploadID, part, path.Base(fullPath), section, size, options)
			_ = rc.Close()
			if err != nil {
				if transfer != nil {
					transfer.Done(ctx, err)
					transfer = nil
				}
				return err
			}
			fs.Debugf(f, "baidu uploaded part: path=%q part=%d size=%d returned_md5=%q", fullPath, part, size, md5sum)
			if md5sum != "" && part >= 0 && part < len(hashes.blockList) {
				hashes.blockList[part] = md5sum
				if blockJSON, err := json.Marshal(hashes.blockList); err == nil {
					hashes.blockListStr = string(blockJSON)
				}
			}
			precreate.BlockList = removePendingPart(precreate.BlockList, part)
			if err = f.saveProgress(progressKey, &uploadProgress{
				UploadID:   precreate.UploadID,
				UploadURL:  precreate.UploadURL,
				Pending:    append([]int(nil), precreate.BlockList...),
				RemotePath: fullPath,
				BlockList:  append([]string(nil), hashes.blockList...),
			}); err != nil {
				return err
			}
		}
		return nil
	}

	if err = uploadOnce(); err != nil {
		if !errors.Is(err, errUploadIDExpired) {
			if transfer != nil {
				transfer.Done(ctx, err)
				transfer = nil
			}
			return err
		}
		precreate, err = f.apiPrecreate(ctx, fullPath, src.Size(), hashes.blockListStr, "", "", modTime, modTime)
		if err != nil {
			if transfer != nil {
				transfer.Done(ctx, err)
				transfer = nil
			}
			return err
		}
		if precreate.ReturnType == 2 {
			_ = f.saveProgress(progressKey, nil)
			return nil
		}
		if precreate.UploadURL == "" {
			precreate.UploadURL, _ = f.getUploadURL(ctx, fullPath, precreate.UploadID)
		}
		if err = f.saveProgress(progressKey, &uploadProgress{
			UploadID:   precreate.UploadID,
			UploadURL:  precreate.UploadURL,
			Pending:    append([]int(nil), precreate.BlockList...),
			RemotePath: fullPath,
			BlockList:  append([]string(nil), hashes.blockList...),
		}); err != nil {
			if transfer != nil {
				transfer.Done(ctx, err)
				transfer = nil
			}
			return err
		}
		if err = uploadOnce(); err != nil {
			if transfer != nil {
				transfer.Done(ctx, err)
				transfer = nil
			}
			return err
		}
	}

	if err = f.apiCreate(ctx, fullPath, src.Size(), 0, precreate.UploadID, hashes.blockListStr, modTime, modTime); err != nil {
		if transfer != nil {
			transfer.Done(ctx, err)
			transfer = nil
		}
		return err
	}
	_ = f.saveProgress(progressKey, nil)
	return nil
}
