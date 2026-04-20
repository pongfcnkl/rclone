package oneguangyapan

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/fserrors"
)

type readCloserWithReader struct {
	io.Reader
	io.Closer
}

type reopenableSource struct {
	open func(context.Context, ...fs.OpenOption) (io.ReadCloser, error)
}

func newReopenableSource(src fs.ObjectInfo) (*reopenableSource, error) {
	if src.Size() < 0 {
		return nil, errors.New("1guangyapan upload requires known size")
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
	return nil, fmt.Errorf("1guangyapan upload source is not reopenable (src=%T remote=%q)", src, src.Remote())
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
	name := f.opt.Enc.FromStandardName(path.Base(src.Remote()))

	token, code, err := f.getUploadToken(ctx, parent.FileID, name, src.Size())
	if err != nil {
		return err
	}
	taskID := strings.TrimSpace(token.TaskID)
	if code == 156 {
		if taskID == "" {
			return errors.New("instant upload returned empty task id")
		}
		return f.waitUploadTaskInfo(ctx, taskID)
	}
	client, err := f.newOSSClient(token)
	if err != nil {
		return err
	}

	source, sourceErr := newReopenableSource(src)
	if sourceErr == nil {
		if err = f.multipartUploadFromSource(ctx, client, token, source, src, acc, transfer, options...); err != nil {
			return err
		}
	} else {
		if err = f.multipartUploadFromReader(ctx, client, token, baseIn, src, acc, transfer, options...); err != nil {
			return err
		}
	}

	if taskID == "" {
		return nil
	}
	return f.waitUploadTaskInfo(ctx, taskID)
}

func (f *Fs) getUploadToken(ctx context.Context, parentID, name string, size int64) (*uploadTokenData, int, error) {
	var out uploadTokenResp
	err := f.postAPI(ctx, "/nd.bizuserres.s/v1/get_res_center_token", map[string]any{
		"capacity": 2,
		"name":     name,
		"parentId": parentID,
		"res": map[string]any{
			"fileSize": size,
		},
	}, &out)
	if err != nil {
		return nil, 0, err
	}
	msg := strings.TrimSpace(out.Msg)
	if msg != "" && !strings.EqualFold(msg, "success") {
		return nil, out.Code, fmt.Errorf("get upload token failed: %s", msg)
	}
	if out.Data.TaskID == "" {
		return nil, out.Code, errors.New("get upload token failed: empty task id")
	}
	if out.Data.AccessKeyID == "" {
		out.Data.AccessKeyID = out.Data.Creds.AccessKeyID
	}
	if out.Data.SecretAccessKey == "" {
		out.Data.SecretAccessKey = out.Data.Creds.SecretAccessKey
	}
	if out.Data.SessionToken == "" {
		out.Data.SessionToken = out.Data.Creds.SessionToken
	}
	if strings.TrimSpace(out.Data.EndPoint) == "" {
		out.Data.EndPoint = strings.TrimSpace(out.Data.FullEndPoint)
	}
	if strings.TrimSpace(out.Data.EndPoint) != "" && !strings.HasPrefix(out.Data.EndPoint, "http://") && !strings.HasPrefix(out.Data.EndPoint, "https://") {
		if strings.TrimSpace(out.Data.FullEndPoint) != "" {
			out.Data.EndPoint = strings.TrimSpace(out.Data.FullEndPoint)
		} else if strings.TrimSpace(out.Data.BucketName) != "" {
			host := strings.TrimSpace(out.Data.EndPoint)
			prefix := strings.TrimSpace(out.Data.BucketName) + "."
			if strings.HasPrefix(host, prefix) {
				out.Data.EndPoint = "https://" + host
			} else {
				out.Data.EndPoint = "https://" + strings.TrimSpace(out.Data.BucketName) + "." + host
			}
		} else {
			out.Data.EndPoint = "https://" + strings.TrimSpace(out.Data.EndPoint)
		}
	}
	return &out.Data, out.Code, nil
}

func (f *Fs) waitUploadTaskInfo(ctx context.Context, taskID string) error {
	const (
		maxTry   = 300
		interval = time.Second
	)
	for i := 0; i < maxTry; i++ {
		var out taskInfoResp
		if err := f.postAPI(ctx, "/nd.bizuserres.s/v1/file/get_info_by_task_id", map[string]any{"taskId": taskID}, &out); err != nil {
			return err
		}
		if out.Data.FileID != "" {
			return nil
		}
		switch out.Code {
		case 145, 146, 147, 155, 163, 0:
		default:
			if strings.TrimSpace(out.Msg) != "" {
				return fmt.Errorf("upload task failed: code=%d msg=%s", out.Code, strings.TrimSpace(out.Msg))
			}
		}
		if i == maxTry-1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
	return fmt.Errorf("upload task %s timeout", taskID)
}

func (f *Fs) newOSSClient(token *uploadTokenData) (*oss.Client, error) {
	endpoint := normalizeOSSEndpoint(token.EndPoint, token.BucketName)
	if endpoint == "" || token.BucketName == "" || token.ObjectPath == "" || token.AccessKeyID == "" || token.SecretAccessKey == "" {
		return nil, errors.New("upload token is incomplete")
	}
	provider := credentials.NewStaticCredentialsProvider(token.AccessKeyID, token.SecretAccessKey, token.SessionToken)
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(provider).
		WithEndpoint(endpoint).
		WithRegion(normalizeOSSRegion(token.Region, endpoint))
	client := oss.NewClient(cfg)
	if client == nil {
		return nil, errors.New("failed to create OSS client")
	}
	return client, nil
}

func (f *Fs) multipartUploadFromSource(ctx context.Context, client *oss.Client, token *uploadTokenData, source *reopenableSource, src fs.ObjectInfo, acc *accounting.Account, transfer *accounting.Transfer, options ...fs.OpenOption) error {
	partSize := calcUploadPartSize(src.Size())
	initReq := &oss.InitiateMultipartUploadRequest{
		Bucket: oss.Ptr(token.BucketName),
		Key:    oss.Ptr(token.ObjectPath),
	}
	initReq.Parameters = map[string]string{"sequential": ""}
	uploadInfo, err := client.InitiateMultipartUpload(ctx, initReq)
	if err != nil {
		return err
	}
	completed := make([]oss.UploadPart, 0, (src.Size()+partSize-1)/partSize)
	for partNumber, offset := int32(1), int64(0); offset < src.Size(); partNumber, offset = partNumber+1, offset+partSize {
		curPartSize := partSize
		if remaining := src.Size() - offset; remaining < curPartSize {
			curPartSize = remaining
		}
		rc, err := source.OpenRange(ctx, offset, curPartSize)
		if err != nil {
			return abortMultipart(ctx, client, token, uploadInfo, err)
		}
		body := wrapUploadReader(ctx, rc, acc, transfer)
		req := &oss.UploadPartRequest{
			Bucket:     oss.Ptr(token.BucketName),
			Key:        oss.Ptr(token.ObjectPath),
			UploadId:   uploadInfo.UploadId,
			PartNumber: partNumber,
			Body:       body,
		}
		res, err := client.UploadPart(ctx, req)
		_ = body.Close()
		if err != nil {
			return abortMultipart(ctx, client, token, uploadInfo, err)
		}
		completed = append(completed, oss.UploadPart{
			PartNumber: partNumber,
			ETag:       res.ETag,
		})
	}
	_, err = client.CompleteMultipartUpload(ctx, &oss.CompleteMultipartUploadRequest{
		Bucket:   oss.Ptr(token.BucketName),
		Key:      oss.Ptr(token.ObjectPath),
		UploadId: uploadInfo.UploadId,
		CompleteMultipartUpload: &oss.CompleteMultipartUpload{
			Parts: completed,
		},
	})
	return err
}

func (f *Fs) multipartUploadFromReader(ctx context.Context, client *oss.Client, token *uploadTokenData, in io.Reader, src fs.ObjectInfo, acc *accounting.Account, transfer *accounting.Transfer, options ...fs.OpenOption) error {
	partSize := calcUploadPartSize(src.Size())
	initReq := &oss.InitiateMultipartUploadRequest{
		Bucket: oss.Ptr(token.BucketName),
		Key:    oss.Ptr(token.ObjectPath),
	}
	initReq.Parameters = map[string]string{"sequential": ""}
	uploadInfo, err := client.InitiateMultipartUpload(ctx, initReq)
	if err != nil {
		return err
	}
	completed := make([]oss.UploadPart, 0, (src.Size()+partSize-1)/partSize)
	var uploaded int64
	for partNumber := int32(1); uploaded < src.Size(); partNumber++ {
		curPartSize := partSize
		if remaining := src.Size() - uploaded; remaining < curPartSize {
			curPartSize = remaining
		}
		partReader := io.LimitReader(in, curPartSize)
		body := wrapUploadReader(ctx, io.NopCloser(partReader), acc, transfer)
		req := &oss.UploadPartRequest{
			Bucket:     oss.Ptr(token.BucketName),
			Key:        oss.Ptr(token.ObjectPath),
			UploadId:   uploadInfo.UploadId,
			PartNumber: partNumber,
			Body:       body,
		}
		res, err := client.UploadPart(ctx, req)
		_ = body.Close()
		if err != nil {
			return abortMultipart(ctx, client, token, uploadInfo, err)
		}
		completed = append(completed, oss.UploadPart{
			PartNumber: partNumber,
			ETag:       res.ETag,
		})
		uploaded += curPartSize
	}
	_, err = client.CompleteMultipartUpload(ctx, &oss.CompleteMultipartUploadRequest{
		Bucket:   oss.Ptr(token.BucketName),
		Key:      oss.Ptr(token.ObjectPath),
		UploadId: uploadInfo.UploadId,
		CompleteMultipartUpload: &oss.CompleteMultipartUpload{
			Parts: completed,
		},
	})
	return err
}

func wrapUploadReader(ctx context.Context, rc io.ReadCloser, acc *accounting.Account, transfer *accounting.Transfer) io.ReadCloser {
	if acc != nil {
		return readCloserWithReader{
			Reader: acc.WrapStream(rc),
			Closer: rc,
		}
	}
	if transfer != nil {
		return transfer.Account(ctx, rc)
	}
	return rc
}

func abortMultipart(ctx context.Context, client *oss.Client, token *uploadTokenData, uploadInfo *oss.InitiateMultipartUploadResult, uploadErr error) error {
	if uploadInfo != nil && uploadInfo.UploadId != nil {
		_, _ = client.AbortMultipartUpload(ctx, &oss.AbortMultipartUploadRequest{
			Bucket:   oss.Ptr(token.BucketName),
			Key:      oss.Ptr(token.ObjectPath),
			UploadId: uploadInfo.UploadId,
		})
	}
	if fserrors.ContextError(ctx, &uploadErr) {
		return uploadErr
	}
	return uploadErr
}

func calcUploadPartSize(size int64) int64 {
	const (
		mb = int64(1024 * 1024)
		gb = int64(1024 * 1024 * 1024)
	)
	switch {
	case size <= 100*mb:
		return 1 * mb
	case size <= 16*gb:
		return 2 * mb
	case size <= 160*gb:
		return 4 * mb
	default:
		return 8 * mb
	}
}

func normalizeOSSRegion(region, endpoint string) string {
	region = strings.TrimSpace(region)
	if region != "" {
		region = strings.TrimPrefix(region, "oss-")
		return region
	}
	u, err := url.Parse(endpoint)
	if err == nil {
		host := strings.ToLower(u.Host)
		if idx := strings.Index(host, "oss-"); idx >= 0 {
			host = host[idx+4:]
			if end := strings.IndexByte(host, '.'); end > 0 {
				return host[:end]
			}
		}
	}
	return "cn-hangzhou"
}
