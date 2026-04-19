package halalcloudopen

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/halalcloud/golang-sdk-lite/halalcloud/apiclient"
	sdkConfig "github.com/halalcloud/golang-sdk-lite/halalcloud/config"
	sdkModel "github.com/halalcloud/golang-sdk-lite/halalcloud/model"
	sdkUser "github.com/halalcloud/golang-sdk-lite/halalcloud/services/user"
	sdkUserFile "github.com/halalcloud/golang-sdk-lite/halalcloud/services/userfile"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
)

const (
	name        = "halalcloud_open"
	description = "HalalCloud Open"
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        name,
		Description: description,
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:      "refresh_token",
			Help:      "HalalCloud refresh token.",
			Sensitive: true,
		}, {
			Name:      config.ConfigClientID,
			Help:      "HalalCloud client ID.",
			Required:  true,
			Sensitive: true,
		}, {
			Name:      config.ConfigClientSecret,
			Help:      "HalalCloud client secret.",
			Required:  true,
			Sensitive: true,
		}, {
			Name:     "host",
			Help:     "HalalCloud Open API host.",
			Default:  "openapi.2dland.cn",
			Advanced: true,
		}, {
			Name:     "timeout",
			Help:     "API timeout in seconds.",
			Default:  60,
			Advanced: true,
		}, {
			Name:     "upload_thread",
			Help:     "Preferred upload concurrency. The current implementation uploads sequentially.",
			Default:  3,
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			Default: (encoder.Display |
				encoder.EncodeBackQuote |
				encoder.EncodeDoubleQuote |
				encoder.EncodeLtGt |
				encoder.EncodeLeftSpace |
				encoder.EncodeInvalidUtf8),
		}},
	})
}

type Options struct {
	RefreshToken string               `config:"refresh_token"`
	ClientID     string               `config:"client_id"`
	ClientSecret string               `config:"client_secret"`
	Host         string               `config:"host"`
	Timeout      int                  `config:"timeout"`
	UploadThread int                  `config:"upload_thread"`
	Enc          encoder.MultiEncoder `config:"encoding"`
}

type configStore struct {
	refreshTokenFunc func(string) error
	configs          sync.Map
}

func (c *configStore) GetConfig(key string) (string, error) {
	value, ok := c.configs.Load(key)
	if !ok {
		return "", nil
	}
	return value.(string), nil
}

func (c *configStore) SetConfig(key, value string) error {
	c.configs.Store(key, value)
	return nil
}

func (c *configStore) DeleteConfig(key string) error {
	c.configs.Delete(key)
	return nil
}

func (c *configStore) ListConfigs() (map[string]string, error) {
	out := map[string]string{}
	c.configs.Range(func(key, value any) bool {
		out[key.(string)] = value.(string)
		return true
	})
	return out, nil
}

func (c *configStore) ClearConfigs() error {
	c.configs = sync.Map{}
	return nil
}

func (c *configStore) GetRefreshToken() (string, error) { return c.GetConfig("refresh_token") }

func (c *configStore) SetRefreshToken(token string) error {
	c.configs.Store("refresh_token", token)
	if c.refreshTokenFunc != nil {
		return c.refreshTokenFunc(token)
	}
	return nil
}

func (c *configStore) SetAccessToken(token string) error {
	c.configs.Store("access_token", token)
	return nil
}

func (c *configStore) GetAccessToken() (string, error) { return c.GetConfig("access_token") }

func (c *configStore) SetToken(accessToken, refreshToken string, expiresIn int64) error {
	c.configs.Store("access_token", accessToken)
	c.configs.Store("refresh_token", refreshToken)
	c.configs.Store("expires_in", fmt.Sprintf("%d", expiresIn))
	if c.refreshTokenFunc != nil {
		return c.refreshTokenFunc(refreshToken)
	}
	return nil
}

var _ sdkConfig.ConfigStore = (*configStore)(nil)

type Fs struct {
	name         string
	originalName string
	root         string
	rootMissing  bool
	opt          Options
	features     *fs.Features
	httpClient   *http.Client
	configStore  *configStore
	client       *apiclient.Client
	userService  *sdkUser.UserService
	fileService  *sdkUserFile.UserFileService
}

type Object struct {
	fs      *Fs
	file    *sdkUserFile.File
	remote  string
	size    int64
	modTime time.Time
}

var (
	_ fs.Fs             = (*Fs)(nil)
	_ fs.Abouter        = (*Fs)(nil)
	_ fs.Copier         = (*Fs)(nil)
	_ fs.Mover          = (*Fs)(nil)
	_ fs.Purger         = (*Fs)(nil)
	_ fs.PutUncheckeder = (*Fs)(nil)
	_ fs.Object         = (*Object)(nil)
)

func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}
	originalName := name
	if idx := strings.IndexRune(name, '{'); idx > 0 {
		originalName = name[:idx]
	}
	root = strings.Trim(root, "/")
	if opt.UploadThread < 1 || opt.UploadThread > 32 {
		opt.UploadThread = 3
	}
	if opt.Timeout <= 0 {
		opt.Timeout = 60
	}
	if opt.Host == "" {
		opt.Host = "openapi.2dland.cn"
	}

	store := &configStore{
		refreshTokenFunc: func(token string) error {
			return config.SetValueAndSave(originalName, "refresh_token", token)
		},
	}
	if opt.RefreshToken != "" {
		_ = store.SetRefreshToken(opt.RefreshToken)
	}

	httpClient := fshttp.NewClient(ctx)
	client := apiclient.NewClient(httpClient, opt.Host, opt.ClientID, opt.ClientSecret, store, apiclient.WithTimeout(time.Second*time.Duration(opt.Timeout)))
	f := &Fs{
		name:         name,
		originalName: originalName,
		root:         root,
		opt:          *opt,
		httpClient:   httpClient,
		configStore:  store,
		client:       client,
		userService:  sdkUser.NewUserService(client),
		fileService:  sdkUserFile.NewUserFileService(client),
	}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		NoMultiThreading:        true,
		ReadMetadata:            true,
	}).Fill(ctx, f)
	if _, err := f.userService.Get(ctx, &sdkUser.User{}); err != nil {
		return nil, err
	}
	if root == "" {
		return f, nil
	}
	actualRoot, item, err := f.findDeepestExistingAbsoluteRemote(ctx, root)
	if err != nil {
		return nil, err
	}
	if item == nil {
		f.root = root
		f.rootMissing = true
		return f, nil
	}
	f.root = actualRoot
	if !item.Dir {
		newRoot := path.Dir(actualRoot)
		if newRoot == "." {
			newRoot = ""
		}
		tempF := *f
		tempF.root = newRoot
		return &tempF, fs.ErrorIsFile
	}
	return f, nil
}

func (f *Fs) Name() string             { return f.name }
func (f *Fs) Root() string             { return f.root }
func (f *Fs) String() string           { return fmt.Sprintf("HalalCloud Open root '%s'", f.root) }
func (f *Fs) Precision() time.Duration { return time.Second }
func (f *Fs) Hashes() hash.Set         { return hash.Set(hash.None) }
func (f *Fs) Features() *fs.Features   { return f.features }

func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	info, err := f.userService.GetStatisticsAndQuota(ctx)
	if err != nil {
		return nil, err
	}
	quota := info.DiskStatisticsQuota
	return &fs.Usage{
		Total: fs.NewUsageValue(quota.BytesQuota),
		Used:  fs.NewUsageValue(quota.BytesUsed),
		Free:  fs.NewUsageValue(quota.BytesFree),
	}, nil
}

func (f *Fs) fullPath(remote string) string {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		if f.root == "" {
			return "/"
		}
		return "/" + f.root
	}
	if f.root == "" {
		return "/" + f.opt.Enc.FromStandardPath(remote)
	}
	return "/" + path.Join(f.root, f.opt.Enc.FromStandardPath(remote))
}

func fileModTime(file *sdkUserFile.File) time.Time {
	if file.UpdateTs != 0 {
		return time.UnixMilli(file.UpdateTs)
	}
	return time.UnixMilli(file.CreateTs)
}

func (f *Fs) newObject(remote string, file *sdkUserFile.File) *Object {
	return &Object{
		fs:      f,
		file:    file,
		remote:  remote,
		size:    file.Size,
		modTime: fileModTime(file),
	}
}

func (f *Fs) listPath(ctx context.Context, parentPath string) ([]*sdkUserFile.File, error) {
	parent := &sdkUserFile.File{Path: parentPath}
	if parentPath == "/" || parentPath == "" {
		parent.Path = "/"
	}
	out := make([]*sdkUserFile.File, 0)
	token := ""
	for {
		result, err := f.fileService.List(ctx, &sdkUserFile.FileListRequest{
			Parent: parent,
			ListInfo: &sdkModel.ScanListRequest{
				Limit: 100,
				Token: token,
			},
		})
		if err != nil {
			return nil, err
		}
		out = append(out, result.Files...)
		if result.ListInfo == nil || result.ListInfo.Token == "" {
			break
		}
		token = result.ListInfo.Token
		parent = &sdkUserFile.File{Path: parentPath}
	}
	return out, nil
}

func (f *Fs) findItemByRemote(ctx context.Context, remote string) (*sdkUserFile.File, error) {
	return f.findItemByAbsoluteRemote(ctx, f.fullPath(remote))
}

func (f *Fs) findItemByAbsoluteRemote(ctx context.Context, remote string) (*sdkUserFile.File, error) {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		return &sdkUserFile.File{Path: "/", Dir: true}, nil
	}
	parent := path.Dir("/" + remote)
	if parent == "." {
		parent = "/"
	}
	leaf := path.Base(remote)
	items, err := f.listPath(ctx, parent)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if item.Name == f.opt.Enc.FromStandardName(leaf) {
			return item, nil
		}
	}
	return nil, fs.ErrorObjectNotFound
}

func (f *Fs) findDeepestExistingAbsoluteRemote(ctx context.Context, remote string) (string, *sdkUserFile.File, error) {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		return "", nil, nil
	}
	parts := strings.Split(remote, "/")
	current := ""
	var last *sdkUserFile.File
	for i, part := range parts {
		next := part
		if current != "" {
			next = path.Join(current, part)
		}
		item, err := f.findItemByAbsoluteRemote(ctx, next)
		if err != nil {
			if errors.Is(err, fs.ErrorObjectNotFound) {
				if i == len(parts)-1 {
					return remote, nil, nil
				}
				return current, nil, nil
			}
			return "", nil, err
		}
		current = next
		last = item
	}
	return current, last, nil
}

func (f *Fs) ensureDir(ctx context.Context, dir string) error {
	if f.rootMissing {
		parts := strings.Split(strings.Trim(f.root, "/"), "/")
		current := ""
		for _, part := range parts {
			if part == "" {
				continue
			}
			next := part
			if current != "" {
				next = path.Join(current, part)
			}
			item, err := f.findItemByAbsoluteRemote(ctx, next)
			if err == nil {
				if !item.Dir {
					return fs.ErrorIsFile
				}
				current = next
				continue
			}
			if !errors.Is(err, fs.ErrorObjectNotFound) {
				return err
			}
			parentPath := "/"
			if current != "" {
				parentPath = "/" + current
			}
			if _, err = f.fileService.Create(ctx, &sdkUserFile.File{
				Path: parentPath,
				Name: part,
			}); err != nil {
				return err
			}
			current = next
		}
		f.rootMissing = false
	}
	dir = strings.Trim(dir, "/")
	if dir == "" {
		return nil
	}
	parts := strings.Split(dir, "/")
	current := ""
	for _, part := range parts {
		next := part
		if current != "" {
			next = path.Join(current, part)
		}
		item, err := f.findItemByRemote(ctx, next)
		if err == nil {
			if !item.Dir {
				return fs.ErrorIsFile
			}
			current = next
			continue
		}
		if !errors.Is(err, fs.ErrorObjectNotFound) {
			return err
		}
		parentPath := f.fullPath(current)
		if parentPath == "" {
			parentPath = "/"
		}
		if _, err = f.fileService.Create(ctx, &sdkUserFile.File{
			Path: parentPath,
			Name: f.opt.Enc.FromStandardName(part),
		}); err != nil {
			return err
		}
		current = next
	}
	return nil
}

func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	if f.rootMissing && dir == "" {
		return entries, nil
	}
	dirPath := f.fullPath(dir)
	if dirPath == "" {
		dirPath = "/"
	}
	items, err := f.listPath(ctx, dirPath)
	if err != nil {
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return nil, fs.ErrorDirNotFound
		}
		return nil, err
	}
	for _, item := range items {
		remote := path.Join(dir, f.opt.Enc.ToStandardName(item.Name))
		if item.Dir {
			entries = append(entries, fs.NewDir(remote, fileModTime(item)).SetID(item.Identity))
			continue
		}
		entries = append(entries, f.newObject(remote, item))
	}
	return entries, nil
}

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	if f.rootMissing {
		return nil, fs.ErrorObjectNotFound
	}
	item, err := f.findItemByRemote(ctx, remote)
	if err != nil {
		return nil, err
	}
	if item.Dir {
		return nil, fs.ErrorIsDir
	}
	return f.newObject(remote, item), nil
}

func (f *Fs) readBackObject(ctx context.Context, src fs.ObjectInfo) (fs.Object, error) {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		obj, err := f.NewObject(ctx, src.Remote())
		if err == nil {
			return obj, nil
		}
		if !errors.Is(err, fs.ErrorObjectNotFound) {
			return nil, err
		}
		lastErr = err
		time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
	}
	return &Object{fs: f, remote: src.Remote(), size: src.Size(), modTime: src.ModTime(ctx)}, lastErr
}

func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	if src.Size() < 0 {
		return nil, errors.New("halalcloud_open requires known size")
	}
	if err := f.upload(ctx, in, src, options...); err != nil {
		return nil, err
	}
	return f.readBackObject(ctx, src)
}

func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}

func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	err := f.ensureDir(ctx, dir)
	if err == nil {
		f.rootMissing = false
	}
	return err
}

func (f *Fs) Rmdir(ctx context.Context, dir string) error { return fs.ErrorNotImplemented }

func (f *Fs) Purge(ctx context.Context, dir string) error {
	item, err := f.findItemByRemote(ctx, dir)
	if err != nil {
		return err
	}
	_, err = f.fileService.Delete(ctx, &sdkUserFile.BatchOperationRequest{
		Source: []*sdkUserFile.File{{Identity: item.Identity, Path: item.Path}},
	})
	return err
}

func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantCopy
	}
	parentRemote := path.Dir(remote)
	if parentRemote == "." {
		parentRemote = ""
	}
	if err := f.ensureDir(ctx, parentRemote); err != nil {
		return nil, err
	}
	dstPath := f.fullPath(parentRemote)
	if dstPath == "" {
		dstPath = "/"
	}
	_, err := f.fileService.Copy(ctx, &sdkUserFile.BatchOperationRequest{
		Source: []*sdkUserFile.File{{Identity: srcObj.file.Identity, Path: srcObj.file.Path}},
		Dest:   &sdkUserFile.File{Path: dstPath},
	})
	if err != nil {
		return nil, err
	}
	if path.Base(srcObj.remote) != path.Base(remote) {
		obj, err := f.NewObject(ctx, path.Join(parentRemote, path.Base(srcObj.remote)))
		if err != nil {
			return nil, err
		}
		return f.Move(ctx, obj, remote)
	}
	return f.NewObject(ctx, remote)
}

func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantMove
	}
	parentRemote := path.Dir(remote)
	if parentRemote == "." {
		parentRemote = ""
	}
	if err := f.ensureDir(ctx, parentRemote); err != nil {
		return nil, err
	}
	dstPath := f.fullPath(parentRemote)
	if dstPath == "" {
		dstPath = "/"
	}
	srcParent := path.Dir(srcObj.file.Path)
	if srcParent != dstPath {
		if _, err := f.fileService.Move(ctx, &sdkUserFile.BatchOperationRequest{
			Source: []*sdkUserFile.File{{Identity: srcObj.file.Identity, Path: srcObj.file.Path}},
			Dest:   &sdkUserFile.File{Path: dstPath},
		}); err != nil {
			return nil, err
		}
	}
	if srcObj.file.Name != f.opt.Enc.FromStandardName(path.Base(remote)) {
		if _, err := f.fileService.Rename(ctx, &sdkUserFile.File{
			Path: srcObj.file.Path,
			Name: f.opt.Enc.FromStandardName(path.Base(remote)),
		}); err != nil {
			return nil, err
		}
	}
	return f.NewObject(ctx, remote)
}

func (o *Object) Fs() fs.Info                            { return o.fs }
func (o *Object) String() string                         { return o.remote }
func (o *Object) Remote() string                         { return o.remote }
func (o *Object) ModTime(ctx context.Context) time.Time  { return o.modTime }
func (o *Object) Size() int64                            { return o.size }
func (o *Object) Storable() bool                         { return true }
func (o *Object) ID() string                             { return o.file.Identity }
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	info, err := o.fs.fileService.GetDirectDownloadAddress(ctx, &sdkUserFile.DirectDownloadRequest{
		Identity: o.file.Identity,
		Path:     o.file.Path,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, info.DownloadAddress, nil)
	if err != nil {
		return nil, err
	}
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	resp, err := o.fs.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		defer resp.Body.Close()
		return nil, fmt.Errorf("halalcloud_open: download http %d", resp.StatusCode)
	}
	return resp.Body, nil
}

func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	if src.Size() < 0 {
		return errors.New("halalcloud_open requires known size")
	}
	return o.fs.upload(ctx, in, src, options...)
}

func (o *Object) Remove(ctx context.Context) error {
	_, err := o.fs.fileService.Delete(ctx, &sdkUserFile.BatchOperationRequest{
		Source: []*sdkUserFile.File{{Identity: o.file.Identity, Path: o.file.Path}},
	})
	return err
}
