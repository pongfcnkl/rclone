package baidu

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/baidu/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/kv"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

const (
	name                 = "baidu"
	description          = "Baidu Netdisk"
	baseURL              = "https://pan.baidu.com"
	openAPIURL           = "https://openapi.baidu.com"
	defaultUploadAPI     = "https://d.pcs.baidu.com"
	defaultSliceSize     = 4 * fs.Mebi
	vipSliceSize         = 16 * fs.Mebi
	svipSliceSize        = 32 * fs.Mebi
	maxSliceNum          = 2048
	sliceStep            = 1 * fs.Mebi
	defaultUploadTimeout = 60
	minSleep             = 200 * time.Millisecond
	maxSleep             = 2 * time.Second
	decayConstant        = 2
	defaultUserAgent     = "pan.baidu.com"
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        name,
		Description: description,
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:      "refresh_token",
			Help:      "Baidu refresh token.",
			Required:  true,
			Sensitive: true,
		}, {
			Name:      config.ConfigClientID,
			Help:      "Baidu app client_id.",
			Required:  true,
			Sensitive: true,
		}, {
			Name:      config.ConfigClientSecret,
			Help:      "Baidu app client_secret.",
			Required:  true,
			Sensitive: true,
		}, {
			Name:      "access_token",
			Help:      "Cached Baidu access token.",
			Advanced:  true,
			Sensitive: true,
		}, {
			Name:     "order_by",
			Help:     "Directory listing sort field.",
			Default:  "name",
			Advanced: true,
		}, {
			Name:     "order_direction",
			Help:     "Directory listing sort direction.",
			Default:  "asc",
			Advanced: true,
		}, {
			Name:     "upload_api",
			Help:     "Fallback upload API endpoint.",
			Default:  defaultUploadAPI,
			Advanced: true,
		}, {
			Name:     "use_dynamic_upload_api",
			Help:     "Locate upload host dynamically.",
			Default:  true,
			Advanced: true,
		}, {
			Name:     "upload_timeout",
			Help:     "Per-slice upload timeout in seconds.",
			Default:  defaultUploadTimeout,
			Advanced: true,
		}, {
			Name:     "custom_upload_part_size",
			Help:     "Custom upload part size in bytes, 0 for automatic.",
			Default:  fs.SizeSuffix(0),
			Advanced: true,
		}, {
			Name:     "low_bandwidth_upload_mode",
			Help:     "Prefer smaller upload part sizes for low bandwidth links.",
			Default:  false,
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
	RefreshToken         string               `config:"refresh_token"`
	ClientID             string               `config:"client_id"`
	ClientSecret         string               `config:"client_secret"`
	AccessToken          string               `config:"access_token"`
	OrderBy              string               `config:"order_by"`
	OrderDirection       string               `config:"order_direction"`
	UploadAPI            string               `config:"upload_api"`
	UseDynamicUploadAPI  bool                 `config:"use_dynamic_upload_api"`
	UploadTimeout        int                  `config:"upload_timeout"`
	CustomUploadPartSize fs.SizeSuffix        `config:"custom_upload_part_size"`
	LowBandwidthMode     bool                 `config:"low_bandwidth_upload_mode"`
	Enc                  encoder.MultiEncoder `config:"encoding"`
}

type Fs struct {
	name         string
	originalName string
	root         string
	rootMissing  bool
	opt          Options
	features     *fs.Features
	client       *rest.Client
	pacer        *fs.Pacer
	httpClient   *http.Client
	progressDB   *kv.DB

	mu      sync.Mutex
	vipType int
}

type Object struct {
	fs           *Fs
	id           int64
	remote       string
	size         int64
	modTime      time.Time
	createdTime  time.Time
	downloadLink string
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
	client := rest.NewClient(fshttp.NewClient(ctx))
	client.SetRoot(baseURL)
	f := &Fs{
		name:         name,
		originalName: originalName,
		root:         root,
		opt:          *opt,
		client:       client,
		httpClient:   fshttp.NewClient(ctx),
		pacer:        fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
	}
	if f.opt.UploadAPI == "" {
		f.opt.UploadAPI = defaultUploadAPI
	}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		NoMultiThreading:        true,
	}).Fill(ctx, f)
	if kv.Supported() {
		db, err := kv.Start(ctx, "baidu-upload", f)
		if err != nil {
			return nil, err
		}
		f.progressDB = db
	}
	if err := f.init(ctx); err != nil {
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
	if item.IsDir == 0 {
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

func (f *Fs) findItemByRemote(ctx context.Context, remote string) (*api.File, error) {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		return &api.File{
			Path:           f.fullPath(""),
			ServerFilename: "",
			IsDir:          1,
			ServerMTime:    time.Now().Unix(),
			ServerCTime:    time.Now().Unix(),
		}, nil
	}
	parent := path.Dir(remote)
	if parent == "." {
		parent = ""
	}
	leaf := path.Base(remote)
	items, err := f.apiList(ctx, f.fullPath(parent))
	if api.ErrIsNum(err, -9) {
		return nil, fs.ErrorObjectNotFound
	}
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if item.ServerFilename == f.opt.Enc.FromStandardName(leaf) {
			found := item
			return &found, nil
		}
	}
	return nil, fs.ErrorObjectNotFound
}

func (f *Fs) findItemByAbsoluteRemote(ctx context.Context, remote string) (*api.File, error) {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		return &api.File{
			Path:           "/",
			ServerFilename: "",
			IsDir:          1,
			ServerMTime:    time.Now().Unix(),
			ServerCTime:    time.Now().Unix(),
		}, nil
	}
	parent := path.Dir(remote)
	if parent == "." {
		parent = ""
	}
	leaf := path.Base(remote)
	fullParent := "/"
	if parent != "" {
		fullParent = "/" + parent
	}
	items, err := f.apiList(ctx, fullParent)
	if api.ErrIsNum(err, -9) {
		return nil, fs.ErrorObjectNotFound
	}
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if item.ServerFilename == f.opt.Enc.FromStandardName(leaf) {
			found := item
			return &found, nil
		}
	}
	return nil, fs.ErrorObjectNotFound
}

func (f *Fs) findDeepestExistingRemote(ctx context.Context, remote string) (string, *api.File, error) {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		return "", nil, nil
	}
	parts := strings.Split(remote, "/")
	current := ""
	var last *api.File
	for i, part := range parts {
		next := part
		if current != "" {
			next = path.Join(current, part)
		}
		item, err := f.findItemByRemote(ctx, next)
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

func (f *Fs) findDeepestExistingAbsoluteRemote(ctx context.Context, remote string) (string, *api.File, error) {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		return "", nil, nil
	}
	parts := strings.Split(remote, "/")
	current := ""
	var last *api.File
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
				if item.IsDir == 0 {
					return fs.ErrorIsFile
				}
				current = next
				continue
			}
			if !errors.Is(err, fs.ErrorObjectNotFound) {
				return err
			}
			if err = f.apiCreate(ctx, "/"+next, 0, 1, "", "", 0, 0); err != nil {
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
			if item.IsDir == 0 {
				return fs.ErrorIsFile
			}
			current = next
			continue
		}
		if !errors.Is(err, fs.ErrorObjectNotFound) {
			return err
		}
		if err = f.apiCreate(ctx, f.fullPath(next), 0, 1, "", "", 0, 0); err != nil {
			return err
		}
		current = next
	}
	f.rootMissing = false
	return nil
}

func (f *Fs) init(ctx context.Context) error {
	if f.opt.AccessToken == "" {
		if err := f.refreshToken(ctx); err != nil {
			return err
		}
	}
	info, err := f.apiUserInfo(ctx)
	if err != nil {
		return err
	}
	f.vipType = info
	return nil
}

func (f *Fs) Name() string             { return f.name }
func (f *Fs) Root() string             { return f.root }
func (f *Fs) String() string           { return fmt.Sprintf("Baidu root '%s'", f.root) }
func (f *Fs) Features() *fs.Features   { return f.features }
func (f *Fs) Precision() time.Duration { return time.Second }
func (f *Fs) Hashes() hash.Set         { return hash.Set(hash.None) }

func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	quota, err := f.apiQuota(ctx)
	if err != nil {
		return nil, err
	}
	return &fs.Usage{
		Total: fs.NewUsageValue(quota.Total),
		Used:  fs.NewUsageValue(quota.Used),
		Free:  fs.NewUsageValue(quota.Total - quota.Used),
	}, nil
}

func (f *Fs) fullPath(remote string) string {
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

func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	if f.rootMissing && dir == "" {
		return entries, nil
	}
	files, err := f.apiList(ctx, f.fullPath(dir))
	if err != nil {
		if api.ErrIsNum(err, -9) {
			return nil, fs.ErrorDirNotFound
		}
		return nil, err
	}
	for _, item := range files {
		remote := path.Join(dir, f.opt.Enc.ToStandardName(item.ServerFilename))
		if item.IsDir == 1 {
			entries = append(entries, fs.NewDir(remote, time.Unix(nonZero(item.ServerMTime, item.MTime), 0)).SetID(strconv.FormatInt(item.FsID, 10)))
			continue
		}
		entries = append(entries, &Object{
			fs:          f,
			id:          item.FsID,
			remote:      remote,
			size:        item.Size,
			modTime:     time.Unix(nonZero(item.ServerMTime, item.MTime), 0),
			createdTime: time.Unix(nonZero(item.ServerCTime, item.CTime), 0),
		})
	}
	return entries, nil
}

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	if f.rootMissing {
		return nil, fs.ErrorObjectNotFound
	}
	item, err := f.findItemByRemote(ctx, remote)
	if err != nil {
		if api.ErrIsNum(err, -9) {
			return nil, fs.ErrorObjectNotFound
		}
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	if item.IsDir == 1 {
		return nil, fs.ErrorIsDir
	}
	return &Object{
		fs:          f,
		id:          item.FsID,
		remote:      remote,
		size:        item.Size,
		modTime:     time.Unix(nonZero(item.ServerMTime, item.MTime), 0),
		createdTime: time.Unix(nonZero(item.ServerCTime, item.CTime), 0),
	}, nil
}

func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	if src.Size() < 0 {
		return nil, errors.New("baidu requires known size")
	}
	if err := f.upload(ctx, in, src, options...); err != nil {
		return nil, err
	}
	return f.readBackObject(ctx, src)
}

func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	if src.Size() < 0 {
		return nil, errors.New("baidu requires known size")
	}
	if err := f.upload(ctx, in, src, options...); err != nil {
		return nil, err
	}
	return f.readBackObject(ctx, src)
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
	return &Object{
		fs:      f,
		remote:  src.Remote(),
		size:    src.Size(),
		modTime: src.ModTime(ctx),
	}, lastErr
}

func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	err := f.ensureDir(ctx, dir)
	if err == nil {
		f.rootMissing = false
	}
	return err
}

func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return fs.ErrorNotImplemented
}

func (f *Fs) Purge(ctx context.Context, dir string) error {
	return f.apiDelete(ctx, []string{f.fullPath(dir)})
}

func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantCopy
	}
	if err := f.apiManage(ctx, "copy", []map[string]string{{
		"path":    srcObj.fs.fullPath(srcObj.remote),
		"dest":    path.Dir(f.fullPath(remote)),
		"newname": path.Base(f.fullPath(remote)),
	}}); err != nil {
		return nil, err
	}
	return f.NewObject(ctx, remote)
}

func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantMove
	}
	if err := f.apiManage(ctx, "move", []map[string]string{{
		"path":    srcObj.fs.fullPath(srcObj.remote),
		"dest":    path.Dir(f.fullPath(remote)),
		"newname": path.Base(f.fullPath(remote)),
	}}); err != nil {
		return nil, err
	}
	return f.NewObject(ctx, remote)
}

func (o *Object) Fs() fs.Info    { return o.fs }
func (o *Object) String() string { return o.remote }
func (o *Object) Remote() string { return o.remote }
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}
func (o *Object) Size() int64    { return o.size }
func (o *Object) Storable() bool { return true }
func (o *Object) ID() string     { return strconv.FormatInt(o.id, 10) }
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.FixRangeOption(options, o.size)
	if o.downloadLink == "" {
		link, err := o.fs.apiDownloadLink(ctx, o.id)
		if err != nil {
			return nil, err
		}
		o.downloadLink = link
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, o.downloadLink, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "pan.baidu.com")
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	resp, err := o.fs.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		defer resp.Body.Close()
		return nil, fmt.Errorf("baidu: download http %d", resp.StatusCode)
	}
	return resp.Body, nil
}

func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	if src.Size() < 0 {
		return errors.New("baidu requires known size")
	}
	return o.fs.upload(ctx, in, src, options...)
}

func (o *Object) Remove(ctx context.Context) error {
	return o.fs.apiDelete(ctx, []string{o.fs.fullPath(o.remote)})
}

func nonZero(v, fallback int64) int64 {
	if v != 0 {
		return v
	}
	return fallback
}
