package open123

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/rest"
)

const (
	name        = "123_open"
	description = "123 Open"
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        name,
		Description: description,
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:      config.ConfigClientID,
			Help:      "123 Open client ID from https://www.123pan.com/developer.",
			Sensitive: true,
		}, {
			Name:      config.ConfigClientSecret,
			Help:      "123 Open client secret from https://www.123pan.com/developer.",
			Sensitive: true,
		}, {
			Name:      "access_token",
			Help:      "Cached 123 Open access token.",
			Sensitive: true,
			Advanced:  true,
		}, {
			Name:      "refresh_token",
			Help:      "123 Open refresh token used by the renew API.",
			Sensitive: true,
		}, {
			Name:     "use_online_api",
			Help:     "Use the renew API to refresh personal access tokens.",
			Default:  true,
			Advanced: true,
		}, {
			Name:     "api_url_address",
			Help:     "Renew API address when using refresh_token mode.",
			Default:  "https://api.oplist.org/123cloud/renewapi",
			Advanced: true,
		}, {
			Name:     "upload_thread",
			Help:     "Preferred upload concurrency. The current implementation uploads sequentially.",
			Default:  3,
			Advanced: true,
		}, {
			Name:     "direct_link",
			Help:     "Use direct-link API when downloading files.",
			Default:  false,
			Advanced: true,
		}, {
			Name:      "direct_link_private_key",
			Help:      "Private key used to sign direct links when URL authentication is enabled.",
			Sensitive: true,
			Advanced:  true,
		}, {
			Name:     "direct_link_valid_duration",
			Help:     "Signed direct-link validity in minutes.",
			Default:  30,
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
	ClientID                string               `config:"client_id"`
	ClientSecret            string               `config:"client_secret"`
	AccessToken             string               `config:"access_token"`
	RefreshToken            string               `config:"refresh_token"`
	UseOnlineAPI            bool                 `config:"use_online_api"`
	APIURLAddress           string               `config:"api_url_address"`
	UploadThread            int                  `config:"upload_thread"`
	DirectLink              bool                 `config:"direct_link"`
	DirectLinkPrivateKey    string               `config:"direct_link_private_key"`
	DirectLinkValidDuration int64                `config:"direct_link_valid_duration"`
	Enc                     encoder.MultiEncoder `config:"encoding"`
}

type Fs struct {
	name         string
	originalName string
	root         string
	rootMissing  bool
	opt          Options
	features     *fs.Features
	client       *rest.Client
	httpClient   *http.Client
	uid          uint64
	tm           tokenManager
}

type Object struct {
	fs           *Fs
	id           int64
	remote       string
	size         int64
	modTime      time.Time
	createdTime  time.Time
	etag         string
	sha1         string
	directLink   string
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
	if opt.UploadThread < 1 || opt.UploadThread > 32 {
		opt.UploadThread = 3
	}
	client := rest.NewClient(fshttp.NewClient(ctx))
	client.SetRoot(apiBaseURL)
	f := &Fs{
		name:         name,
		originalName: originalName,
		root:         root,
		opt:          *opt,
		client:       client,
		httpClient:   fshttp.NewClient(ctx),
	}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		NoMultiThreading:        true,
		ReadMetadata:            true,
	}).Fill(ctx, f)
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
	if item.Type == 2 {
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

func (f *Fs) init(ctx context.Context) error {
	_, err := f.getAccessToken(ctx, false)
	if err != nil {
		return err
	}
	_, err = f.apiUserInfo(ctx)
	return err
}

func (f *Fs) Name() string             { return f.name }
func (f *Fs) Root() string             { return f.root }
func (f *Fs) String() string           { return fmt.Sprintf("123 Open root '%s'", f.root) }
func (f *Fs) Features() *fs.Features   { return f.features }
func (f *Fs) Precision() time.Duration { return time.Second }
func (f *Fs) Hashes() hash.Set         { return hash.NewHashSet(hash.MD5, hash.SHA1) }

func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	info, err := f.apiUserInfo(ctx)
	if err != nil {
		return nil, err
	}
	total := info.Data.SpacePermanent + info.Data.SpaceTemp
	return &fs.Usage{
		Total: fs.NewUsageValue(total),
		Used:  fs.NewUsageValue(info.Data.SpaceUsed),
		Free:  fs.NewUsageValue(total - info.Data.SpaceUsed),
	}, nil
}

func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	if f.rootMissing && dir == "" {
		return entries, nil
	}
	item, err := f.findDirByRemote(ctx, dir)
	if err != nil {
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return nil, fs.ErrorDirNotFound
		}
		return nil, err
	}
	files, err := f.apiListFiles(ctx, item.FileID)
	if err != nil {
		return nil, err
	}
	for _, it := range files {
		remote := path.Join(dir, f.opt.Enc.ToStandardName(it.FileName))
		if it.Type == 1 {
			entries = append(entries, fs.NewDir(remote, it.ModTime()).SetID(strconv.FormatInt(it.FileID, 10)))
			continue
		}
		entries = append(entries, f.newObjectFromFile(remote, it))
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
	if item.Type == 1 {
		return nil, fs.ErrorIsDir
	}
	return f.newObjectFromFile(remote, item), nil
}

func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	if src.Size() < 0 {
		return nil, errors.New("123_open requires known size")
	}
	if err := f.upload(ctx, in, src, options...); err != nil {
		return nil, err
	}
	return f.readBackObject(ctx, src)
}

func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	if src.Size() < 0 {
		return nil, errors.New("123_open requires known size")
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
	item, err := f.findDirByRemote(ctx, dir)
	if err != nil {
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return fs.ErrorDirNotFound
		}
		return err
	}
	children, err := f.apiListFiles(ctx, item.FileID)
	if err != nil {
		return err
	}
	if len(children) != 0 {
		return fs.ErrorDirectoryNotEmpty
	}
	return f.apiTrash(ctx, item.FileID)
}

func (f *Fs) Purge(ctx context.Context, dir string) error {
	item, err := f.findDirByRemote(ctx, dir)
	if err != nil {
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return fs.ErrorDirNotFound
		}
		return err
	}
	return f.apiTrash(ctx, item.FileID)
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
	parent, err := f.findDirByRemote(ctx, parentRemote)
	if err != nil {
		return nil, err
	}
	if srcObj.etag == "" {
		return nil, fs.ErrorCantCopy
	}
	resp, err := f.apiCreateFile(ctx, parent.FileID, f.opt.Enc.FromStandardName(path.Base(remote)), srcObj.etag, srcObj.size, 2, false)
	if err != nil {
		return nil, err
	}
	if !resp.Data.Reuse {
		return nil, fs.ErrorCantCopy
	}
	return f.NewObject(ctx, remote)
}

func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantMove
	}
	dstParentRemote := path.Dir(remote)
	if dstParentRemote == "." {
		dstParentRemote = ""
	}
	if err := f.ensureDir(ctx, dstParentRemote); err != nil {
		return nil, err
	}
	dstParent, err := f.findDirByRemote(ctx, dstParentRemote)
	if err != nil {
		return nil, err
	}
	srcParentRemote := path.Dir(srcObj.remote)
	if srcParentRemote == "." {
		srcParentRemote = ""
	}
	srcName := path.Base(srcObj.remote)
	dstName := path.Base(remote)
	if srcParentRemote != dstParentRemote {
		if err := f.apiMove(ctx, srcObj.id, dstParent.FileID); err != nil {
			return nil, err
		}
	}
	if srcName != dstName {
		if err := f.apiRename(ctx, srcObj.id, f.opt.Enc.FromStandardName(dstName)); err != nil {
			return nil, err
		}
	}
	return f.NewObject(ctx, remote)
}

func (f *Fs) fullPath(remote string) string {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		return f.root
	}
	if f.root == "" {
		return remote
	}
	return path.Join(f.root, remote)
}

func (f *Fs) newObjectFromFile(remote string, item File) *Object {
	return &Object{
		fs:          f,
		id:          item.FileID,
		remote:      remote,
		size:        item.Size,
		modTime:     item.ModTime(),
		createdTime: item.CreateTime(),
		etag:        item.Etag,
		sha1:        item.SHA1,
	}
}

func (f *Fs) findItemByRemote(ctx context.Context, remote string) (File, error) {
	return f.findItemByAbsoluteRemote(ctx, f.fullPath(remote))
}

func (f *Fs) findDirByRemote(ctx context.Context, remote string) (File, error) {
	item, err := f.findItemByRemote(ctx, remote)
	if err != nil {
		return File{}, err
	}
	if item.Type != 1 {
		return File{}, fs.ErrorIsFile
	}
	return item, nil
}

func (f *Fs) findItemByAbsoluteRemote(ctx context.Context, remote string) (File, error) {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		return File{
			FileID:   0,
			Type:     1,
			FileName: "",
		}, nil
	}
	parts := strings.Split(remote, "/")
	parentID := int64(0)
	var current File
	for _, part := range parts {
		files, err := f.apiListFiles(ctx, parentID)
		if err != nil {
			return File{}, err
		}
		found := false
		for _, item := range files {
			if item.FileName == f.opt.Enc.FromStandardName(part) {
				current = item
				parentID = item.FileID
				found = true
				break
			}
		}
		if !found {
			return File{}, fs.ErrorObjectNotFound
		}
	}
	return current, nil
}

func (f *Fs) findDeepestExistingAbsoluteRemote(ctx context.Context, remote string) (string, *File, error) {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		return "", nil, nil
	}
	parts := strings.Split(remote, "/")
	current := ""
	var last *File
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
		itemCopy := item
		last = &itemCopy
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
				if item.Type != 1 {
					return fs.ErrorIsFile
				}
				current = next
				continue
			}
			if !errors.Is(err, fs.ErrorObjectNotFound) {
				return err
			}
			parentID := int64(0)
			if current != "" {
				parent, err := f.findItemByAbsoluteRemote(ctx, current)
				if err != nil {
					return err
				}
				parentID = parent.FileID
			}
			if err = f.apiMkdir(ctx, parentID, part); err != nil {
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
			if item.Type != 1 {
				return fs.ErrorIsFile
			}
			current = next
			continue
		}
		if !errors.Is(err, fs.ErrorObjectNotFound) {
			return err
		}
		parentID := int64(0)
		if current != "" {
			parent, err := f.findDirByRemote(ctx, current)
			if err != nil {
				return err
			}
			parentID = parent.FileID
		} else if f.root != "" {
			parent, err := f.findDirByRemote(ctx, "")
			if err != nil {
				return err
			}
			parentID = parent.FileID
		}
		if err = f.apiMkdir(ctx, parentID, f.opt.Enc.FromStandardName(part)); err != nil {
			return err
		}
		current = next
	}
	return nil
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
	switch t {
	case hash.MD5:
		if o.etag != "" {
			return strings.ToLower(o.etag), nil
		}
	case hash.SHA1:
		if o.sha1 != "" {
			return strings.ToLower(o.sha1), nil
		}
	}
	return "", hash.ErrUnsupported
}

func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.FixRangeOption(options, o.size)
	downloadURL := o.downloadLink
	if downloadURL == "" {
		if o.fs.opt.DirectLink {
			link, err := o.fs.apiDirectLink(ctx, o.id)
			if err != nil {
				return nil, err
			}
			downloadURL = link
			if o.fs.opt.DirectLinkPrivateKey != "" {
				uid, err := o.fs.getUID(ctx)
				if err != nil {
					return nil, err
				}
				duration := time.Duration(o.fs.opt.DirectLinkValidDuration) * time.Minute
				downloadURL, err = signURL(downloadURL, o.fs.opt.DirectLinkPrivateKey, uid, duration)
				if err != nil {
					return nil, err
				}
			}
		} else {
			link, err := o.fs.apiDownloadInfo(ctx, o.id)
			if err != nil {
				return nil, err
			}
			downloadURL = link
		}
		o.downloadLink = downloadURL
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
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
		return nil, fmt.Errorf("123_open: download http %d", resp.StatusCode)
	}
	return resp.Body, nil
}

func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	if src.Size() < 0 {
		return errors.New("123_open requires known size")
	}
	return o.fs.upload(ctx, in, src, options...)
}

func (o *Object) Remove(ctx context.Context) error {
	return o.fs.apiTrash(ctx, o.id)
}
