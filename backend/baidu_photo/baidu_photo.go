package baiduphoto

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

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
)

const (
	name             = "baidu_photo"
	description      = "Baidu Photo"
	defaultUserAgent = "pan.baidu.com"
	minSleep         = 200 * time.Millisecond
	maxSleep         = 2 * time.Second
	decayConstant    = 2
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        name,
		Description: description,
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:      "cookie",
			Help:      "Baidu cookie used by photo.baidu.com.",
			Required:  true,
			Sensitive: true,
		}, {
			Name:     "show_type",
			Help:     "Objects shown at the root when album_id is empty: root, root_only_album or root_only_file.",
			Advanced: true,
		}, {
			Name:     "album_id",
			Help:     "Album id to use as the rclone root. Values like album_id|password are accepted; only album_id is used.",
			Advanced: true,
		}, {
			Name:     "delete_origin",
			Help:     "Delete original photo when deleting from albums.",
			Default:  false,
			Advanced: true,
		}, {
			Name:     "upload_thread",
			Help:     "Preferred upload concurrency. The current implementation uploads sequentially for accurate progress.",
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
	Cookie       string               `config:"cookie"`
	ShowType     string               `config:"show_type"`
	AlbumID      string               `config:"album_id"`
	DeleteOrigin bool                 `config:"delete_origin"`
	UploadThread int                  `config:"upload_thread"`
	Enc          encoder.MultiEncoder `config:"encoding"`
}

type Fs struct {
	name         string
	originalName string
	root         string
	opt          Options
	features     *fs.Features
	httpClient   *http.Client
	pacer        *fs.Pacer
	uk           int64
	bdstoken     string
	rootAlbum    *album
	mu           sync.Mutex
}

type Object struct {
	fs           *Fs
	id           int64
	remote       string
	size         int64
	modTime      time.Time
	createdTime  time.Time
	md5          string
	albumFile    *albumFile
	downloadLink string
}

var (
	_ fs.Fs             = (*Fs)(nil)
	_ fs.PutUncheckeder = (*Fs)(nil)
	_ fs.Object         = (*Object)(nil)
)

func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}
	initShowType(opt)
	originalName := name
	if idx := strings.IndexRune(name, '{'); idx > 0 {
		originalName = name[:idx]
	}
	root = strings.Trim(root, "/")
	if opt.UploadThread < 1 || opt.UploadThread > 32 {
		opt.UploadThread = 3
	}
	f := &Fs{
		name:         name,
		originalName: originalName,
		opt:          *opt,
		httpClient:   fshttp.NewClient(ctx),
		pacer:        fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
	}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		NoMultiThreading:        true,
	}).Fill(ctx, f)
	if err := f.init(ctx); err != nil {
		return nil, err
	}
	if root == "" {
		return f, nil
	}
	if _, dirErr := f.findAlbumByName(ctx, root); dirErr == nil {
		f.root = root
		return f, nil
	}
	if f.opt.ShowType != "root_only_album" {
		item, err := f.NewObject(ctx, root)
		if err == nil {
			parent := path.Dir(root)
			if parent == "." {
				parent = ""
			}
			tempF := *f
			tempF.root = parent
			return &tempF, fs.ErrorIsFile
		}
		if !errors.Is(err, fs.ErrorObjectNotFound) && !errors.Is(err, fs.ErrorDirNotFound) {
			_ = item
			return nil, err
		}
	}
	f.root = root
	return f, nil
}

func initShowType(opt *Options) {
	switch strings.TrimSpace(opt.ShowType) {
	case "", "root_only_album":
		opt.ShowType = "root_only_album"
	case "root", "root_only_file":
	default:
		opt.ShowType = "root_only_album"
	}
}

func (f *Fs) init(ctx context.Context) error {
	info, err := f.apiUserInfo(ctx)
	if err != nil {
		return err
	}
	f.uk, err = strconv.ParseInt(info.YouaID, 10, 64)
	if err != nil {
		return err
	}
	f.bdstoken, err = f.apiBDSToken(ctx)
	if err != nil {
		return err
	}
	if f.opt.AlbumID != "" {
		f.rootAlbum, err = f.apiAlbumDetail(ctx, trimAlbumID(f.opt.AlbumID))
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *Fs) Name() string             { return f.name }
func (f *Fs) Root() string             { return f.root }
func (f *Fs) String() string           { return fmt.Sprintf("Baidu Photo root '%s'", f.root) }
func (f *Fs) Features() *fs.Features   { return f.features }
func (f *Fs) Precision() time.Duration { return time.Second }
func (f *Fs) Hashes() hash.Set         { return hash.NewHashSet(hash.MD5) }

func (f *Fs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	userDir := strings.Trim(dir, "/")
	lookupDir := userDir
	if f.root != "" {
		if lookupDir == "" {
			lookupDir = f.root
		} else {
			lookupDir = path.Join(f.root, lookupDir)
		}
	}
	if f.rootAlbum != nil {
		if userDir != "" {
			return nil, fs.ErrorDirNotFound
		}
		files, err := f.apiAlbumFiles(ctx, f.rootAlbum)
		if err != nil {
			return nil, err
		}
		entries := make(fs.DirEntries, 0, len(files))
		for _, item := range files {
			entries = append(entries, f.objectFromAlbumFile(item.name(), item))
		}
		return entries, nil
	}
	if lookupDir == "" {
		return f.listRoot(ctx)
	}
	a, err := f.findAlbumByName(ctx, lookupDir)
	if err != nil {
		if userDir == "" && f.root != "" && errors.Is(err, fs.ErrorDirNotFound) {
			return fs.DirEntries{}, nil
		}
		return nil, err
	}
	files, err := f.apiAlbumFiles(ctx, a)
	if err != nil {
		return nil, err
	}
	entries := make(fs.DirEntries, 0, len(files))
	for _, item := range files {
		entries = append(entries, f.objectFromAlbumFile(path.Join(userDir, item.name()), item))
	}
	return entries, nil
}

func (f *Fs) listRoot(ctx context.Context) (fs.DirEntries, error) {
	var entries fs.DirEntries
	if f.opt.ShowType != "root_only_file" {
		albums, err := f.apiAllAlbums(ctx)
		if err != nil {
			return nil, err
		}
		for _, a := range albums {
			entries = append(entries, fs.NewDir(f.opt.Enc.ToStandardName(a.Title), nonZeroTime(a.Mtime, a.CreationTime)).SetID(a.AlbumID))
		}
	}
	if f.opt.ShowType != "root_only_album" {
		files, err := f.apiAllFiles(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range files {
			entries = append(entries, f.objectFromFile(f.opt.Enc.ToStandardName(item.name()), item))
		}
	}
	return entries, nil
}

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	userRemote := strings.Trim(remote, "/")
	lookupRemote := userRemote
	if f.root != "" {
		lookupRemote = path.Join(f.root, lookupRemote)
	}
	if lookupRemote == "" {
		return nil, fs.ErrorIsDir
	}
	if f.rootAlbum != nil {
		item, err := f.findAlbumFileByName(ctx, f.rootAlbum, path.Base(lookupRemote))
		if err != nil {
			return nil, err
		}
		return f.objectFromAlbumFile(userRemote, *item), nil
	}
	dir, leaf := path.Split(lookupRemote)
	dir = strings.Trim(dir, "/")
	if dir == "" {
		item, err := f.findRootFileByName(ctx, leaf)
		if err != nil {
			if errors.Is(err, fs.ErrorObjectNotFound) {
				if _, dirErr := f.findAlbumByName(ctx, leaf); dirErr == nil {
					return nil, fs.ErrorIsDir
				}
			}
			return nil, err
		}
		return f.objectFromFile(userRemote, *item), nil
	}
	a, err := f.findAlbumByName(ctx, dir)
	if err != nil {
		if errors.Is(err, fs.ErrorDirNotFound) {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	item, err := f.findAlbumFileByName(ctx, a, leaf)
	if err != nil {
		return nil, err
	}
	return f.objectFromAlbumFile(userRemote, *item), nil
}

func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	if src.Size() < 0 {
		return nil, errors.New("baidu_photo requires known size")
	}
	if err := f.upload(ctx, in, src, options...); err != nil {
		return nil, err
	}
	return f.NewObject(ctx, src.Remote())
}

func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}

func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	dir = strings.Trim(dir, "/")
	if f.rootAlbum != nil {
		if dir == "" {
			return nil
		}
		return fs.ErrorCantDirMove
	}
	if f.root != "" {
		dir = path.Join(f.root, dir)
	}
	if dir == "" {
		return nil
	}
	if strings.Contains(dir, "/") {
		return fs.ErrorCantDirMove
	}
	if _, err := f.findAlbumByName(ctx, dir); err == nil {
		return nil
	}
	_, err := f.apiCreateAlbum(ctx, f.opt.Enc.FromStandardName(dir))
	if err != nil {
		return fmt.Errorf("baidu_photo: failed to create album %q: %w", dir, err)
	}
	return nil
}

func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	dir = strings.Trim(dir, "/")
	if f.rootAlbum != nil || dir == "" {
		return nil
	}
	if f.root != "" {
		dir = path.Join(f.root, dir)
	}
	a, err := f.findAlbumByName(ctx, dir)
	if err != nil {
		return err
	}
	files, err := f.apiAlbumFiles(ctx, a)
	if err != nil {
		return err
	}
	if len(files) != 0 {
		return fs.ErrorDirectoryNotEmpty
	}
	return f.apiDeleteAlbum(ctx, a)
}

func (f *Fs) objectFromFile(remote string, item file) *Object {
	return &Object{
		fs:          f,
		id:          item.Fsid,
		remote:      remote,
		size:        item.Size,
		modTime:     nonZeroTime(item.Mtime, item.Ctime),
		createdTime: time.Unix(item.Ctime, 0),
		md5:         decryptMD5(item.MD5),
	}
}

func (f *Fs) objectFromAlbumFile(remote string, item albumFile) *Object {
	obj := f.objectFromFile(remote, item.file)
	obj.albumFile = &item
	return obj
}

func (f *Fs) findRootFileByName(ctx context.Context, name string) (*file, error) {
	files, err := f.apiAllFiles(ctx)
	if err != nil {
		return nil, err
	}
	want := f.opt.Enc.FromStandardName(name)
	for _, item := range files {
		if item.name() == want {
			found := item
			return &found, nil
		}
	}
	return nil, fs.ErrorObjectNotFound
}

func (f *Fs) findAlbumByName(ctx context.Context, name string) (*album, error) {
	return f.findAlbumByTitle(ctx, f.opt.Enc.FromStandardName(name))
}

func (f *Fs) findAlbumByTitle(ctx context.Context, title string) (*album, error) {
	albums, err := f.apiAllAlbums(ctx)
	if err != nil {
		return nil, err
	}
	for _, a := range albums {
		if a.Title == title || a.AlbumID == title {
			found := a
			return &found, nil
		}
	}
	return nil, fs.ErrorDirNotFound
}

func (f *Fs) findAlbumFileByName(ctx context.Context, a *album, name string) (*albumFile, error) {
	files, err := f.apiAlbumFiles(ctx, a)
	if err != nil {
		return nil, err
	}
	want := f.opt.Enc.FromStandardName(name)
	for _, item := range files {
		if item.name() == want {
			found := item
			return &found, nil
		}
	}
	return nil, fs.ErrorObjectNotFound
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
	if t != hash.MD5 || o.md5 == "" {
		return "", hash.ErrUnsupported
	}
	return o.md5, nil
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
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Referer", "https://photo.baidu.com/")
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	resp, err := o.fs.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		defer resp.Body.Close()
		return nil, fmt.Errorf("baidu_photo: download http %d", resp.StatusCode)
	}
	return resp.Body, nil
}

func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	if src.Size() < 0 {
		return errors.New("baidu_photo requires known size")
	}
	return o.fs.upload(ctx, in, src, options...)
}

func (o *Object) Remove(ctx context.Context) error {
	if o.albumFile != nil {
		return o.fs.apiDeleteAlbumFile(ctx, *o.albumFile)
	}
	return o.fs.apiDeleteFile(ctx, file{Fsid: o.id})
}
