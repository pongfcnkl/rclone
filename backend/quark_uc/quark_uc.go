package quarkuc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
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
)

const (
	name        = "quark_uc"
	description = "Quark / UC Drive"
)

type serviceConfig struct {
	name    string
	ua      string
	referer string
	api     string
	pr      string
}

var (
	quarkService = serviceConfig{
		name:    "quark",
		ua:      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) quark-cloud-drive/2.5.20 Chrome/100.0.4896.160 Electron/18.3.5.4-b478491100 Safari/537.36 Channel/pckk_other_ch",
		referer: "https://pan.quark.cn",
		api:     "https://drive.quark.cn/1/clouddrive",
		pr:      "ucpro",
	}
	ucService = serviceConfig{
		name:    "uc",
		ua:      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) uc-cloud-drive/2.5.20 Chrome/100.0.4896.160 Electron/18.3.5.4-b478491100 Safari/537.36 Channel/pckk_other_ch",
		referer: "https://drive.uc.cn",
		api:     "https://pc-api.uc.cn/1/clouddrive",
		pr:      "UCBrowser",
	}
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        name,
		Description: description,
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:      "cookie",
			Help:      "Cookie copied from Quark/UC web login.",
			Required:  true,
			Sensitive: true,
		}, {
			Name:     "service",
			Help:     "Select Quark or UC service.",
			Default:  "quark",
			Examples: []fs.OptionExample{{Value: "quark", Help: "Use Quark."}, {Value: "uc", Help: "Use UC."}},
		}, {
			Name:     "root_folder_id",
			Help:     "Root folder fid.",
			Default:  "0",
			Advanced: true,
		}, {
			Name:     "order_by",
			Help:     "Directory listing sort field.",
			Default:  "none",
			Advanced: true,
			Examples: []fs.OptionExample{{Value: "none", Help: "No explicit sort."}, {Value: "file_type", Help: "Sort by file type."}, {Value: "file_name", Help: "Sort by file name."}, {Value: "updated_at", Help: "Sort by update time."}},
		}, {
			Name:     "order_direction",
			Help:     "Directory listing sort direction.",
			Default:  "asc",
			Advanced: true,
			Examples: []fs.OptionExample{{Value: "asc", Help: "Ascending."}, {Value: "desc", Help: "Descending."}},
		}, {
			Name:     "use_transcoding_address",
			Help:     "Use transcoding links for Quark video playback.",
			Default:  false,
			Advanced: true,
		}, {
			Name:     "only_list_video_file",
			Help:     "List only directories and video files.",
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
	Cookie            string               `config:"cookie"`
	Service           string               `config:"service"`
	RootFolderID      string               `config:"root_folder_id"`
	OrderBy           string               `config:"order_by"`
	OrderDirection    string               `config:"order_direction"`
	UseTranscoding    bool                 `config:"use_transcoding_address"`
	OnlyListVideoFile bool                 `config:"only_list_video_file"`
	Enc               encoder.MultiEncoder `config:"encoding"`
}

type Fs struct {
	name         string
	originalName string
	root         string
	rootMissing  bool
	opt          Options
	features     *fs.Features
	httpClient   *http.Client
	service      serviceConfig
}

type Object struct {
	fs          *Fs
	id          string
	remote      string
	size        int64
	modTime     time.Time
	createTime  time.Time
	category    int
	isDir       bool
	downloadURL string
}

type fileItem struct {
	Fid       string `json:"fid"`
	FileName  string `json:"file_name"`
	Category  int    `json:"category"`
	Size      int64  `json:"size"`
	File      bool   `json:"file"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

var (
	_ fs.Fs             = (*Fs)(nil)
	_ fs.Abouter        = (*Fs)(nil)
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
	service := quarkService
	switch strings.ToLower(strings.TrimSpace(opt.Service)) {
	case "", "quark":
		service = quarkService
		opt.Service = "quark"
	case "uc":
		service = ucService
		opt.Service = "uc"
	default:
		return nil, fmt.Errorf("quark_uc: unsupported service %q", opt.Service)
	}
	if strings.TrimSpace(opt.RootFolderID) == "" {
		opt.RootFolderID = "0"
	}
	f := &Fs{
		name:         name,
		originalName: originalName,
		root:         root,
		opt:          *opt,
		httpClient:   fshttp.NewClient(ctx),
		service:      service,
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
	actualRoot, item, err := f.findDeepestExistingRemote(ctx, root)
	if err != nil {
		return nil, err
	}
	if item == nil {
		f.root = root
		f.rootMissing = true
		return f, nil
	}
	f.root = actualRoot
	if !item.isDir {
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
	_, err := f.call(ctx, http.MethodGet, "/config", nil, nil, nil, nil)
	return err
}

func (f *Fs) Name() string             { return f.name }
func (f *Fs) Root() string             { return f.root }
func (f *Fs) String() string           { return fmt.Sprintf("Quark/UC root '%s'", f.root) }
func (f *Fs) Precision() time.Duration { return time.Second }
func (f *Fs) Hashes() hash.Set         { return hash.Set(hash.None) }
func (f *Fs) Features() *fs.Features   { return f.features }

func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	var resp memberResp
	_, err := f.call(ctx, http.MethodGet, "/member", map[string]string{
		"fetch_subscribe": "false",
		"_ch":             "home",
		"fetch_identity":  "false",
	}, nil, nil, &resp)
	if err != nil {
		return nil, err
	}
	return &fs.Usage{
		Total: fs.NewUsageValue(resp.Data.TotalCapacity),
		Used:  fs.NewUsageValue(resp.Data.UseCapacity),
		Free:  fs.NewUsageValue(resp.Data.TotalCapacity - resp.Data.UseCapacity),
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
	items, err := f.listByID(ctx, item.id)
	if err != nil {
		return nil, err
	}
	for _, it := range items {
		remote := path.Join(dir, f.opt.Enc.ToStandardName(it.FileName))
		if !it.File {
			entries = append(entries, fs.NewDir(remote, time.UnixMilli(it.UpdatedAt)).SetID(it.Fid))
			continue
		}
		entries = append(entries, f.newObject(remote, it))
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
	if item.isDir {
		return nil, fs.ErrorIsDir
	}
	return item, nil
}

func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	if src.Size() < 0 {
		return nil, errors.New("quark_uc requires known size")
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
	if err := f.ensureDir(ctx, dir); err != nil {
		return err
	}
	f.rootMissing = false
	return nil
}

func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	item, err := f.findDirByRemote(ctx, dir)
	if err != nil {
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return fs.ErrorDirNotFound
		}
		return err
	}
	children, err := f.listByID(ctx, item.id)
	if err != nil {
		return err
	}
	if len(children) != 0 {
		return fs.ErrorDirectoryNotEmpty
	}
	return f.deleteByID(ctx, item.id)
}

func (f *Fs) Purge(ctx context.Context, dir string) error {
	item, err := f.findDirByRemote(ctx, dir)
	if err != nil {
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return fs.ErrorDirNotFound
		}
		return err
	}
	return f.deleteByID(ctx, item.id)
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
	parent, err := f.findDirByRemote(ctx, parentRemote)
	if err != nil {
		return nil, err
	}
	if err := f.moveByID(ctx, srcObj.id, parent.id); err != nil {
		return nil, err
	}
	newName := f.opt.Enc.FromStandardName(path.Base(remote))
	if newName != path.Base(srcObj.remote) {
		if err := f.renameByID(ctx, srcObj.id, newName); err != nil {
			return nil, err
		}
	}
	return f.NewObject(ctx, remote)
}

func (f *Fs) fullRoot(remote string) string {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		return f.root
	}
	if f.root == "" {
		return remote
	}
	return path.Join(f.root, remote)
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

func (f *Fs) rootDirObject() *Object {
	return &Object{
		fs:      f,
		id:      f.opt.RootFolderID,
		remote:  "",
		modTime: time.Now(),
		isDir:   true,
	}
}

func (f *Fs) newObject(remote string, item fileItem) *Object {
	return &Object{
		fs:         f,
		id:         item.Fid,
		remote:     remote,
		size:       item.Size,
		modTime:    time.UnixMilli(item.UpdatedAt),
		createTime: time.UnixMilli(item.CreatedAt),
		category:   item.Category,
		isDir:      !item.File,
	}
}

func (f *Fs) findDirByRemote(ctx context.Context, remote string) (*Object, error) {
	item, err := f.findItemByRemote(ctx, remote)
	if err != nil {
		return nil, err
	}
	if !item.isDir {
		return nil, fs.ErrorIsFile
	}
	return item, nil
}

func (f *Fs) findItemByRemote(ctx context.Context, remote string) (*Object, error) {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		return f.rootDirObject(), nil
	}
	parts := strings.Split(remote, "/")
	parentID := f.opt.RootFolderID
	var out *Object
	for _, part := range parts {
		items, err := f.listByID(ctx, parentID)
		if err != nil {
			return nil, err
		}
		needle := f.opt.Enc.FromStandardName(part)
		found := false
		for _, item := range items {
			if item.FileName == needle {
				nextRemote := part
				if out != nil && out.remote != "" {
					nextRemote = path.Join(out.remote, part)
				}
				out = f.newObject(nextRemote, item)
				parentID = item.Fid
				found = true
				break
			}
		}
		if !found {
			return nil, fs.ErrorObjectNotFound
		}
	}
	return out, nil
}

func (f *Fs) findDeepestExistingRemote(ctx context.Context, remote string) (string, *Object, error) {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		return "", nil, nil
	}
	parts := strings.Split(remote, "/")
	current := ""
	var last *Object
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

func (f *Fs) ensureDir(ctx context.Context, dir string) error {
	if f.rootMissing {
		parts := strings.Split(strings.Trim(f.root, "/"), "/")
		current := ""
		parent := f.rootDirObject()
		for _, part := range parts {
			if part == "" {
				continue
			}
			next := part
			if current != "" {
				next = path.Join(current, part)
			}
			item, err := f.findItemByRemote(ctx, next)
			if err == nil {
				if !item.isDir {
					return fs.ErrorIsFile
				}
				current = next
				parent = item
				continue
			}
			if !errors.Is(err, fs.ErrorObjectNotFound) {
				return err
			}
			if err := f.mkdirByID(ctx, parent.id, f.opt.Enc.FromStandardName(part)); err != nil {
				return err
			}
			item, err = f.findItemByRemote(ctx, next)
			if err != nil {
				return err
			}
			current = next
			parent = item
		}
		f.rootMissing = false
	}
	dir = strings.Trim(dir, "/")
	if dir == "" {
		return nil
	}
	parts := strings.Split(dir, "/")
	current := ""
	parent := f.rootDirObject()
	for _, part := range parts {
		next := part
		if current != "" {
			next = path.Join(current, part)
		}
		item, err := f.findItemByRemote(ctx, next)
		if err == nil {
			if !item.isDir {
				return fs.ErrorIsFile
			}
			current = next
			parent = item
			continue
		}
		if !errors.Is(err, fs.ErrorObjectNotFound) {
			return err
		}
		if err := f.mkdirByID(ctx, parent.id, f.opt.Enc.FromStandardName(part)); err != nil {
			return err
		}
		item, err = f.findItemByRemote(ctx, next)
		if err != nil {
			return err
		}
		current = next
		parent = item
	}
	return nil
}

func (f *Fs) listByID(ctx context.Context, parentID string) ([]fileItem, error) {
	items := make([]fileItem, 0)
	page := 1
	size := 100
	query := map[string]string{
		"pdir_fid":             parentID,
		"_size":                strconv.Itoa(size),
		"_fetch_total":         "1",
		"fetch_all_file":       "1",
		"fetch_risk_file_name": "1",
	}
	if f.opt.OrderBy != "" && f.opt.OrderBy != "none" {
		query["_sort"] = "file_type:asc," + f.opt.OrderBy + ":" + f.opt.OrderDirection
	}
	for {
		query["_page"] = strconv.Itoa(page)
		var resp sortResp
		_, err := f.call(ctx, http.MethodGet, "/file/sort", query, nil, nil, &resp)
		if err != nil {
			return nil, err
		}
		for _, item := range resp.Data.List {
			item.FileName = strings.TrimSpace(item.FileName)
			if f.opt.OnlyListVideoFile && item.File && item.Category != 1 {
				continue
			}
			items = append(items, item)
		}
		if page*size >= resp.Metadata.Total {
			break
		}
		page++
	}
	return items, nil
}

func (f *Fs) mkdirByID(ctx context.Context, parentID, name string) error {
	body := map[string]any{
		"dir_init_lock": false,
		"dir_path":      "",
		"file_name":     name,
		"pdir_fid":      parentID,
	}
	_, err := f.call(ctx, http.MethodPost, "/file", nil, body, nil, nil)
	if err == nil {
		time.Sleep(time.Second)
	}
	return err
}

func (f *Fs) moveByID(ctx context.Context, id, parentID string) error {
	body := map[string]any{
		"action_type":  1,
		"exclude_fids": []string{},
		"filelist":     []string{id},
		"to_pdir_fid":  parentID,
	}
	_, err := f.call(ctx, http.MethodPost, "/file/move", nil, body, nil, nil)
	return err
}

func (f *Fs) renameByID(ctx context.Context, id, name string) error {
	body := map[string]any{
		"fid":       id,
		"file_name": name,
	}
	_, err := f.call(ctx, http.MethodPost, "/file/rename", nil, body, nil, nil)
	return err
}

func (f *Fs) deleteByID(ctx context.Context, id string) error {
	body := map[string]any{
		"action_type":  1,
		"exclude_fids": []string{},
		"filelist":     []string{id},
	}
	_, err := f.call(ctx, http.MethodPost, "/file/delete", nil, body, nil, nil)
	return err
}

func (f *Fs) call(ctx context.Context, method, pathname string, query map[string]string, body any, headers map[string]string, out any) ([]byte, error) {
	u := strings.TrimRight(f.service.api, "/") + pathname
	if len(query) != 0 {
		q := url.Values{}
		for k, v := range query {
			q.Set(k, v)
		}
		q.Set("pr", f.service.pr)
		q.Set("fr", "pc")
		u += "?" + q.Encode()
	} else {
		u += "?pr=" + url.QueryEscape(f.service.pr) + "&fr=pc"
	}
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reqBody = strings.NewReader(string(data))
	}
	req, err := http.NewRequestWithContext(ctx, method, u, reqBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Referer", f.service.referer)
	req.Header.Set("User-Agent", f.service.ua)
	req.Header.Set("Cookie", f.opt.Cookie)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	f.updateCookie(resp.Cookies())
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var apiErr baseResp
	_ = json.Unmarshal(data, &apiErr)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("quark_uc: http %d: %s", resp.StatusCode, string(data))
	}
	if apiErr.Status >= 400 || apiErr.Code != 0 {
		msg := strings.TrimSpace(apiErr.Message)
		if msg == "" {
			msg = string(data)
		}
		return nil, errors.New(msg)
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (f *Fs) updateCookie(cookies []*http.Cookie) {
	updated := f.opt.Cookie
	changed := false
	for _, ck := range cookies {
		if ck == nil {
			continue
		}
		if ck.Name != "__puus" && ck.Name != "__pus" {
			continue
		}
		updated = mergeCookie(updated, ck.Name, ck.Value)
		changed = true
	}
	if changed && updated != f.opt.Cookie {
		f.opt.Cookie = updated
		_ = config.SetValueAndSave(f.originalName, "cookie", updated)
	}
}

func mergeCookie(cookieStr, key, value string) string {
	parts := strings.Split(cookieStr, ";")
	found := false
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, key+"=") {
			parts[i] = key + "=" + value
			found = true
		}
	}
	if !found {
		if strings.TrimSpace(cookieStr) == "" {
			return key + "=" + value
		}
		return cookieStr + "; " + key + "=" + value
	}
	return strings.Join(parts, "; ")
}

func cacheFilePath(prefix string) (string, error) {
	dir := filepath.Join(config.GetCacheDir(), "quark_uc")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	file, err := os.CreateTemp(dir, prefix)
	if err != nil {
		return "", err
	}
	name := file.Name()
	if err = file.Close(); err != nil {
		return "", err
	}
	return name, nil
}

func (o *Object) Fs() fs.Info    { return o.fs }
func (o *Object) String() string { return o.remote }
func (o *Object) Remote() string { return o.remote }
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}
func (o *Object) Size() int64    { return o.size }
func (o *Object) Storable() bool { return true }
func (o *Object) ID() string     { return o.id }
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.FixRangeOption(options, o.size)
	link, headers, err := o.fs.downloadLink(ctx, o)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link, nil)
	if err != nil {
		return nil, err
	}
	for k, vv := range headers {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	resp, err := o.fs.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		defer resp.Body.Close()
		return nil, fmt.Errorf("quark_uc: download http %d", resp.StatusCode)
	}
	return resp.Body, nil
}

func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	if src.Size() < 0 {
		return errors.New("quark_uc requires known size")
	}
	return o.fs.upload(ctx, in, src, options...)
}

func (o *Object) Remove(ctx context.Context) error {
	return o.fs.deleteByID(ctx, o.id)
}
