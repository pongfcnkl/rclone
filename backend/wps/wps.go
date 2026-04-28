package wps

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
)

const (
	name               = "wps"
	description        = "WPS"
	endpointBusiness   = "https://365.kdocs.cn"
	endpointPersonal   = "https://drive.wps.cn"
	loginCheckEndpoint = "https://account.kdocs.cn/api/v3/islogin"
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        name,
		Description: description,
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:      "cookie",
			Help:      "Cookie copied from WPS/KDocs web login.",
			Required:  true,
			Sensitive: true,
		}, {
			Name:     "mode",
			Help:     "Account mode.",
			Default:  "Personal",
			Examples: []fs.OptionExample{{Value: "Personal", Help: "Use personal drive."}, {Value: "Business", Help: "Use business drive."}},
		}, {
			Name:     "custom_ua",
			Help:     "Optional custom User-Agent.",
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
	Cookie   string               `config:"cookie"`
	Mode     string               `config:"mode"`
	CustomUA string               `config:"custom_ua"`
	Enc      encoder.MultiEncoder `config:"encoding"`
}

type loginState struct {
	CompanyID        int64 `json:"companyid"`
	CurrentCompanyID int64 `json:"current_companyid"`
	IsCompanyAccount bool  `json:"is_company_account"`
}

type apiResult struct {
	Result string `json:"result"`
	Msg    string `json:"msg"`
}

type Group struct {
	CompanyID int64  `json:"company_id"`
	GroupID   int64  `json:"group_id"`
	Name      string `json:"name"`
	Type      string `json:"type"`
}

type groupsResp struct {
	Groups []Group `json:"groups"`
}

type personalGroupsResp struct {
	apiResult
	Groups []struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	} `json:"groups"`
}

type filePerms struct {
	Download int `json:"download"`
}

type FileInfo struct {
	GroupID   int64     `json:"groupid"`
	ParentID  int64     `json:"parentid"`
	Name      string    `json:"fname"`
	Size      int64     `json:"fsize"`
	Type      string    `json:"ftype"`
	Ctime     int64     `json:"ctime"`
	Mtime     int64     `json:"mtime"`
	ID        int64     `json:"id"`
	Deleted   bool      `json:"deleted"`
	FilePerms filePerms `json:"file_perms_acl"`
}

type filesResp struct {
	Files      []FileInfo `json:"files"`
	NextOffset int        `json:"next_offset"`
}

type downloadResp struct {
	URL    string `json:"url"`
	Result string `json:"result"`
}

type spacesResp struct {
	Result string `json:"result"`
	Total  int64  `json:"total"`
	Used   int64  `json:"used"`
}

type serviceSpaceResp struct {
	Info []struct {
		ID         int64 `json:"id"`
		SpaceTotal int64 `json:"space_total"`
		SpaceUsed  int64 `json:"space_used"`
	} `json:"info"`
}

type uploadCreateUpdateResp struct {
	apiResult
	Method  string `json:"method"`
	URL     string `json:"url"`
	Store   string `json:"store"`
	Request struct {
		Headers  map[string]string `json:"headers"`
		FormData map[string]string `json:"formData"`
	} `json:"request"`
	Response struct {
		ExpectCode []int  `json:"expect_code"`
		ArgsETag   string `json:"args_etag"`
		ArgsKey    string `json:"args_key"`
	} `json:"response"`
}

type uploadPutResp struct {
	NewFilename string `json:"newfilename"`
	Sha1        string `json:"sha1"`
	MD5         string `json:"md5"`
}

type node struct {
	kind        string
	groupID     int64
	fileID      int64
	name        string
	path        string
	size        int64
	modTime     time.Time
	canDownload bool
}

type Fs struct {
	name         string
	originalName string
	root         string
	opt          Options
	httpClient   *http.Client
	features     *fs.Features
	login        *loginState
	rootNode     *node
	rootMissing  bool
	dirMu        sync.Mutex
}

type Object struct {
	fs   *Fs
	node *node
}

type readCloserWithReader struct {
	io.Reader
	io.Closer
}

type reopenableSource struct {
	open func(context.Context, ...fs.OpenOption) (io.ReadCloser, error)
}

type hashInfo struct {
	size   int64
	sha1   string
	sha256 string
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
	opt.Mode = normalizeMode(opt.Mode)
	if strings.TrimSpace(opt.Mode) == "" {
		opt.Mode = "Personal"
	}
	f := &Fs{
		name:         name,
		originalName: originalName,
		root:         root,
		opt:          *opt,
		httpClient:   fshttp.NewClient(ctx),
	}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		NoMultiThreading:        true,
	}).Fill(ctx, f)
	if err := f.init(ctx); err != nil {
		return nil, err
	}
	if err := f.initRoot(ctx); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *Fs) init(ctx context.Context) error {
	if strings.TrimSpace(f.opt.Cookie) == "" {
		return errors.New("cookie is empty")
	}
	var login loginState
	if _, err := f.doRequest(ctx, http.MethodGet, loginCheckEndpoint, nil, nil, nil, &login); err != nil {
		return err
	}
	f.login = &login
	return nil
}

func (f *Fs) initRoot(ctx context.Context) error {
	rootNode := &node{
		kind:    "root",
		name:    "root",
		path:    "",
		modTime: time.Now(),
	}
	if f.root == "" {
		f.rootNode = rootNode
		return nil
	}
	resolved, missing, err := f.resolveRootPath(ctx, f.root)
	if err != nil {
		return err
	}
	if missing {
		f.rootMissing = true
		f.rootNode = resolved
		return nil
	}
	f.rootNode = resolved
	if resolved.kind != "group" && resolved.kind != "folder" {
		newRoot := path.Dir(f.root)
		if newRoot == "." {
			newRoot = ""
		}
		tempF := *f
		tempF.root = newRoot
		if newRoot == "" {
			tempF.rootNode = rootNode
		} else {
			parent, _, err := tempF.resolveRootPath(ctx, newRoot)
			if err != nil {
				return err
			}
			tempF.rootNode = parent
		}
		return fs.ErrorIsFile
	}
	return nil
}

func (f *Fs) Name() string             { return f.name }
func (f *Fs) Root() string             { return f.root }
func (f *Fs) String() string           { return fmt.Sprintf("WPS root '%s'", f.root) }
func (f *Fs) Precision() time.Duration { return time.Second }
func (f *Fs) Hashes() hash.Set         { return hash.Set(hash.None) }
func (f *Fs) Features() *fs.Features   { return f.features }

func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	if f.isPersonal() {
		var resp spacesResp
		if _, err := f.doRequest(ctx, http.MethodGet, endpointPersonal+"/api/v3/spaces", nil, nil, nil, &resp); err != nil {
			return nil, err
		}
		return &fs.Usage{
			Total: fs.NewUsageValue(resp.Total),
			Used:  fs.NewUsageValue(resp.Used),
			Free:  fs.NewUsageValue(resp.Total - resp.Used),
		}, nil
	}
	var resp serviceSpaceResp
	u := endpointBusiness + "/3rd/plussvr/compose/v1/u/companies/batch/service-space?comp_ids=" + strconv.FormatInt(f.login.CompanyID, 10)
	if _, err := f.doRequest(ctx, http.MethodGet, u, nil, nil, nil, &resp); err != nil {
		return nil, err
	}
	for _, info := range resp.Info {
		if info.ID == f.login.CompanyID {
			return &fs.Usage{
				Total: fs.NewUsageValue(info.SpaceTotal),
				Used:  fs.NewUsageValue(info.SpaceUsed),
				Free:  fs.NewUsageValue(info.SpaceTotal - info.SpaceUsed),
			}, nil
		}
	}
	return nil, fmt.Errorf("service space info not found for company ID: %d", f.login.CompanyID)
}

func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	base, err := f.findDirByRemote(ctx, dir)
	if err != nil {
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return nil, fs.ErrorDirNotFound
		}
		return nil, err
	}
	if base.kind == "root" {
		groups, err := f.getGroups(ctx)
		if err != nil {
			return nil, err
		}
		for _, g := range groups {
			remote := path.Join(dir, f.opt.Enc.ToStandardName(g.Name))
			entries = append(entries, fs.NewDir(remote, time.Time{}).SetID(strconv.FormatInt(g.GroupID, 10)))
		}
		return entries, nil
	}
	parentID := int64(0)
	if base.kind == "folder" {
		parentID = base.fileID
	}
	files, err := f.getFiles(ctx, base.groupID, parentID)
	if err != nil {
		return nil, err
	}
	for _, item := range files {
		remote := path.Join(dir, f.opt.Enc.ToStandardName(item.Name))
		if item.Type == "folder" {
			entries = append(entries, fs.NewDir(remote, parseTime(item.Mtime)).SetID(strconv.FormatInt(item.ID, 10)))
			continue
		}
		entries = append(entries, &Object{fs: f, node: fileInfoToNode(item, remote, f.isPersonal())})
	}
	return entries, nil
}

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	item, err := f.findItemByRemote(ctx, remote)
	if err != nil {
		return nil, err
	}
	if item.kind != "file" {
		return nil, fs.ErrorIsDir
	}
	return &Object{fs: f, node: item}, nil
}

func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	if src.Size() < 0 {
		return nil, errors.New("wps requires known size")
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
	return f.ensureDir(ctx, dir)
}

func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	item, err := f.findDirByRemote(ctx, dir)
	if err != nil {
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return fs.ErrorDirNotFound
		}
		return err
	}
	if item.kind != "group" && item.kind != "folder" {
		return fs.ErrorIsFile
	}
	children, err := f.listChildrenForNode(ctx, item)
	if err != nil {
		return err
	}
	if len(children) != 0 {
		return fs.ErrorDirectoryNotEmpty
	}
	if item.kind == "group" {
		return fs.ErrorNotImplemented
	}
	return f.removeNode(ctx, item)
}

func (f *Fs) Purge(ctx context.Context, dir string) error {
	item, err := f.findDirByRemote(ctx, dir)
	if err != nil {
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return fs.ErrorDirNotFound
		}
		return err
	}
	if item.kind == "group" {
		return fs.ErrorNotImplemented
	}
	return f.removeNode(ctx, item)
}

func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantCopy
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
	if err := f.copyNode(ctx, srcObj.node, dstParent); err != nil {
		return nil, err
	}
	if path.Base(srcObj.node.path) != path.Base(remote) {
		obj, err := f.NewObject(ctx, path.Join(dstParentRemote, path.Base(srcObj.node.path)))
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
	srcParentRemote := path.Dir(srcObj.node.path)
	if srcParentRemote == "." {
		srcParentRemote = ""
	}
	if srcParentRemote != dstParentRemote {
		if err := f.moveNode(ctx, srcObj.node, dstParent); err != nil {
			return nil, err
		}
	}
	newName := f.opt.Enc.FromStandardName(path.Base(remote))
	if newName != path.Base(srcObj.node.path) {
		if err := f.renameNode(ctx, srcObj.node, newName); err != nil {
			return nil, err
		}
	}
	return f.NewObject(ctx, remote)
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
	return &Object{fs: f, node: &node{kind: "file", path: src.Remote(), name: path.Base(src.Remote()), size: src.Size(), modTime: src.ModTime(ctx)}}, lastErr
}

func (f *Fs) isPersonal() bool {
	if f.login != nil {
		return !f.login.IsCompanyAccount
	}
	return normalizeMode(f.opt.Mode) == "Personal"
}

func (f *Fs) driveHost() string {
	if f.isPersonal() {
		return endpointPersonal
	}
	return endpointBusiness
}

func (f *Fs) drivePrefix() string {
	if f.isPersonal() {
		return ""
	}
	return "/3rd/drive"
}

func (f *Fs) driveURL(p string) string {
	return f.driveHost() + f.drivePrefix() + p
}

func (f *Fs) origin() string {
	return f.driveHost()
}

func (f *Fs) userAgent() string {
	if strings.TrimSpace(f.opt.CustomUA) != "" {
		return f.opt.CustomUA
	}
	return "rclone"
}

func (f *Fs) doRequest(ctx context.Context, method, endpoint string, query map[string]string, headers map[string]string, body any, out any) ([]byte, error) {
	u := endpoint
	if len(query) != 0 {
		q := url.Values{}
		for k, v := range query {
			q.Set(k, v)
		}
		if strings.Contains(u, "?") {
			u += "&" + q.Encode()
		} else {
			u += "?" + q.Encode()
		}
	}
	var reqBody io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reqBody = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, reqBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Cookie", f.opt.Cookie)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", f.userAgent())
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", f.origin())
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("wps: http %d: %s", resp.StatusCode, string(data))
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (f *Fs) doJSONAPI(ctx context.Context, method, endpoint string, body any, out any) error {
	headers := map[string]string{
		"Content-Type": "application/json",
		"Origin":       f.origin(),
	}
	_, err := f.doRequest(ctx, method, endpoint, nil, headers, body, out)
	return err
}

func (f *Fs) getGroups(ctx context.Context) ([]Group, error) {
	mode := normalizeMode(f.opt.Mode)
	switch mode {
	case "", "Personal":
		var resp personalGroupsResp
		if _, err := f.doRequest(ctx, http.MethodGet, f.driveURL("/api/v3/groups"), nil, nil, nil, &resp); err != nil {
			return nil, err
		}
		if resp.Result != "" && resp.Result != "ok" {
			return nil, fmt.Errorf("%s", firstNonEmpty(resp.Msg, "list groups failed"))
		}
		out := make([]Group, 0, len(resp.Groups))
		for _, g := range resp.Groups {
			out = append(out, Group{GroupID: g.ID, Name: g.Name})
		}
		return out, nil
	case "Business":
		var resp groupsResp
		u := fmt.Sprintf("%s/3rd/plus/groups/v1/companies/%d/users/self/groups/private", endpointBusiness, f.login.CompanyID)
		if _, err := f.doRequest(ctx, http.MethodGet, u, nil, nil, nil, &resp); err != nil {
			return nil, err
		}
		return resp.Groups, nil
	default:
		return nil, fmt.Errorf("unsupported mode: %s", f.opt.Mode)
	}
}

func (f *Fs) getFiles(ctx context.Context, groupID, parentID int64) ([]FileInfo, error) {
	var files []FileInfo
	nextOffset := 0
	for i := 0; i < 50; i++ {
		var resp filesResp
		_, err := f.doRequest(ctx, http.MethodGet, fmt.Sprintf("%s/api/v5/groups/%d/files", f.driveHost()+f.drivePrefix(), groupID), map[string]string{
			"parentid": strconv.FormatInt(parentID, 10),
			"offset":   strconv.Itoa(nextOffset),
		}, nil, nil, &resp)
		if err != nil {
			return nil, err
		}
		files = append(files, resp.Files...)
		if resp.NextOffset == -1 {
			break
		}
		nextOffset = resp.NextOffset
	}
	return files, nil
}

func (f *Fs) resolveRootPath(ctx context.Context, root string) (*node, bool, error) {
	root = strings.Trim(root, "/")
	if root == "" {
		return &node{kind: "root", name: "root", path: ""}, false, nil
	}
	parts := strings.Split(root, "/")
	groups, err := f.getGroups(ctx)
	if err != nil {
		return nil, false, err
	}
	var current *node
	for _, g := range groups {
		if g.Name == f.opt.Enc.FromStandardName(parts[0]) || g.Name == parts[0] {
			current = &node{kind: "group", groupID: g.GroupID, name: g.Name, path: f.opt.Enc.ToStandardName(g.Name)}
			break
		}
	}
	if current == nil {
		return &node{kind: "root", name: "root", path: ""}, true, nil
	}
	parentID := int64(0)
	for _, part := range parts[1:] {
		files, err := f.getFiles(ctx, current.groupID, parentID)
		if err != nil {
			return nil, false, err
		}
		var next *node
		want := f.opt.Enc.FromStandardName(part)
		for _, item := range files {
			if item.Type == "folder" && item.Name == want {
				next = fileInfoToNode(item, path.Join(current.path, f.opt.Enc.ToStandardName(item.Name)), f.isPersonal())
				break
			}
		}
		if next == nil {
			missingBase := &node{kind: current.kind, groupID: current.groupID, fileID: current.fileID, name: current.name, path: current.path}
			return missingBase, true, nil
		}
		current = next
		parentID = current.fileID
	}
	return current, false, nil
}

func (f *Fs) findDirByRemote(ctx context.Context, remote string) (*node, error) {
	item, err := f.findItemByRemote(ctx, remote)
	if err != nil {
		return nil, err
	}
	if item.kind != "root" && item.kind != "group" && item.kind != "folder" {
		return nil, fs.ErrorIsFile
	}
	return item, nil
}

func (f *Fs) findItemByRemote(ctx context.Context, remote string) (*node, error) {
	remote = strings.Trim(remote, "/")
	base := f.rootNode
	if base == nil {
		base = &node{kind: "root", name: "root", path: ""}
	}
	if remote == "" {
		if f.rootMissing {
			return nil, fs.ErrorObjectNotFound
		}
		return base, nil
	}
	if f.rootMissing {
		return nil, fs.ErrorObjectNotFound
	}
	parts := strings.Split(remote, "/")
	current := &node{
		kind:        base.kind,
		groupID:     base.groupID,
		fileID:      base.fileID,
		name:        base.name,
		path:        "",
		canDownload: base.canDownload,
	}
	for _, part := range parts {
		children, err := f.listChildrenForNode(ctx, current)
		if err != nil {
			return nil, err
		}
		want := f.opt.Enc.FromStandardName(part)
		var next *node
		for _, child := range children {
			if child.name == want {
				cp := *child
				cp.path = path.Join(current.path, f.opt.Enc.ToStandardName(child.name))
				next = &cp
				break
			}
		}
		if next == nil {
			return nil, fs.ErrorObjectNotFound
		}
		current = next
	}
	return current, nil
}

func (f *Fs) listChildrenForNode(ctx context.Context, n *node) ([]*node, error) {
	if n == nil {
		return nil, fs.ErrorObjectNotFound
	}
	if n.kind == "root" {
		groups, err := f.getGroups(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]*node, 0, len(groups))
		for _, g := range groups {
			out = append(out, &node{kind: "group", groupID: g.GroupID, name: g.Name, path: f.opt.Enc.ToStandardName(g.Name)})
		}
		return out, nil
	}
	if n.kind != "group" && n.kind != "folder" {
		return nil, nil
	}
	parentID := int64(0)
	if n.kind == "folder" {
		parentID = n.fileID
	}
	files, err := f.getFiles(ctx, n.groupID, parentID)
	if err != nil {
		return nil, err
	}
	out := make([]*node, 0, len(files))
	for _, item := range files {
		cp := fileInfoToNode(item, path.Join(n.path, f.opt.Enc.ToStandardName(item.Name)), f.isPersonal())
		out = append(out, cp)
	}
	return out, nil
}

func (f *Fs) ensureDir(ctx context.Context, dir string) error {
	f.dirMu.Lock()
	defer f.dirMu.Unlock()
	if f.rootMissing {
		resolved, missing, err := f.resolveRootPath(ctx, f.root)
		if err != nil {
			return err
		}
		if missing {
			parts := []string{}
			if f.root != "" {
				parts = strings.Split(strings.Trim(f.root, "/"), "/")
			}
			if len(parts) == 0 {
				f.rootMissing = false
				f.rootNode = resolved
			} else {
				baseParts := []string{}
				if resolved.path != "" {
					baseParts = strings.Split(strings.Trim(resolved.path, "/"), "/")
				}
				if resolved.kind == "root" && len(baseParts) == 0 {
					return fmt.Errorf("root path %q not found", f.root)
				}
				for _, part := range parts[len(baseParts):] {
					if part == "" {
						continue
					}
					if resolved.kind != "group" && resolved.kind != "folder" {
						return fmt.Errorf("root path %q not found", f.root)
					}
					created, err := f.mkdirUnderNode(ctx, resolved, f.opt.Enc.FromStandardName(part))
					if err != nil {
						return err
					}
					resolved = created
				}
				f.rootNode = resolved
				f.rootMissing = false
			}
		} else {
			f.rootNode = resolved
			f.rootMissing = false
		}
	}
	dir = strings.Trim(dir, "/")
	if dir == "" {
		return nil
	}
	current := f.rootNode
	parts := strings.Split(dir, "/")
	for _, part := range parts {
		children, err := f.listChildrenForNode(ctx, current)
		if err != nil {
			return err
		}
		want := f.opt.Enc.FromStandardName(part)
		var next *node
		for _, child := range children {
			if child.kind != "group" && child.kind != "folder" {
				continue
			}
			if child.name == want {
				next = child
				break
			}
		}
		if next == nil {
			next, err = f.mkdirUnderNode(ctx, current, want)
			if err != nil {
				return err
			}
		}
		current = next
	}
	return nil
}

func (f *Fs) mkdirUnderNode(ctx context.Context, parent *node, name string) (*node, error) {
	if parent == nil || (parent.kind != "group" && parent.kind != "folder") {
		return nil, fs.ErrorNotImplemented
	}
	parentID := int64(0)
	if parent.kind == "folder" {
		parentID = parent.fileID
	}
	body := map[string]any{
		"groupid":  parent.groupID,
		"name":     name,
		"parentid": parentID,
	}
	var result apiResult
	if err := f.doJSONAPI(ctx, http.MethodPost, f.driveURL("/api/v5/files/folder"), body, &result); err != nil {
		return nil, err
	}
	if err := checkAPIResult(result); err != nil {
		return nil, err
	}
	time.Sleep(200 * time.Millisecond)
	children, err := f.listChildrenForNode(ctx, parent)
	if err != nil {
		return nil, err
	}
	for _, child := range children {
		if (child.kind == "folder" || child.kind == "group") && child.name == name {
			return child, nil
		}
	}
	return nil, fmt.Errorf("mkdir succeeded but folder %q not found", name)
}

func (f *Fs) renameNode(ctx context.Context, n *node, newName string) error {
	url := fmt.Sprintf("%s/api/v3/groups/%d/files/%d", f.driveHost()+f.drivePrefix(), n.groupID, n.fileID)
	var result apiResult
	if err := f.doJSONAPI(ctx, http.MethodPut, url, map[string]string{"fname": newName}, &result); err != nil {
		return err
	}
	return checkAPIResult(result)
}

func (f *Fs) moveNode(ctx context.Context, src, dst *node) error {
	targetParentID := int64(0)
	if dst.kind == "folder" {
		targetParentID = dst.fileID
	}
	body := map[string]any{
		"fileids":         []int64{src.fileID},
		"target_groupid":  dst.groupID,
		"target_parentid": targetParentID,
	}
	url := fmt.Sprintf("%s/api/v3/groups/%d/files/batch/move", f.driveHost()+f.drivePrefix(), src.groupID)
	return f.repeatTaskRequest(ctx, url, body)
}

func (f *Fs) copyNode(ctx context.Context, src, dst *node) error {
	targetParentID := int64(0)
	if dst.kind == "folder" {
		targetParentID = dst.fileID
	}
	body := map[string]any{
		"fileids":               []int64{src.fileID},
		"groupid":               src.groupID,
		"target_groupid":        dst.groupID,
		"target_parentid":       targetParentID,
		"duplicated_name_model": 1,
	}
	url := fmt.Sprintf("%s/api/v3/groups/%d/files/batch/copy", f.driveHost()+f.drivePrefix(), src.groupID)
	return f.repeatTaskRequest(ctx, url, body)
}

func (f *Fs) removeNode(ctx context.Context, n *node) error {
	body := map[string]any{
		"fileids": []int64{n.fileID},
	}
	url := fmt.Sprintf("%s/api/v3/groups/%d/files/batch/delete", f.driveHost()+f.drivePrefix(), n.groupID)
	return f.repeatTaskRequest(ctx, url, body)
}

func (f *Fs) repeatTaskRequest(ctx context.Context, endpoint string, body any) error {
	for {
		var res apiResult
		_, err := f.doRequest(ctx, http.MethodPost, endpoint, nil, map[string]string{
			"Content-Type": "application/json",
			"Origin":       f.origin(),
		}, body, &res)
		if err != nil {
			return err
		}
		if res.Result == "fileTaskDuplicated" {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if res.Result != "" && res.Result != "ok" {
			return fmt.Errorf("%s: %s", res.Result, firstNonEmpty(res.Msg, "unknown error"))
		}
		return nil
	}
}

func newReopenableSource(src fs.ObjectInfo) (*reopenableSource, error) {
	if src.Size() < 0 {
		return nil, errors.New("wps upload requires known size")
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
	return nil, fmt.Errorf("wps upload requires reopenable source object; streaming readers without local cache are unsupported (src=%T remote=%q)", src, src.Remote())
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
	source, sourceErr := newReopenableSource(src)
	if sourceErr == nil {
		return f.uploadFromSource(ctx, source, acc, transfer, src, parent, options...)
	}
	return f.uploadFromCache(ctx, baseIn, acc, transfer, src, parent, options...)
}

func (f *Fs) uploadFromCache(ctx context.Context, in io.Reader, acc *accounting.Account, transfer *accounting.Transfer, src fs.ObjectInfo, parent *node, options ...fs.OpenOption) error {
	cachePath, err := cacheFilePath("upload-*")
	if err != nil {
		return err
	}
	tmp, err := os.Create(cachePath)
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()
	written, err := io.Copy(tmp, in)
	if err != nil {
		return err
	}
	if written != src.Size() {
		return fmt.Errorf("wps: expected %d bytes, got %d", src.Size(), written)
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	return f.uploadFromSource(ctx, &reopenableSource{
		open: func(_ context.Context, opts ...fs.OpenOption) (io.ReadCloser, error) {
			file, err := os.Open(tmpName)
			if err != nil {
				return nil, err
			}
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
			if _, err = file.Seek(start, io.SeekStart); err != nil {
				_ = file.Close()
				return nil, err
			}
			reader := io.Reader(file)
			if end >= start {
				reader = io.LimitReader(file, end-start+1)
			}
			return readCloserWithReader{Reader: reader, Closer: file}, nil
		},
	}, acc, transfer, src, parent, options...)
}

func (f *Fs) uploadFromSource(ctx context.Context, source *reopenableSource, acc *accounting.Account, transfer *accounting.Transfer, src fs.ObjectInfo, parent *node, options ...fs.OpenOption) error {
	hashes, err := computeHashes(ctx, source)
	if err != nil {
		return err
	}
	realName := f.opt.Enc.FromStandardName(path.Base(src.Remote()))
	uploadName := realName
	if strings.HasPrefix(realName, ".") {
		uploadName = "_" + realName
	}
	parentID := int64(0)
	if parent.kind == "folder" {
		parentID = parent.fileID
	}
	info, err := f.createUpload(ctx, parent.groupID, parentID, uploadName, hashes.size, hashes.sha1, hashes.sha256)
	if err != nil {
		return err
	}
	return f.performUpload(ctx, source, acc, transfer, src, parent, uploadName, hashes, info, options...)
}

func computeHashes(ctx context.Context, source *reopenableSource) (*hashInfo, error) {
	rc, err := source.OpenRange(ctx, 0, -1)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	h1 := sha1.New()
	h256 := sha256.New()
	n, err := io.Copy(io.MultiWriter(h1, h256), rc)
	if err != nil {
		return nil, err
	}
	return &hashInfo{
		size:   n,
		sha1:   hex.EncodeToString(h1.Sum(nil)),
		sha256: hex.EncodeToString(h256.Sum(nil)),
	}, nil
}

func (f *Fs) createUpload(ctx context.Context, groupID, parentID int64, name string, size int64, sha1Hex, sha256Hex string) (*uploadCreateUpdateResp, error) {
	body := map[string]string{
		"group_id":  strconv.FormatInt(groupID, 10),
		"name":      name,
		"parent_id": strconv.FormatInt(parentID, 10),
		"sha1":      sha1Hex,
		"sha256":    sha256Hex,
		"size":      strconv.FormatInt(size, 10),
	}
	var resp uploadCreateUpdateResp
	if _, err := f.doRequest(ctx, http.MethodPut, f.driveURL("/api/v5/files/upload/create_update"), nil, map[string]string{
		"Content-Type": "application/json",
		"Origin":       f.origin(),
	}, body, &resp); err != nil {
		return nil, err
	}
	if err := checkAPIResult(resp.apiResult); err != nil {
		return nil, err
	}
	if resp.URL == "" {
		return nil, errors.New("empty upload url")
	}
	return &resp, nil
}

func (f *Fs) performUpload(ctx context.Context, source *reopenableSource, acc *accounting.Account, transfer *accounting.Transfer, src fs.ObjectInfo, parent *node, uploadName string, hashes *hashInfo, info *uploadCreateUpdateResp, options ...fs.OpenOption) error {
	parentID := int64(0)
	if parent.kind == "folder" {
		parentID = parent.fileID
	}
	method := strings.ToUpper(strings.TrimSpace(info.Method))
	if method == "" {
		method = http.MethodPut
	}
	rc, err := source.OpenRange(ctx, 0, -1)
	if err != nil {
		return err
	}
	defer rc.Close()
	body := wrapUploadReader(ctx, rc, acc, transfer, nil)
	defer body.Close()
	req, contentLength, err := f.buildUploadRequest(ctx, method, info, uploadName, body, hashes.size, options)
	if err != nil {
		return err
	}
	if contentLength >= 0 {
		req.ContentLength = contentLength
	}
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if !statusOK(resp.StatusCode, info.Response.ExpectCode) {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("wps: upload http %d: %s", resp.StatusCode, string(data))
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	etag := normalizeETag(respArg(info.Response.ArgsETag, resp, respBody))
	if etag == "" {
		etag = normalizeETag(resp.Header.Get("ETag"))
	}
	key := strings.TrimSpace(respArg(info.Response.ArgsKey, resp, respBody))
	if key == "" {
		key = strings.TrimSpace(resp.Header.Get("x-obs-save-key"))
	}
	var putResp uploadPutResp
	sha1FromServer := ""
	if err := json.Unmarshal(respBody, &putResp); err == nil {
		sha1FromServer = strings.TrimSpace(putResp.NewFilename)
		if sha1FromServer == "" {
			sha1FromServer = strings.TrimSpace(putResp.Sha1)
		}
		if etag == "" && putResp.MD5 != "" {
			etag = strings.TrimSpace(putResp.MD5)
		}
	}
	if sha1FromServer == "" {
		if v := extractXMLTag(string(respBody), "ETag"); v != "" {
			sha1FromServer = v
			if etag == "" {
				etag = v
			}
		}
	}
	if sha1FromServer == "" && key != "" && len(key) == 40 {
		sha1FromServer = key
	}
	if sha1FromServer == "" {
		sha1FromServer = hashes.sha1
	}
	if etag == "" {
		return errors.New("empty etag")
	}
	store := strings.TrimSpace(info.Store)
	if store == "" {
		store = "ks3"
	}
	commitKey := ""
	if strings.TrimSpace(info.Response.ArgsKey) != "" {
		commitKey = key
		if commitKey == "" {
			commitKey = sha1FromServer
		}
	}
	return f.commitUpload(ctx, etag, commitKey, parent.groupID, parentID, uploadName, sha1FromServer, hashes.size, store)
}

func (f *Fs) buildUploadRequest(ctx context.Context, method string, info *uploadCreateUpdateResp, uploadName string, body io.Reader, size int64, options []fs.OpenOption) (*http.Request, int64, error) {
	if method == http.MethodPost && len(info.Request.FormData) > 0 {
		pr, pw := io.Pipe()
		mw := multipart.NewWriter(pw)
		req, err := http.NewRequestWithContext(ctx, method, info.URL, pr)
		if err != nil {
			return nil, -1, err
		}
		for k, v := range info.Request.Headers {
			req.Header.Set(k, v)
		}
		req.Header.Set("Content-Type", mw.FormDataContentType())
		fs.OpenOptionAddHTTPHeaders(req.Header, options)
		go func() {
			for k, v := range info.Request.FormData {
				if err := mw.WriteField(k, v); err != nil {
					_ = pw.CloseWithError(err)
					return
				}
			}
			part, err := mw.CreateFormFile("file", uploadName)
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			if _, err = io.Copy(part, body); err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			if err = mw.Close(); err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			_ = pw.Close()
		}()
		return req, -1, nil
	}
	req, err := http.NewRequestWithContext(ctx, method, info.URL, body)
	if err != nil {
		return nil, -1, err
	}
	for k, v := range info.Request.Headers {
		req.Header.Set(k, v)
	}
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	req.Header.Set("Content-Length", strconv.FormatInt(size, 10))
	return req, size, nil
}

func (f *Fs) commitUpload(ctx context.Context, etag, key string, groupID, parentID int64, name, sha1Hex string, size int64, store string) error {
	storeKey := ""
	if key != "" {
		storeKey = key
	}
	body := map[string]any{
		"etag":     etag,
		"groupid":  groupID,
		"key":      key,
		"name":     name,
		"parentid": parentID,
		"sha1":     sha1Hex,
		"size":     size,
		"store":    store,
		"storekey": storeKey,
	}
	var result apiResult
	if err := f.doJSONAPI(ctx, http.MethodPost, f.driveURL("/api/v5/files/file"), body, &result); err != nil {
		return err
	}
	return checkAPIResult(result)
}

func wrapUploadReader(ctx context.Context, rc io.ReadCloser, acc *accounting.Account, transfer *accounting.Transfer, progressAcc **accounting.Account) io.ReadCloser {
	if acc != nil {
		return readCloserWithReader{Reader: acc.WrapStream(rc), Closer: rc}
	}
	if transfer != nil {
		if progressAcc != nil && *progressAcc != nil {
			return readCloserWithReader{Reader: (*progressAcc).WrapStream(rc), Closer: rc}
		}
		wrapped := transfer.Account(ctx, rc)
		if progressAcc != nil {
			*progressAcc = wrapped
		}
		return wrapped
	}
	return rc
}

func fileInfoToNode(item FileInfo, remote string, isPersonal bool) *node {
	kind := "file"
	if item.Type == "folder" {
		kind = "folder"
	}
	canDownload := item.Type != "folder" && (item.FilePerms.Download != 0 || isPersonal)
	return &node{
		kind:        kind,
		groupID:     item.GroupID,
		fileID:      item.ID,
		name:        item.Name,
		path:        remote,
		size:        item.Size,
		modTime:     parseTime(item.Mtime),
		canDownload: canDownload,
	}
}

func parseTime(v int64) time.Time {
	if v <= 0 {
		return time.Time{}
	}
	return time.Unix(v, 0)
}

func statusOK(code int, expect []int) bool {
	if len(expect) == 0 {
		return code >= 200 && code < 300
	}
	for _, v := range expect {
		if v == code {
			return true
		}
	}
	return false
}

func respArg(arg string, resp *http.Response, body []byte) string {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return ""
	}
	l := strings.ToLower(arg)
	if strings.HasPrefix(l, "header.") {
		k := strings.TrimSpace(arg[len("header."):])
		return strings.TrimSpace(resp.Header.Get(k))
	}
	if strings.HasPrefix(l, "body.") {
		k := strings.TrimSpace(arg[len("body."):])
		var m map[string]any
		if err := json.Unmarshal(body, &m); err != nil {
			return ""
		}
		if v, ok := m[k].(string); ok {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func extractXMLTag(v, tag string) string {
	s := strings.TrimSpace(v)
	if s == "" {
		return ""
	}
	open := "<" + strings.ToLower(tag) + ">"
	close := "</" + strings.ToLower(tag) + ">"
	ls := strings.ToLower(s)
	i := strings.Index(ls, open)
	if i < 0 {
		return ""
	}
	i += len(open)
	j := strings.Index(ls[i:], close)
	if j < 0 {
		return ""
	}
	r := strings.TrimSpace(s[i : i+j])
	r = strings.ReplaceAll(r, "&quot;", "")
	return strings.Trim(r, `"'`)
}

func normalizeETag(v string) string {
	v = strings.TrimSpace(v)
	if strings.HasPrefix(v, "W/") {
		v = strings.TrimSpace(strings.TrimPrefix(v, "W/"))
	}
	return strings.Trim(v, `"`)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func normalizeMode(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "business":
		return "Business"
	case "personal", "":
		return "Personal"
	default:
		return strings.TrimSpace(v)
	}
}

func checkAPIResult(result apiResult) error {
	if result.Result != "" && result.Result != "ok" {
		return fmt.Errorf("%s: %s", result.Result, firstNonEmpty(result.Msg, "unknown error"))
	}
	return nil
}

func cacheFilePath(prefix string) (string, error) {
	dir := filepath.Join(config.GetCacheDir(), "wps")
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
func (o *Object) String() string { return o.node.path }
func (o *Object) Remote() string { return o.node.path }
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.node.modTime
}
func (o *Object) Size() int64    { return o.node.size }
func (o *Object) Storable() bool { return true }
func (o *Object) ID() string     { return strconv.FormatInt(o.node.fileID, 10) }
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	if !o.node.canDownload {
		return nil, errors.New("can not download")
	}
	var resp downloadResp
	u := fmt.Sprintf("%s/api/v5/groups/%d/files/%d/download?support_checksums=sha1", o.fs.driveHost()+o.fs.drivePrefix(), o.node.groupID, o.node.fileID)
	if _, err := o.fs.doRequest(ctx, http.MethodGet, u, nil, nil, nil, &resp); err != nil {
		return nil, err
	}
	if strings.TrimSpace(resp.URL) == "" {
		return nil, errors.New("empty download url")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, resp.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", o.fs.userAgent())
	req.Header.Set("Referer", o.fs.driveHost())
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	r, err := o.fs.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if r.StatusCode < 200 || r.StatusCode > 299 {
		data, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		return nil, fmt.Errorf("wps: download http %d: %s", r.StatusCode, string(data))
	}
	return r.Body, nil
}

func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	if src.Size() < 0 {
		return errors.New("wps requires known size")
	}
	return o.fs.upload(ctx, in, src, options...)
}

func (o *Object) Remove(ctx context.Context) error {
	return o.fs.removeNode(ctx, o.node)
}
