// Package aliyunpds provides an interface to the Aliyun PDS storage system.
package aliyunpds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
)

const (
	endpointURL     = "https://web-sv.aliyunpds.com/endpoint/get_endpoints"
	defaultPartSize = 10 * fs.Mebi
	minSleep        = 10 * time.Millisecond
	maxSleep        = 2 * time.Second
	decayConstant   = 2
	defaultUA       = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
)

var retryErrorCodes = []int{408, 429, 500, 502, 503, 504}

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "aliyunpds",
		Description: "Aliyun PDS",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:      "domain_id",
			Help:      "Aliyun PDS domain ID.",
			Required:  true,
			Sensitive: true,
		}, {
			Name:      "drive_id",
			Help:      "Aliyun PDS drive ID.",
			Required:  true,
			Sensitive: true,
		}, {
			Name:      "refresh_token",
			Help:      "Aliyun PDS refresh token.",
			Required:  true,
			Sensitive: true,
		}, {
			Name: "root_folder_id",
			Help: `ID of the root folder.

Leave this as "root" normally.`,
			Default:   "root",
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
			Default:  "ASC",
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			Default: (encoder.EncodeLtGt |
				encoder.EncodeDoubleQuote |
				encoder.EncodeLeftSpace |
				encoder.EncodeRightSpace |
				encoder.EncodeCtl |
				encoder.EncodeInvalidUtf8),
		}},
	})
}

type Options struct {
	DomainID       string               `config:"domain_id"`
	DriveID        string               `config:"drive_id"`
	RefreshToken   string               `config:"refresh_token"`
	RootFolderID   string               `config:"root_folder_id"`
	OrderBy        string               `config:"order_by"`
	OrderDirection string               `config:"order_direction"`
	Enc            encoder.MultiEncoder `config:"encoding"`
}

type Fs struct {
	name         string
	originalName string
	root         string
	opt          Options
	features     *fs.Features
	dirCache     *dircache.DirCache
	pacer        *fs.Pacer
	srv          *http.Client

	mu           sync.Mutex
	accessToken  string
	apiEndpoint  string
	authEndpoint string
	uiEndpoint   string
}

type Object struct {
	fs          *Fs
	remote      string
	id          string
	parentID    string
	size        int64
	modTime     time.Time
	isDir       bool
	sha1sum     string
	hasMetaData bool
}

type respErr struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type tokenResp struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

type endpointResp struct {
	AuthEndpoint string `json:"auth_endpoint"`
	APIEndpoint  string `json:"api_endpoint"`
	UIEndpoint   string `json:"ui_endpoint"`
}

type fileInfo struct {
	FileID          string    `json:"file_id"`
	Name            string    `json:"name"`
	Size            int64     `json:"size"`
	Type            string    `json:"type"`
	UpdatedAt       time.Time `json:"updated_at"`
	ParentFileID    string    `json:"parent_file_id"`
	ContentHash     string    `json:"content_hash"`
	ContentHashName string    `json:"content_hash_name"`
}

type listResp struct {
	Items      []fileInfo `json:"items"`
	NextMarker string     `json:"next_marker"`
}

type uploadResp struct {
	FileID       string `json:"file_id"`
	UploadID     string `json:"upload_id"`
	RapidUpload  bool   `json:"rapid_upload"`
	PartInfoList []struct {
		UploadURL string `json:"upload_url"`
	} `json:"part_info_list"`
}

type driveResp struct {
	UsedSize  int64 `json:"used_size"`
	TotalSize int64 `json:"total_size"`
}

func parsePath(in string) string {
	return strings.Trim(in, "/")
}

func newRawURLRequest(ctx context.Context, method, rawURL string, body io.Reader) (*http.Request, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, parsed.String(), body)
	if err != nil {
		return nil, err
	}
	req.URL = parsed
	return req, nil
}

func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}
	originalName := name
	if idx := strings.IndexRune(name, '{'); idx > 0 {
		originalName = name[:idx]
	}
	root = parsePath(root)
	f := &Fs{
		name:         name,
		originalName: originalName,
		root:         root,
		opt:          *opt,
		pacer:        fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		srv:          fshttp.NewClient(ctx),
	}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		NoMultiThreading:        true,
	}).Fill(ctx, f)
	if err := f.init(ctx); err != nil {
		return nil, err
	}
	rootID := f.opt.RootFolderID
	if rootID == "" {
		rootID = "root"
	}
	f.dirCache = dircache.New(root, rootID, f)
	err := f.dirCache.FindRoot(ctx, false)
	if err != nil {
		newRoot, remote := dircache.SplitPath(root)
		tempF := *f
		tempF.root = newRoot
		tempF.dirCache = dircache.New(newRoot, rootID, &tempF)
		err = tempF.dirCache.FindRoot(ctx, false)
		if err != nil {
			return f, nil
		}
		_, err = tempF.NewObject(ctx, remote)
		if err != nil {
			if err == fs.ErrorObjectNotFound {
				return f, nil
			}
			return nil, err
		}
		f.features.Fill(ctx, &tempF)
		f.dirCache = tempF.dirCache
		f.root = tempF.root
		return f, fs.ErrorIsFile
	}
	return f, nil
}

func (f *Fs) init(ctx context.Context) error {
	var endpoints endpointResp
	_, err := f.callJSON(ctx, http.MethodPost, endpointURL, map[string]any{
		"domain_id":    f.opt.DomainID,
		"is_vpc":       false,
		"product_type": "edm",
	}, &endpoints, false)
	if err != nil {
		return err
	}
	f.apiEndpoint = endpoints.APIEndpoint
	f.authEndpoint = endpoints.AuthEndpoint
	f.uiEndpoint = endpoints.UIEndpoint
	if f.apiEndpoint == "" || f.authEndpoint == "" || f.uiEndpoint == "" {
		return errors.New("aliyunpds: incomplete endpoint response")
	}
	return f.refreshToken(ctx)
}

func (f *Fs) Name() string             { return f.name }
func (f *Fs) Root() string             { return f.root }
func (f *Fs) String() string           { return fmt.Sprintf("Aliyun PDS root '%s'", f.root) }
func (f *Fs) Features() *fs.Features   { return f.features }
func (f *Fs) Precision() time.Duration { return time.Second }
func (f *Fs) Hashes() hash.Set         { return hash.Set(hash.SHA1) }
func (f *Fs) DirCacheFlush()           { f.dirCache.ResetRoot() }

func (f *Fs) refreshToken(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refreshTokenLocked(ctx)
}

func (f *Fs) refreshTokenLocked(ctx context.Context) error {
	var out tokenResp
	_, err := f.callJSONLocked(ctx, http.MethodPost, strings.TrimRight(f.authEndpoint, "/")+"/v2/account/token", map[string]any{
		"refresh_token": f.opt.RefreshToken,
		"grant_type":    "refresh_token",
	}, &out, false, false)
	if err != nil {
		return err
	}
	if out.AccessToken == "" || out.RefreshToken == "" {
		return errors.New("aliyunpds: token refresh returned empty token")
	}
	f.accessToken = out.AccessToken
	f.opt.RefreshToken = out.RefreshToken
	_ = config.SetValueAndSave(f.originalName, "refresh_token", out.RefreshToken)
	return nil
}

func (f *Fs) callJSON(ctx context.Context, method, endpoint string, payload any, out any, auth bool) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callJSONLocked(ctx, method, endpoint, payload, out, auth, true)
}

func (f *Fs) callJSONLocked(ctx context.Context, method, endpoint string, payload any, out any, auth bool, retryAuth bool) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	var data []byte
	err = f.pacer.Call(func() (bool, error) {
		req, reqErr := http.NewRequestWithContext(ctx, method, endpoint, strings.NewReader(string(body)))
		if reqErr != nil {
			return false, reqErr
		}
		req.Header.Set("Accept", "application/json, text/plain, */*")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", defaultUA)
		if auth {
			req.Header.Set("Authorization", "Bearer\t"+f.accessToken)
			req.Header.Set("Origin", f.uiEndpoint)
			req.Header.Set("Referer", f.uiEndpoint+"/")
		}
		resp, doErr := f.srv.Do(req)
		if doErr != nil {
			return shouldRetry(ctx, resp, doErr)
		}
		defer resp.Body.Close()
		data, doErr = io.ReadAll(resp.Body)
		if doErr != nil {
			return shouldRetry(ctx, resp, doErr)
		}
		if resp.StatusCode >= 400 {
			return shouldRetry(ctx, resp, fmt.Errorf("aliyunpds: http %d: %s", resp.StatusCode, string(data)))
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	var apiErr respErr
	if unmarshalErr := json.Unmarshal(data, &apiErr); unmarshalErr == nil && apiErr.Code != "" {
		if apiErr.Code == "AccessTokenInvalid" && auth && retryAuth {
			if err = f.refreshTokenLocked(ctx); err != nil {
				return nil, err
			}
			return f.callJSONLocked(ctx, method, endpoint, payload, out, auth, false)
		}
		if apiErr.Message == "" {
			apiErr.Message = apiErr.Code
		}
		return nil, errors.New(apiErr.Message)
	}
	if out != nil {
		if err = json.Unmarshal(data, out); err != nil {
			return nil, err
		}
	}
	return data, nil
}

func shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

func itemSHA1(item *fileInfo) string {
	if strings.EqualFold(item.ContentHashName, "sha1") {
		return strings.ToLower(item.ContentHash)
	}
	return ""
}

func (f *Fs) listAll(ctx context.Context, dirID string, fn func(*fileInfo) bool) (bool, error) {
	marker := ""
	for {
		var out listResp
		_, err := f.callJSON(ctx, http.MethodPost, strings.TrimRight(f.apiEndpoint, "/")+"/v2/file/list", map[string]any{
			"domain_id":               f.opt.DomainID,
			"drive_id":                f.opt.DriveID,
			"fields":                  "*",
			"image_thumbnail_process": "image/resize,w_400/format,jpeg",
			"image_url_process":       "image/resize,w_1920/format,jpeg",
			"limit":                   200,
			"marker":                  marker,
			"order_by":                f.opt.OrderBy,
			"order_direction":         f.opt.OrderDirection,
			"parent_file_id":          dirID,
			"video_thumbnail_process": "video/snapshot,t_0,f_jpg,ar_auto,w_300",
			"url_expire_sec":          7200,
		}, &out, true)
		if err != nil {
			return false, err
		}
		for i := range out.Items {
			item := out.Items[i]
			item.Name = f.opt.Enc.ToStandardName(item.Name)
			if fn(&item) {
				return true, nil
			}
		}
		if out.NextMarker == "" {
			break
		}
		marker = out.NextMarker
	}
	return false, nil
}

func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (string, bool, error) {
	var foundID string
	found, err := f.listAll(ctx, pathID, func(item *fileInfo) bool {
		if item.Name == leaf && item.Type == "folder" {
			foundID = item.FileID
			return true
		}
		return false
	})
	return foundID, found, err
}

func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (string, error) {
	var out struct {
		FileID string `json:"file_id"`
	}
	_, err := f.callJSON(ctx, http.MethodPost, strings.TrimRight(f.apiEndpoint, "/")+"/v2/file/create", map[string]any{
		"check_name_mode": "refuse",
		"drive_id":        f.opt.DriveID,
		"name":            f.opt.Enc.FromStandardName(leaf),
		"parent_file_id":  pathID,
		"type":            "folder",
		"actionType":      "folder",
	}, &out, true)
	if err != nil {
		return "", err
	}
	if out.FileID != "" {
		return out.FileID, nil
	}
	foundID, found, err := f.FindLeaf(ctx, pathID, leaf)
	if err != nil {
		return "", err
	}
	if found {
		return foundID, nil
	}
	return "", errors.New("aliyunpds: create directory returned empty id")
}

func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	_, err = f.listAll(ctx, dirID, func(item *fileInfo) bool {
		remote := path.Join(dir, item.Name)
		if item.Type == "folder" {
			d := fs.NewDir(remote, item.UpdatedAt).SetID(item.FileID)
			f.dirCache.Put(remote, item.FileID)
			entries = append(entries, d)
			return false
		}
		entries = append(entries, &Object{
			fs:          f,
			remote:      remote,
			id:          item.FileID,
			parentID:    dirID,
			size:        item.Size,
			modTime:     item.UpdatedAt,
			sha1sum:     itemSHA1(item),
			hasMetaData: true,
		})
		return false
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (f *Fs) readMetaDataForPath(ctx context.Context, remote string) (*fileInfo, string, error) {
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, false)
	if err != nil {
		if err == fs.ErrorDirNotFound {
			return nil, "", fs.ErrorObjectNotFound
		}
		return nil, "", err
	}
	var info *fileInfo
	found, err := f.listAll(ctx, dirID, func(item *fileInfo) bool {
		if item.Name == leaf {
			cp := *item
			info = &cp
			return true
		}
		return false
	})
	if err != nil {
		return nil, "", err
	}
	if !found || info == nil {
		return nil, "", fs.ErrorObjectNotFound
	}
	return info, dirID, nil
}

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f.newObjectWithInfo(ctx, remote, nil)
}

func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, item *fileInfo) (fs.Object, error) {
	o := &Object{fs: f, remote: remote}
	if item != nil {
		o.id = item.FileID
		o.parentID = item.ParentFileID
		o.size = item.Size
		o.modTime = item.UpdatedAt
		o.isDir = item.Type == "folder"
		o.sha1sum = itemSHA1(item)
		o.hasMetaData = true
	} else if err := o.readMetaData(ctx); err != nil {
		return nil, err
	}
	if o.isDir {
		return nil, fs.ErrorObjectNotFound
	}
	return o, nil
}

func (f *Fs) createObject(ctx context.Context, remote string, modTime time.Time, size int64) (*Object, string, string, error) {
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, "", "", err
	}
	return &Object{fs: f, remote: remote, parentID: dirID, modTime: modTime, size: size}, leaf, dirID, nil
}

func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	existing, err := f.NewObject(ctx, src.Remote())
	if err == nil {
		return existing, existing.Update(ctx, in, src, options...)
	}
	if err != fs.ErrorObjectNotFound {
		return nil, err
	}
	return f.putUnchecked(ctx, in, src, src.Remote())
}

func (f *Fs) putUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, remote string) (fs.Object, error) {
	size := src.Size()
	if size < 0 {
		return nil, errors.New("aliyunpds: file size unknown")
	}
	o, leaf, dirID, err := f.createObject(ctx, remote, src.ModTime(ctx), size)
	if err != nil {
		return nil, err
	}
	if err = o.upload(ctx, in, leaf, dirID); err != nil {
		return nil, err
	}
	f.dirCache.FlushDir(path.Dir(remote))
	return f.NewObject(ctx, remote)
}

func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

func (f *Fs) purgeCheck(ctx context.Context, dir string, check bool) error {
	if dir == "" {
		return errors.New("aliyunpds: refusing to remove backend root")
	}
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	if check {
		found, err := f.listAll(ctx, dirID, func(item *fileInfo) bool { return true })
		if err != nil {
			return err
		}
		if found {
			return fs.ErrorDirectoryNotEmpty
		}
	}
	_, err = f.callJSON(ctx, http.MethodPost, strings.TrimRight(f.apiEndpoint, "/")+"/v2/recyclebin/trash", map[string]any{
		"drive_id":    f.opt.DriveID,
		"file_id":     dirID,
		"permanently": true,
	}, nil, true)
	if err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	return nil
}

func (f *Fs) Rmdir(ctx context.Context, dir string) error { return f.purgeCheck(ctx, dir, true) }
func (f *Fs) Purge(ctx context.Context, dir string) error { return f.purgeCheck(ctx, dir, false) }

func (f *Fs) renameByID(ctx context.Context, fileID, leaf string) error {
	_, err := f.callJSON(ctx, http.MethodPost, strings.TrimRight(f.apiEndpoint, "/")+"/v2/file/update", map[string]any{
		"check_name_mode": "refuse",
		"drive_id":        f.opt.DriveID,
		"file_id":         fileID,
		"name":            f.opt.Enc.FromStandardName(leaf),
	}, nil, true)
	return err
}

func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantMove
	}
	if err := srcObj.readMetaData(ctx); err != nil {
		return nil, err
	}
	dstLeaf, dstParentID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	srcLeaf, srcParentID, err := srcObj.fs.dirCache.FindPath(ctx, srcObj.remote, false)
	if err != nil {
		srcLeaf = path.Base(srcObj.remote)
		srcParentID = srcObj.parentID
	}
	if srcParentID != dstParentID {
		_, err = f.callJSON(ctx, http.MethodPost, strings.TrimRight(f.apiEndpoint, "/")+"/v2/file/move", map[string]any{
			"auto_rename":       true,
			"drive_id":          f.opt.DriveID,
			"file_id":           srcObj.id,
			"to_drive_id":       f.opt.DriveID,
			"to_parent_file_id": dstParentID,
		}, nil, true)
		if err != nil {
			return nil, err
		}
	}
	if srcLeaf != dstLeaf {
		if err = f.renameByID(ctx, srcObj.id, dstLeaf); err != nil {
			return nil, err
		}
	}
	srcObj.fs.dirCache.FlushDir(path.Dir(srcObj.remote))
	f.dirCache.FlushDir(path.Dir(remote))
	return f.NewObject(ctx, remote)
}

func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok {
		return fs.ErrorCantDirMove
	}
	srcID, srcParentID, srcLeaf, dstParentID, dstLeaf, err := f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}
	if srcParentID != dstParentID {
		_, err = f.callJSON(ctx, http.MethodPost, strings.TrimRight(f.apiEndpoint, "/")+"/v2/file/move", map[string]any{
			"auto_rename":       true,
			"drive_id":          f.opt.DriveID,
			"file_id":           srcID,
			"to_drive_id":       f.opt.DriveID,
			"to_parent_file_id": dstParentID,
		}, nil, true)
		if err != nil {
			return err
		}
	}
	if srcLeaf != dstLeaf {
		if err = f.renameByID(ctx, srcID, dstLeaf); err != nil {
			return err
		}
	}
	srcFs.dirCache.FlushDir(srcRemote)
	f.dirCache.FlushDir(path.Dir(dstRemote))
	return nil
}

func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantCopy
	}
	if err := srcObj.readMetaData(ctx); err != nil {
		return nil, err
	}
	dstLeaf, dstParentID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	_, err = f.callJSON(ctx, http.MethodPost, strings.TrimRight(f.apiEndpoint, "/")+"/v2/file/copy", map[string]any{
		"auto_rename":       true,
		"drive_id":          f.opt.DriveID,
		"file_id":           srcObj.id,
		"to_drive_id":       f.opt.DriveID,
		"to_parent_file_id": dstParentID,
	}, nil, true)
	if err != nil {
		return nil, err
	}
	f.dirCache.FlushDir(path.Dir(remote))
	srcLeaf := path.Base(srcObj.remote)
	if srcLeaf != dstLeaf {
		copied, copyErr := f.NewObject(ctx, path.Join(path.Dir(remote), srcLeaf))
		if copyErr != nil {
			return nil, copyErr
		}
		if err = f.renameByID(ctx, copied.(*Object).id, dstLeaf); err != nil {
			return nil, err
		}
	}
	return f.NewObject(ctx, remote)
}

func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	var out driveResp
	_, err := f.callJSON(ctx, http.MethodPost, strings.TrimRight(f.apiEndpoint, "/")+"/v2/drive/get", map[string]any{"drive_id": f.opt.DriveID}, &out, true)
	if err != nil {
		return nil, err
	}
	return &fs.Usage{
		Total: fs.NewUsageValue(out.TotalSize),
		Used:  fs.NewUsageValue(out.UsedSize),
		Free:  fs.NewUsageValue(out.TotalSize - out.UsedSize),
	}, nil
}

func (o *Object) upload(ctx context.Context, in io.Reader, leaf, parentID string) error {
	size := o.size
	if size < 0 {
		return errors.New("aliyunpds: file size unknown")
	}
	partSize := int64(defaultPartSize)
	count := 0
	if size > 0 {
		count = int(math.Ceil(float64(size) / float64(partSize)))
	}
	partInfoList := make([]map[string]int, 0, count)
	for i := 1; i <= count; i++ {
		partInfoList = append(partInfoList, map[string]int{"part_number": i})
	}
	var create uploadResp
	_, err := o.fs.callJSON(ctx, http.MethodPost, strings.TrimRight(o.fs.apiEndpoint, "/")+"/v2/file/create", map[string]any{
		"check_name_mode":   "refuse",
		"drive_id":          o.fs.opt.DriveID,
		"name":              o.fs.opt.Enc.FromStandardName(leaf),
		"parent_file_id":    parentID,
		"part_info_list":    partInfoList,
		"size":              size,
		"type":              "file",
		"content_hash_name": "none",
	}, &create, true)
	if err != nil {
		return err
	}
	if create.RapidUpload {
		return nil
	}
	if len(create.PartInfoList) != count {
		return fmt.Errorf("aliyunpds: upload part count mismatch: expected %d got %d", count, len(create.PartInfoList))
	}
	remaining := size
	for _, part := range create.PartInfoList {
		chunkSize := minInt64(partSize, remaining)
		req, reqErr := newRawURLRequest(ctx, http.MethodPut, part.UploadURL, io.LimitReader(in, chunkSize))
		if reqErr != nil {
			return reqErr
		}
		req.ContentLength = chunkSize
		resp, doErr := o.fs.srv.Do(req)
		if doErr != nil {
			return doErr
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("aliyunpds: upload failed: http %d: %s", resp.StatusCode, string(body))
		}
		remaining -= chunkSize
	}
	_, err = o.fs.callJSON(ctx, http.MethodPost, strings.TrimRight(o.fs.apiEndpoint, "/")+"/v2/file/complete", map[string]any{
		"drive_id":  o.fs.opt.DriveID,
		"file_id":   create.FileID,
		"upload_id": create.UploadID,
	}, nil, true)
	return err
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (o *Object) Fs() fs.Info    { return o.fs }
func (o *Object) String() string { return o.remote }
func (o *Object) Remote() string { return o.remote }
func (o *Object) Size() int64    { return o.size }
func (o *Object) Storable() bool { return true }
func (o *Object) ID() string     { return o.id }

func (o *Object) ModTime(ctx context.Context) time.Time {
	if err := o.readMetaData(ctx); err != nil {
		fs.Debugf(o, "Failed to read metadata: %v", err)
	}
	return o.modTime
}

func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t != hash.SHA1 {
		return "", hash.ErrUnsupported
	}
	if err := o.readMetaData(ctx); err != nil {
		return "", err
	}
	if o.sha1sum == "" {
		return "", hash.ErrUnsupported
	}
	return o.sha1sum, nil
}

func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}

func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	if err := o.readMetaData(ctx); err != nil {
		return nil, err
	}
	var out struct {
		URL string `json:"url"`
	}
	_, err := o.fs.callJSON(ctx, http.MethodPost, strings.TrimRight(o.fs.apiEndpoint, "/")+"/v2/file/get_download_url", map[string]any{
		"drive_id":   o.fs.opt.DriveID,
		"file_id":    o.id,
		"file_name":  o.fs.opt.Enc.FromStandardName(path.Base(o.remote)),
		"expire_sec": 7200,
	}, &out, true)
	if err != nil {
		return nil, err
	}
	if out.URL == "" {
		return nil, errors.New("aliyunpds: empty download url")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, out.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Referer", o.fs.uiEndpoint+"/")
	headers := map[string]string{}
	fs.OpenOptionAddHeaders(options, headers)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := o.fs.srv.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("aliyunpds: download failed: http %d: %s", resp.StatusCode, string(body))
	}
	return resp.Body, nil
}

func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	if err := o.Remove(ctx); err != nil {
		return err
	}
	newObj, err := o.fs.putUnchecked(ctx, in, src, src.Remote())
	if err != nil {
		return err
	}
	*o = *(newObj.(*Object))
	return nil
}

func (o *Object) Remove(ctx context.Context) error {
	if err := o.readMetaData(ctx); err != nil {
		return err
	}
	_, err := o.fs.callJSON(ctx, http.MethodPost, strings.TrimRight(o.fs.apiEndpoint, "/")+"/v2/recyclebin/trash", map[string]any{
		"drive_id":    o.fs.opt.DriveID,
		"file_id":     o.id,
		"permanently": true,
	}, nil, true)
	if err == nil {
		o.fs.dirCache.FlushDir(path.Dir(o.remote))
	}
	return err
}

func (o *Object) readMetaData(ctx context.Context) error {
	if o.hasMetaData {
		return nil
	}
	info, parentID, err := o.fs.readMetaDataForPath(ctx, o.remote)
	if err != nil {
		return err
	}
	o.id = info.FileID
	o.parentID = parentID
	o.size = info.Size
	o.modTime = info.UpdatedAt
	o.isDir = info.Type == "folder"
	o.sha1sum = itemSHA1(info)
	o.hasMetaData = true
	if o.isDir {
		return fs.ErrorObjectNotFound
	}
	return nil
}

var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Purger          = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.Copier          = (*Fs)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ fs.DirCacheFlusher = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.IDer            = (*Object)(nil)
)
