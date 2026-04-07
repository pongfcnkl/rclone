// Package _139 provides an interface to China Mobile 139 cloud drive.
//
// This backend currently ports the "personal_new" mode from OpenList's 139
// driver. It supports listing, downloading, uploading, moving, copying,
// renaming, deleting and quota reporting for the personal drive API.
package _139

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	cryptoRand "crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
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
	routeURL      = "https://user-njs.yun.139.com/user/route/qryRoutePolicy"
	personalDisk  = "https://user-njs.yun.139.com/user/disk/getPersonalDiskInfo"
	tokenRefresh  = "https://aas.caiyun.feixin.10086.cn:443/tellin/authTokenRefresh.do"
	defaultUA     = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
	defaultAppVer = "7.14.0"
	keyHex1       = "73634235495062495331515373756c734e7253306c673d3d"
	keyHex2       = "7150714477323633586746674c337538"

	defaultPartSize = 100 * fs.Mebi
	largePartSize   = 512 * fs.Mebi
	minSleep        = 100 * time.Millisecond
	maxSleep        = 2 * time.Second
	decayConstant   = 2
)

var retryErrorCodes = []int{429, 500, 502, 503, 504}

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "139",
		Description: "China Mobile 139 Cloud (personal drive)",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:      "authorization",
			Help:      "Base64 encoded authorization string used by 139 cloud web APIs.",
			Required:  true,
			Sensitive: true,
		}, {
			Name:      "username",
			Help:      "Phone number used for password login fallback.",
			Sensitive: true,
		}, {
			Name:      "password",
			Help:      "Password used for password login fallback.",
			Sensitive: true,
		}, {
			Name:      "mail_cookies",
			Help:      "Cookies from mail.10086.cn used by the password login fallback flow.",
			Sensitive: true,
		}, {
			Name: "user_domain_id",
			Help: `Optional ud_id value for quota reporting.

Fill this if you want the backend to expose usage information via rclone about.`,
			Sensitive: true,
		}, {
			Name: "root_folder_id",
			Help: `ID of the root folder.

Leave blank to use the personal drive root "/".`,
			Advanced: true,
		}, {
			Name: "custom_upload_part_size",
			Help: `Custom multipart upload part size in bytes.

Set to 0 to use the backend default strategy.`,
			Default:  int64(0),
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

// Options defines the configuration of this backend.
type Options struct {
	Authorization        string               `config:"authorization"`
	Username             string               `config:"username"`
	Password             string               `config:"password"`
	MailCookies          string               `config:"mail_cookies"`
	UserDomainID         string               `config:"user_domain_id"`
	RootFolderID         string               `config:"root_folder_id"`
	CustomUploadPartSize int64                `config:"custom_upload_part_size"`
	Enc                  encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a 139 remote.
type Fs struct {
	name         string
	originalName string
	root         string
	opt          Options
	features     *fs.Features
	dirCache     *dircache.DirCache
	pacer        *fs.Pacer
	srv          *http.Client

	mu                sync.Mutex
	account           string
	personalCloudHost string
	rootFolderID      string
}

// Object describes a remote file.
type Object struct {
	fs          *Fs
	remote      string
	id          string
	parentID    string
	size        int64
	modTime     time.Time
	createdTime time.Time
	isDir       bool
	hasMetaData bool
}

type baseResp struct {
	Success bool   `json:"success"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type personalThumbnail struct {
	Style string `json:"style"`
	URL   string `json:"url"`
}

type personalFileItem struct {
	FileID     string              `json:"fileId"`
	Name       string              `json:"name"`
	Size       int64               `json:"size"`
	Type       string              `json:"type"`
	CreatedAt  string              `json:"createdAt"`
	UpdatedAt  string              `json:"updatedAt"`
	Thumbnails []personalThumbnail `json:"thumbnailUrls"`
}

type personalListResp struct {
	baseResp
	Data struct {
		Items          []personalFileItem `json:"items"`
		NextPageCursor string             `json:"nextPageCursor"`
	} `json:"data"`
}

type personalPartInfo struct {
	PartNumber int    `json:"partNumber"`
	UploadURL  string `json:"uploadUrl"`
}

type personalUploadResp struct {
	baseResp
	Data struct {
		FileID      string             `json:"fileId"`
		FileName    string             `json:"fileName"`
		PartInfos   []personalPartInfo `json:"partInfos"`
		Exist       bool               `json:"exist"`
		RapidUpload bool               `json:"rapidUpload"`
		UploadID    string             `json:"uploadId"`
	} `json:"data"`
}

type personalUploadURLResp struct {
	baseResp
	Data struct {
		FileID    string             `json:"fileId"`
		UploadID  string             `json:"uploadId"`
		PartInfos []personalPartInfo `json:"partInfos"`
	} `json:"data"`
}

type routePolicyResp struct {
	baseResp
	Data struct {
		RoutePolicyList []struct {
			ModName  string `json:"modName"`
			HttpsURL string `json:"httpsUrl"`
		} `json:"routePolicyList"`
	} `json:"data"`
}

type refreshTokenResp struct {
	XMLName xml.Name `xml:"root"`
	Return  string   `xml:"return"`
	Token   string   `xml:"token"`
	Desc    string   `xml:"desc"`
}

type diskInfoResp struct {
	baseResp
	Data struct {
		FreeDiskSize string `json:"freeDiskSize"`
		DiskSize     string `json:"diskSize"`
	} `json:"data"`
}

type openResult struct {
	URL    string `json:"url"`
	CdnURL string `json:"cdnUrl"`
}

type uploadPart struct {
	PartNumber      int   `json:"partNumber"`
	PartSize        int64 `json:"partSize"`
	ParallelHashCtx struct {
		PartOffset int64 `json:"partOffset"`
	} `json:"parallelHashCtx"`
}

type itemListFn func(item *personalFileItem) bool

// NewFs constructs a new backend.
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

	f.dirCache = dircache.New(f.root, f.rootFolderID, f)
	if f.root != "" {
		if err := f.dirCache.FindRoot(ctx, false); err != nil {
			newRoot, remote := dircache.SplitPath(f.root)
			tempF := *f
			tempF.root = newRoot
			tempF.dirCache = dircache.New(newRoot, f.rootFolderID, &tempF)
			if err = tempF.dirCache.FindRoot(ctx, false); err != nil {
				return f, nil
			}
			if _, err = tempF.newObjectWithInfo(ctx, remote, nil); err != nil {
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
	}
	return f, nil
}

func (f *Fs) init(ctx context.Context) error {
	if f.opt.Authorization == "" {
		return errors.New("139: authorization is required")
	}
	if err := f.refreshToken(ctx); err != nil {
		return err
	}
	host, err := f.queryPersonalCloudHost(ctx)
	if err != nil {
		return err
	}
	f.personalCloudHost = host
	f.rootFolderID = f.opt.RootFolderID
	if f.rootFolderID == "" {
		f.rootFolderID = "/"
	}
	return nil
}

func (f *Fs) Name() string          { return f.name }
func (f *Fs) Root() string          { return f.root }
func (f *Fs) String() string        { return fmt.Sprintf("139 %s", f.root) }
func (f *Fs) Features() *fs.Features { return f.features }
func (f *Fs) Precision() time.Duration { return time.Millisecond }
func (f *Fs) Hashes() hash.Set      { return hash.Set(hash.None) }
func (f *Fs) DirCacheFlush()        { f.dirCache.ResetRoot() }

func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (string, bool, error) {
	var foundID string
	found, err := f.listAll(ctx, pathID, func(item *personalFileItem) bool {
		if f.opt.Enc.ToStandardName(item.Name) == leaf {
			foundID = item.FileID
			return true
		}
		return false
	})
	return foundID, found, err
}

func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (string, error) {
	payload := map[string]any{
		"parentFileId":   pathID,
		"name":           f.opt.Enc.FromStandardName(leaf),
		"description":    "",
		"type":           "folder",
		"fileRenameMode": "force_rename",
	}
	var out struct {
		baseResp
		Data struct {
			FileID string `json:"fileId"`
		} `json:"data"`
	}
	if _, err := f.personalCall(ctx, http.MethodPost, "/file/create", payload, &out); err != nil {
		return "", err
	}
	if out.Data.FileID == "" {
		foundID, found, err := f.FindLeaf(ctx, pathID, leaf)
		if err != nil {
			return "", err
		}
		if found {
			return foundID, nil
		}
		return "", errors.New("139: mkdir succeeded but no folder id was returned")
	}
	return out.Data.FileID, nil
}

func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	_, err = f.listAll(ctx, dirID, func(item *personalFileItem) bool {
		remote := path.Join(dir, f.opt.Enc.ToStandardName(item.Name))
		if item.Type == "folder" {
			d := fs.NewDir(remote, parsePersonalTime(item.UpdatedAt)).SetID(item.FileID)
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
			modTime:     parsePersonalTime(item.UpdatedAt),
			createdTime: parsePersonalTime(item.CreatedAt),
			hasMetaData: true,
		})
		return false
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f.newObjectWithInfo(ctx, remote, nil)
}

func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, item *personalFileItem) (fs.Object, error) {
	o := &Object{fs: f, remote: remote}
	if item != nil {
		o.id = item.FileID
		o.size = item.Size
		o.modTime = parsePersonalTime(item.UpdatedAt)
		o.createdTime = parsePersonalTime(item.CreatedAt)
		o.isDir = item.Type == "folder"
		o.hasMetaData = true
	} else {
		if err := o.readMetaData(ctx); err != nil {
			return nil, err
		}
	}
	if o.isDir {
		return nil, fs.ErrorObjectNotFound
	}
	return o, nil
}

func (f *Fs) readMetaDataForPath(ctx context.Context, remote string) (*personalFileItem, string, error) {
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, false)
	if err != nil {
		if err == fs.ErrorDirNotFound {
			return nil, "", fs.ErrorObjectNotFound
		}
		return nil, "", err
	}
	var info *personalFileItem
	found, err := f.listAll(ctx, dirID, func(item *personalFileItem) bool {
		if f.opt.Enc.ToStandardName(item.Name) == leaf {
			cpy := *item
			info = &cpy
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

func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	existing, err := f.NewObject(ctx, src.Remote())
	if err == nil {
		if err = existing.Update(ctx, in, src, options...); err != nil {
			return nil, err
		}
		return existing, nil
	}
	if err != fs.ErrorObjectNotFound {
		return nil, err
	}
	return f.putUnchecked(ctx, in, src, src.Remote(), options...)
}

func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.putUnchecked(ctx, in, src, src.Remote(), options...)
}

func (f *Fs) putUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, remote string, options ...fs.OpenOption) (fs.Object, error) {
	baseIn, wrap := accounting.UnWrap(in)
	o, leaf, dirID, err := f.createObject(ctx, remote, src.ModTime(ctx), src.Size())
	if err != nil {
		return nil, err
	}
	if err = o.upload(ctx, baseIn, wrap, src, leaf, dirID); err != nil {
		return nil, err
	}
	f.dirCache.FlushDir(path.Dir(remote))
	return f.NewObject(ctx, remote)
}

func (f *Fs) createObject(ctx context.Context, remote string, modTime time.Time, size int64) (*Object, string, string, error) {
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, "", "", err
	}
	return &Object{
		fs:       f,
		remote:   remote,
		parentID: dirID,
		modTime:  modTime,
		size:     size,
	}, leaf, dirID, nil
}

func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

func (f *Fs) Rmdir(ctx context.Context, dir string) error { return f.purgeCheck(ctx, dir, true) }
func (f *Fs) Purge(ctx context.Context, dir string) error { return f.purgeCheck(ctx, dir, false) }

func (f *Fs) purgeCheck(ctx context.Context, dir string, check bool) error {
	if dir == "" && f.rootFolderID == "/" {
		return errors.New("139: refusing to remove absolute root")
	}
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	if check {
		found, err := f.listAll(ctx, dirID, func(item *personalFileItem) bool { return true })
		if err != nil {
			return err
		}
		if found {
			return fs.ErrorDirectoryNotEmpty
		}
	}
	if _, err := f.personalCall(ctx, http.MethodPost, "/recyclebin/batchTrash", map[string]any{"fileIds": []string{dirID}}, nil); err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	return nil
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
		srcParentID = srcObj.parentID
	}
	if srcParentID != dstParentID {
		if _, err = f.personalCall(ctx, http.MethodPost, "/file/batchMove", map[string]any{
			"fileIds":        []string{srcObj.id},
			"toParentFileId": dstParentID,
		}, nil); err != nil {
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
		if _, err = f.personalCall(ctx, http.MethodPost, "/file/batchMove", map[string]any{
			"fileIds":        []string{srcID},
			"toParentFileId": dstParentID,
		}, nil); err != nil {
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
	if _, err = f.personalCall(ctx, http.MethodPost, "/file/batchCopy", map[string]any{
		"fileIds":        []string{srcObj.id},
		"toParentFileId": dstParentID,
	}, nil); err != nil {
		return nil, err
	}
	f.dirCache.FlushDir(path.Dir(remote))
	srcName := path.Base(srcObj.remote)
	if srcName != dstLeaf {
		copied, err := f.NewObject(ctx, path.Join(path.Dir(remote), srcName))
		if err != nil {
			return nil, err
		}
		if err = f.renameByID(ctx, copied.(*Object).id, dstLeaf); err != nil {
			return nil, err
		}
	}
	return f.NewObject(ctx, remote)
}

func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	if f.opt.UserDomainID == "" {
		return nil, errors.New("139: user_domain_id is required for quota reporting")
	}
	var resp diskInfoResp
	if _, err := f.callJSON(ctx, http.MethodPost, personalDisk, map[string]any{"userDomainId": f.opt.UserDomainID}, &resp, false); err != nil {
		return nil, err
	}
	totalMB, err := strconv.ParseInt(resp.Data.DiskSize, 10, 64)
	if err != nil {
		return nil, err
	}
	freeMB, err := strconv.ParseInt(resp.Data.FreeDiskSize, 10, 64)
	if err != nil {
		return nil, err
	}
	total := totalMB * 1024 * 1024
	free := freeMB * 1024 * 1024
	used := total - free
	return &fs.Usage{
		Total: fs.NewUsageValue(total),
		Used:  fs.NewUsageValue(used),
		Free:  fs.NewUsageValue(free),
	}, nil
}

func (f *Fs) Shutdown(ctx context.Context) error { return nil }

func (f *Fs) listAll(ctx context.Context, dirID string, fn itemListFn) (bool, error) {
	cursor := ""
	for {
		var resp personalListResp
		_, err := f.personalCall(ctx, http.MethodPost, "/file/list", map[string]any{
			"imageThumbnailStyleList": []string{"Small", "Large"},
			"orderBy":                 "updated_at",
			"orderDirection":          "DESC",
			"pageInfo": map[string]any{
				"pageCursor": cursor,
				"pageSize":   100,
			},
			"parentFileId": dirID,
		}, &resp)
		if err != nil {
			return false, err
		}
		for i := range resp.Data.Items {
			if fn(&resp.Data.Items[i]) {
				return true, nil
			}
		}
		if resp.Data.NextPageCursor == "" {
			break
		}
		cursor = resp.Data.NextPageCursor
	}
	return false, nil
}

func (f *Fs) renameByID(ctx context.Context, fileID, newName string) error {
	_, err := f.personalCall(ctx, http.MethodPost, "/file/update", map[string]any{
		"fileId":      fileID,
		"name":        f.opt.Enc.FromStandardName(newName),
		"description": "",
	}, nil)
	return err
}

func (f *Fs) openURL(ctx context.Context, fileID string) (string, error) {
	var out struct {
		baseResp
		Data openResult `json:"data"`
	}
	if _, err := f.personalCall(ctx, http.MethodPost, "/file/getDownloadUrl", map[string]any{"fileId": fileID}, &out); err != nil {
		return "", err
	}
	if out.Data.CdnURL != "" {
		return out.Data.CdnURL, nil
	}
	if out.Data.URL != "" {
		return out.Data.URL, nil
	}
	return "", errors.New("139: empty download url")
}

func (f *Fs) queryPersonalCloudHost(ctx context.Context) (string, error) {
	var resp routePolicyResp
	_, err := f.callJSON(ctx, http.MethodPost, routeURL, map[string]any{
		"userInfo": map[string]any{
			"userType":    1,
			"accountType": 1,
			"accountName": f.account,
		},
		"modAddrType": 1,
	}, &resp, false)
	if err != nil {
		return "", err
	}
	for _, item := range resp.Data.RoutePolicyList {
		if item.ModName == "personal" && item.HttpsURL != "" {
			return item.HttpsURL, nil
		}
	}
	return "", errors.New("139: personal cloud host not found")
}

func (f *Fs) getAuthorization() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.opt.Authorization
}

func (f *Fs) saveAuthorization(newAuth string) {
	f.mu.Lock()
	f.opt.Authorization = newAuth
	account, _ := decodeAuthorizationAccount(newAuth)
	if account != "" {
		f.account = account
	}
	f.mu.Unlock()
	_ = config.SetValueAndSave(f.originalName, "authorization", newAuth)
}

func (f *Fs) refreshToken(ctx context.Context) error {
	account, token, expiry, err := decodeAuthorization(f.opt.Authorization)
	if err != nil {
		return err
	}
	f.account = account
	if time.Until(expiry) > 15*24*time.Hour {
		return nil
	}
	if time.Now().After(expiry) {
		if f.opt.Username == "" || f.opt.Password == "" || f.opt.MailCookies == "" {
			return errors.New("139: authorization expired and password login fallback is not configured")
		}
		newAuth, err := f.loginWithPassword(ctx)
		if err != nil {
			return err
		}
		f.saveAuthorization(newAuth)
		return nil
	}
	body := "<root><token>" + token + "</token><account>" + account + "</account><clienttype>656</clienttype></root>"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenRefresh, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/xml")
	req.Header.Set("User-Agent", defaultUA)
	resp, err := f.do(req, nil)
	if err != nil {
		if f.opt.Username == "" || f.opt.Password == "" || f.opt.MailCookies == "" {
			return err
		}
		newAuth, loginErr := f.loginWithPassword(ctx)
		if loginErr != nil {
			return fmt.Errorf("refresh failed: %w; login fallback failed: %v", err, loginErr)
		}
		f.saveAuthorization(newAuth)
		return nil
	}
	defer resp.Body.Close()
	var out refreshTokenResp
	if err = xml.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	if out.Return != "0" || out.Token == "" {
		if f.opt.Username == "" || f.opt.Password == "" || f.opt.MailCookies == "" {
			if out.Desc != "" {
				return errors.New(out.Desc)
			}
			return errors.New("139: token refresh failed")
		}
		newAuth, loginErr := f.loginWithPassword(ctx)
		if loginErr != nil {
			return fmt.Errorf("refresh failed: %s; login fallback failed: %v", out.Desc, loginErr)
		}
		f.saveAuthorization(newAuth)
		return nil
	}
	f.saveAuthorization(base64.StdEncoding.EncodeToString([]byte("pc:" + account + ":" + out.Token)))
	return nil
}

func decodeAuthorizationAccount(auth string) (string, error) {
	account, _, _, err := decodeAuthorization(auth)
	return account, err
}

func decodeAuthorization(auth string) (account string, token string, expiry time.Time, err error) {
	raw, err := base64.StdEncoding.DecodeString(auth)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("139: authorization decode failed: %w", err)
	}
	parts := strings.Split(string(raw), ":")
	if len(parts) < 3 {
		return "", "", time.Time{}, errors.New("139: invalid authorization")
	}
	account = parts[1]
	token = parts[2]
	expiry = time.Now().Add(365 * 24 * time.Hour)
	tokenParts := strings.Split(token, "|")
	if len(tokenParts) >= 4 {
		ms, convErr := strconv.ParseInt(tokenParts[3], 10, 64)
		if convErr == nil {
			expiry = time.UnixMilli(ms)
		}
	}
	return
}

func (f *Fs) personalCall(ctx context.Context, method, apiPath string, payload any, out any) ([]byte, error) {
	return f.callJSON(ctx, method, strings.TrimRight(f.personalCloudHost, "/")+apiPath, payload, out, true)
}

func (f *Fs) callJSON(ctx context.Context, method, endpoint string, payload any, out any, personal bool) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	f.setHeaders(req, body, personal)
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	resp, err := f.do(req, shouldRetry)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var check baseResp
	if err = json.Unmarshal(data, &check); err == nil && !check.Success {
		if check.Message == "" {
			check.Message = string(data)
		}
		return nil, errors.New(check.Message)
	}
	if out != nil {
		if err = json.Unmarshal(data, out); err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (f *Fs) setHeaders(req *http.Request, body []byte, personal bool) {
	randStr := randomString(16)
	ts := time.Now().Format("2006-01-02 15:04:05")
	sign := calcSign(string(body), ts, randStr)
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Authorization", "Basic "+f.getAuthorization())
	req.Header.Set("Cms-Device", "default")
	req.Header.Set("Mcloud-Channel", "1000101")
	req.Header.Set("Mcloud-Client", "10701")
	req.Header.Set("Mcloud-Version", defaultAppVer)
	req.Header.Set("Mcloud-Sign", fmt.Sprintf("%s,%s,%s", ts, randStr, sign))
	req.Header.Set("Origin", "https://yun.139.com")
	req.Header.Set("Referer", "https://yun.139.com/")
	req.Header.Set("User-Agent", defaultUA)
	req.Header.Set("x-DeviceInfo", "||9|7.14.0|chrome|141.0.0.0|||windows 10||zh-CN|||")
	req.Header.Set("x-huawei-channelSrc", "10000034")
	req.Header.Set("x-inner-ntwk", "2")
	req.Header.Set("x-m4c-caller", "PC")
	req.Header.Set("x-m4c-src", "10002")
	req.Header.Set("x-SvcType", "1")
	req.Header.Set("Inner-Hcy-Router-Https", "1")
	if personal {
		req.Header.Set("Caller", "web")
		req.Header.Set("Mcloud-Route", "001")
		req.Header.Set("X-Yun-Api-Version", "v1")
		req.Header.Set("X-Yun-App-Channel", "10000034")
		req.Header.Set("X-Yun-Channel-Source", "10000034")
		req.Header.Set("X-Yun-Client-Info", "||9|7.14.0|chrome|141.0.0.0|||windows 10||zh-CN|||dW5kZWZpbmVk||")
		req.Header.Set("X-Yun-Module-Type", "100")
		req.Header.Set("X-Yun-Svc-Type", "1")
	}
}

func calcSign(body, ts, randStr string) string {
	body = encodeURIComponent(body)
	parts := strings.Split(body, "")
	for i := 1; i < len(parts); i++ {
		for j := i; j > 0 && parts[j] < parts[j-1]; j-- {
			parts[j], parts[j-1] = parts[j-1], parts[j]
		}
	}
	sortedBody := strings.Join(parts, "")
	encoded := base64.StdEncoding.EncodeToString([]byte(sortedBody))
	return strings.ToUpper(md5Hex(md5Hex(encoded) + md5Hex(ts+":"+randStr)))
}

func encodeURIComponent(str string) string {
	r := url.QueryEscape(str)
	r = strings.ReplaceAll(r, "+", "%20")
	r = strings.ReplaceAll(r, "%21", "!")
	r = strings.ReplaceAll(r, "%27", "'")
	r = strings.ReplaceAll(r, "%28", "(")
	r = strings.ReplaceAll(r, "%29", ")")
	r = strings.ReplaceAll(r, "%2A", "*")
	return r
}

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

func shouldRetry(resp *http.Response, err error) (bool, error) {
	if err != nil {
		return fserrors.ShouldRetry(err), err
	}
	for _, code := range retryErrorCodes {
		if resp != nil && resp.StatusCode == code {
			return true, nil
		}
	}
	return false, nil
}

func (f *Fs) do(req *http.Request, retry func(*http.Response, error) (bool, error)) (*http.Response, error) {
	var resp *http.Response
	var err error
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.Do(req)
		if retry == nil {
			return false, err
		}
		again, retryErr := retry(resp, err)
		if retryErr == nil && resp != nil && resp.StatusCode >= 400 && !again {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return false, fmt.Errorf("139: http %d: %s", resp.StatusCode, string(body))
		}
		return again, retryErr
	})
	return resp, err
}

func parsePersonalTime(s string) time.Time {
	t, err := time.Parse("2006-01-02T15:04:05.999-07:00", s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func randomString(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	buf := make([]byte, n)
	if _, err := cryptoRand.Read(buf); err == nil {
		for i := range buf {
			buf[i] = alphabet[int(buf[i])%len(alphabet)]
		}
		return string(buf)
	}
	seed := uint64(time.Now().UnixNano())
	for i := range buf {
		seed = seed*1664525 + 1013904223
		buf[i] = alphabet[int(seed%uint64(len(alphabet)))]
	}
	return string(buf)
}

func uploadPartSize(size int64, custom int64) int64 {
	if custom > 0 {
		return custom
	}
	if size > 30*int64(fs.Gibi) {
		return int64(largePartSize)
	}
	return int64(defaultPartSize)
}

func partCount(size, partSize int64) int {
	if size <= 0 {
		return 1
	}
	n := size / partSize
	if size%partSize != 0 {
		n++
	}
	if n < 1 {
		n = 1
	}
	return int(n)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func tempFileWithSHA256(in io.Reader) (*os.File, int64, string, error) {
	file, err := os.CreateTemp("", "rclone-139-*")
	if err != nil {
		return nil, 0, "", err
	}
	hasher := sha256.New()
	n, err := io.Copy(io.MultiWriter(file, hasher), in)
	if err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, 0, "", err
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, 0, "", err
	}
	return file, n, hex.EncodeToString(hasher.Sum(nil)), nil
}

func (o *Object) prepareUploadSource(ctx context.Context, in io.Reader, src fs.ObjectInfo) (string, io.Reader, func(), error) {
	if srcObj, ok := src.(fs.Object); ok {
		hashReader, err := srcObj.Open(ctx)
		if err == nil {
			hasher := sha256.New()
			if _, err = io.Copy(hasher, hashReader); err != nil {
				_ = hashReader.Close()
				return "", nil, nil, err
			}
			if err = hashReader.Close(); err != nil {
				return "", nil, nil, err
			}
			uploadReader, err := srcObj.Open(ctx)
			if err == nil {
				return hex.EncodeToString(hasher.Sum(nil)), uploadReader, func() { _ = uploadReader.Close() }, nil
			}
		}
	}

	tmp, _, sha256hex, err := tempFileWithSHA256(in)
	if err != nil {
		return "", nil, nil, err
	}
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}
	return sha256hex, tmp, cleanup, nil
}

func (o *Object) upload(ctx context.Context, in io.Reader, wrap accounting.WrapFn, src fs.ObjectInfo, leaf, parentID string) error {
	size := src.Size()
	sha256hex, uploadReader, cleanup, err := o.prepareUploadSource(ctx, in, src)
	if err != nil {
		return err
	}
	defer cleanup()

	partSize := uploadPartSize(size, o.fs.opt.CustomUploadPartSize)
	totalParts := partCount(size, partSize)
	allParts := make([]uploadPart, 0, totalParts)
	for i := 0; i < totalParts; i++ {
		start := int64(i) * partSize
		p := uploadPart{PartNumber: i + 1, PartSize: minInt64(size-start, partSize)}
		p.ParallelHashCtx.PartOffset = start
		allParts = append(allParts, p)
	}

	firstBatch := allParts[:minInt(100, len(allParts))]
	var create personalUploadResp
	_, err = o.fs.personalCall(ctx, http.MethodPost, "/file/create", map[string]any{
		"contentHash":          sha256hex,
		"contentHashAlgorithm": "SHA256",
		"contentType":          "application/octet-stream",
		"parallelUpload":       false,
		"partInfos":            firstBatch,
		"size":                 size,
		"parentFileId":         parentID,
		"name":                 o.fs.opt.Enc.FromStandardName(leaf),
		"type":                 "file",
		"fileRenameMode":       "auto_rename",
	}, &create)
	if err != nil {
		return err
	}
	if create.Data.Exist {
		return nil
	}
	if len(create.Data.PartInfos) > 0 {
		if err = uploadPartBatch(ctx, uploadReader, wrap, allParts, create.Data.PartInfos); err != nil {
			return err
		}
		for i := 100; i < len(allParts); i += 100 {
			batch := allParts[i:minInt(i+100, len(allParts))]
			var more personalUploadURLResp
			_, err = o.fs.personalCall(ctx, http.MethodPost, "/file/getUploadUrl", map[string]any{
				"fileId":    create.Data.FileID,
				"uploadId":  create.Data.UploadID,
				"partInfos": batch,
			}, &more)
			if err != nil {
				return err
			}
			if err = uploadPartBatch(ctx, uploadReader, wrap, allParts, more.Data.PartInfos); err != nil {
				return err
			}
		}
		_, err = o.fs.personalCall(ctx, http.MethodPost, "/file/complete", map[string]any{
			"contentHash":          sha256hex,
			"contentHashAlgorithm": "SHA256",
			"fileId":               create.Data.FileID,
			"uploadId":             create.Data.UploadID,
		}, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func uploadPartBatch(ctx context.Context, in io.Reader, wrap accounting.WrapFn, allParts []uploadPart, uploadInfos []personalPartInfo) error {
	currentPart := 0
	if len(uploadInfos) > 0 {
		currentPart = uploadInfos[0].PartNumber - 1
	}
	for _, info := range uploadInfos {
		idx := info.PartNumber - 1
		if idx < 0 || idx >= len(allParts) {
			return fmt.Errorf("139: invalid part number %d", info.PartNumber)
		}
		if idx != currentPart {
			return fmt.Errorf("139: non-sequential part upload order: got %d expected %d", idx+1, currentPart+1)
		}
		part := allParts[idx]
		section := io.LimitReader(in, part.PartSize)
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, info.UploadURL, io.NopCloser(wrap(section)))
		if err != nil {
			return err
		}
		req.ContentLength = part.PartSize
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Origin", "https://yun.139.com")
		req.Header.Set("Referer", "https://yun.139.com/")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("139: upload part %d failed: %s", info.PartNumber, string(body))
		}
		currentPart++
	}
	return nil
}

func (o *Object) Fs() fs.Info     { return o.fs }
func (o *Object) String() string  { return o.remote }
func (o *Object) Remote() string  { return o.remote }
func (o *Object) Size() int64     { return o.size }
func (o *Object) Storable() bool  { return true }
func (o *Object) ID() string      { return o.id }

func (o *Object) ModTime(ctx context.Context) time.Time {
	if err := o.readMetaData(ctx); err != nil {
		fs.Debugf(o, "Failed to read metadata for modtime: %v", err)
	}
	return o.modTime
}

func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}

func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	if err := o.readMetaData(ctx); err != nil {
		return nil, err
	}
	url, err := o.fs.openURL(ctx, o.id)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	headers := map[string]string{}
	fs.OpenOptionAddHeaders(options, headers)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := o.fs.do(req, shouldRetry)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("139: download failed: http %d: %s", resp.StatusCode, string(body))
	}
	return resp.Body, nil
}

func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	if err := o.Remove(ctx); err != nil {
		return err
	}
	newObj, err := o.fs.putUnchecked(ctx, in, src, src.Remote(), options...)
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
	_, err := o.fs.personalCall(ctx, http.MethodPost, "/recyclebin/batchTrash", map[string]any{
		"fileIds": []string{o.id},
	}, nil)
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
	o.modTime = parsePersonalTime(info.UpdatedAt)
	o.createdTime = parsePersonalTime(info.CreatedAt)
	o.isDir = info.Type == "folder"
	o.hasMetaData = true
	if o.isDir {
		return fs.ErrorObjectNotFound
	}
	return nil
}

func sha1Hash(data string) string {
	sum := sha1.Sum([]byte(data))
	return hex.EncodeToString(sum[:])
}

func pkcs7Pad(data []byte, blockSize int) []byte {
	padding := blockSize - len(data)%blockSize
	return append(data, bytesRepeat(byte(padding), padding)...)
}

func pkcs7Unpad(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("pkcs7: data is empty")
	}
	unpadding := int(data[len(data)-1])
	if unpadding == 0 || unpadding > len(data) {
		return nil, errors.New("pkcs7: invalid padding")
	}
	return data[:len(data)-unpadding], nil
}

func aesCbcEncrypt(plaintext, key, iv []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(iv) != block.BlockSize() {
		return nil, fmt.Errorf("invalid iv length %d", len(iv))
	}
	padded := pkcs7Pad(plaintext, block.BlockSize())
	ciphertext := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(ciphertext, padded)
	return ciphertext, nil
}

func aesCbcDecrypt(ciphertext, key, iv []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(iv) != block.BlockSize() {
		return nil, fmt.Errorf("invalid iv length %d", len(iv))
	}
	if len(ciphertext)%block.BlockSize() != 0 {
		return nil, errors.New("ciphertext is not a multiple of the block size")
	}
	plaintext := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(plaintext, ciphertext)
	return pkcs7Unpad(plaintext)
}

func aesECBDecrypt(ciphertext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(ciphertext)%block.BlockSize() != 0 {
		return nil, errors.New("ciphertext is not a multiple of the block size")
	}
	plaintext := make([]byte, len(ciphertext))
	for bs, be := 0, block.BlockSize(); bs < len(ciphertext); bs, be = bs+block.BlockSize(), be+block.BlockSize() {
		block.Decrypt(plaintext[bs:be], ciphertext[bs:be])
	}
	return pkcs7Unpad(plaintext)
}

func bytesRepeat(b byte, n int) []byte {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = b
	}
	return buf
}

func sortedJSONString(v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "null", nil
	case string:
		var parsed any
		if err := json.Unmarshal([]byte(x), &parsed); err == nil {
			return sortedJSONString(parsed)
		}
		out, err := json.Marshal(x)
		return string(out), err
	case bool, float64, int, int64, uint64:
		out, err := json.Marshal(x)
		return string(out), err
	case []any:
		items := make([]string, 0, len(x))
		for _, item := range x {
			s, err := sortedJSONString(item)
			if err != nil {
				return "", err
			}
			items = append(items, s)
		}
		return "[" + strings.Join(items, ",") + "]", nil
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		for i := 1; i < len(keys); i++ {
			for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
				keys[j], keys[j-1] = keys[j-1], keys[j]
			}
		}
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			ks, err := json.Marshal(k)
			if err != nil {
				return "", err
			}
			vs, err := sortedJSONString(x[k])
			if err != nil {
				return "", err
			}
			parts = append(parts, string(ks)+":"+vs)
		}
		return "{" + strings.Join(parts, ",") + "}", nil
	default:
		out, err := json.Marshal(x)
		return string(out), err
	}
}

func yun139EncryptedRequest(ctx context.Context, endpoint string, body any, headers map[string]string, aesKeyHex string) ([]byte, error) {
	aesKey, err := hex.DecodeString(aesKeyHex)
	if err != nil {
		return nil, err
	}
	sorted, err := sortedJSONString(body)
	if err != nil {
		return nil, err
	}
	iv := make([]byte, 16)
	if _, err = cryptoRand.Read(iv); err != nil {
		return nil, err
	}
	encrypted, err := aesCbcEncrypt([]byte(sorted), aesKey, iv)
	if err != nil {
		return nil, err
	}
	payload := base64.StdEncoding.EncodeToString(append(iv, encrypted...))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(payload))
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("User-Agent", "okhttp/4.11.0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(raw))
	}
	if len(raw) > 0 && raw[0] == '{' {
		return raw, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(string(raw))
	if err != nil {
		return nil, err
	}
	if len(decoded) < 16 {
		return nil, errors.New("encrypted response too short")
	}
	return aesCbcDecrypt(decoded[16:], aesKey, decoded[:16])
}

func (f *Fs) loginWithPassword(ctx context.Context) (string, error) {
	if f.opt.Username == "" || f.opt.Password == "" || f.opt.MailCookies == "" {
		return "", errors.New("139: username, password and mail_cookies are required for password login")
	}
	sid, err := f.step1PasswordLogin(ctx)
	if err != nil {
		return "", err
	}
	dycpwd, err := f.step2GetSingleToken(ctx, sid)
	if err != nil {
		return "", err
	}
	return f.step3ThirdPartyLogin(ctx, dycpwd)
}

func (f *Fs) step1PasswordLogin(ctx context.Context) (string, error) {
	loginURL := "https://mail.10086.cn/Login/Login.ashx"
	cguid := strconv.FormatInt(time.Now().UnixMilli(), 10)
	form := url.Values{}
	form.Set("UserName", f.opt.Username)
	form.Set("passOld", "")
	form.Set("auto", "on")
	form.Set("Password", sha1Hash("fetion.com.cn:"+f.opt.Password))
	form.Set("webIndexPagePwdLogin", "1")
	form.Set("pwdType", "1")
	form.Set("clientId", "1003")
	form.Set("authType", "2")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, loginURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Origin", "https://mail.10086.cn")
	req.Header.Set("Referer", fmt.Sprintf("https://mail.10086.cn/default.html?&s=1&v=0&u=%s&m=1&ec=S001&resource=indexLogin&clientid=1003&auto=on&cguid=%s&mtime=45", base64.StdEncoding.EncodeToString([]byte(f.opt.Username)), cguid))
	req.Header.Set("User-Agent", defaultUA)
	req.Header.Set("Cookie", f.opt.MailCookies)

	noRedirect := *f.srv
	noRedirect.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	resp, err := noRedirect.Do(req)
	if err != nil && !errors.Is(err, http.ErrUseLastResponse) {
		return "", err
	}
	if resp == nil {
		return "", errors.New("139: empty response from password login")
	}
	defer resp.Body.Close()
	location := resp.Header.Get("Location")
	sid := extractParam(location, "sid")
	if sid == "" {
		for _, cookie := range resp.Header.Values("Set-Cookie") {
			if v := extractCookie(cookie, "Os_SSo_Sid"); v != "" {
				sid = v
				break
			}
		}
	}
	if sid == "" {
		return "", errors.New("139: failed to extract sid from password login")
	}
	return sid, nil
}

func (f *Fs) step2GetSingleToken(ctx context.Context, sid string) (string, error) {
	cguid := strconv.FormatInt(time.Now().UnixMilli(), 10)
	endpoint := fmt.Sprintf("https://smsrebuild1.mail.10086.cn/setting/s?func=%s&sid=%s&cguid=%s", url.QueryEscape("umc:getArtifact"), sid, cguid)
	rmkey := extractCookie(f.opt.MailCookies, "RMKEY")
	if rmkey == "" {
		return "", errors.New("139: RMKEY not found in mail_cookies")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Host", "smsrebuild1.mail.10086.cn")
	req.Header.Set("Cookie", "RMKEY="+rmkey)
	req.Header.Set("Content-Type", "text/xml; charset=utf-8")
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("User-Agent", "okhttp/4.12.0")
	resp, err := f.do(req, shouldRetry)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var out struct {
		Var struct {
			Artifact string `json:"artifact"`
		} `json:"var"`
	}
	if err = json.Unmarshal(body, &out); err != nil {
		return "", err
	}
	if out.Var.Artifact == "" {
		return "", errors.New("139: failed to extract dycpwd from artifact response")
	}
	return out.Var.Artifact, nil
}

func (f *Fs) step3ThirdPartyLogin(ctx context.Context, dycpwd string) (string, error) {
	endpoint := "https://user-njs.yun.139.com/user/thirdlogin"
	body := map[string]any{
		"clientkey_decrypt": "l3TryM&Q+X7@dzwk)qP",
		"clienttype":        "886",
		"cpid":              "507",
		"dycpwd":            dycpwd,
		"extInfo":           map[string]any{"ifOpenAccount": "0"},
		"loginMode":         "0",
		"msisdn":            f.opt.Username,
		"pintype":           "13",
		"secinfo":           strings.ToUpper(sha1Hash("fetion.com.cn:" + dycpwd)),
		"version":           "20250901",
	}
	plain, err := yun139EncryptedRequest(ctx, endpoint, body, map[string]string{
		"hcy-cool-flag":       "1",
		"x-huawei-channelSrc": "10246600",
		"x-sdk-channelSrc":    "",
		"x-MM-Source":         "0",
		"x-UserAgent":         "android|23116PN5BC|android15|1.2.6|||1440x3200|10246600",
		"x-DeviceInfo":        "4|127.0.0.1|5|1.2.6|Xiaomi|23116PN5BC||02-00-00-00-00-00|android 15|1440x3200|android|||",
		"Content-Type":        "text/plain;charset=UTF-8",
		"Host":                "user-njs.yun.139.com",
		"Connection":          "Keep-Alive",
		"Accept-Encoding":     "gzip",
	}, keyHex1)
	if err != nil {
		return "", err
	}
	var layer1 struct {
		Data string `json:"data"`
	}
	if err = json.Unmarshal(plain, &layer1); err != nil {
		return "", err
	}
	if layer1.Data == "" {
		return "", errors.New("139: missing second-layer encrypted payload")
	}
	key2, err := hex.DecodeString(keyHex2)
	if err != nil {
		return "", err
	}
	inner, err := hex.DecodeString(layer1.Data)
	if err != nil {
		return "", err
	}
	finalJSON, err := aesECBDecrypt(inner, key2)
	if err != nil {
		return "", err
	}
	var out struct {
		AuthToken    string `json:"authToken"`
		Account      string `json:"account"`
		UserDomainID string `json:"userDomainId"`
	}
	if err = json.Unmarshal(finalJSON, &out); err != nil {
		return "", err
	}
	if out.AuthToken == "" || out.Account == "" {
		return "", errors.New("139: third-party login did not return auth token")
	}
	if out.UserDomainID != "" {
		f.opt.UserDomainID = out.UserDomainID
		_ = config.SetValueAndSave(f.originalName, "user_domain_id", out.UserDomainID)
	}
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("pc:%s:%s", out.Account, out.AuthToken))), nil
}

func extractParam(location, key string) string {
	if location == "" {
		return ""
	}
	re := regexp.MustCompile(key + `=([^&]+)`)
	match := re.FindStringSubmatch(location)
	if len(match) > 1 {
		return match[1]
	}
	return ""
}

func extractCookie(raw, key string) string {
	for _, part := range strings.Split(raw, ";") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, key+"=") {
			return strings.TrimPrefix(part, key+"=")
		}
	}
	return ""
}

var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Purger          = (*Fs)(nil)
	_ fs.PutUncheckeder  = (*Fs)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.Copier          = (*Fs)(nil)
	_ fs.Shutdowner      = (*Fs)(nil)
	_ fs.DirCacheFlusher = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.IDer            = (*Object)(nil)
)
