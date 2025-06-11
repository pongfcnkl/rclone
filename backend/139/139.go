package _139

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/random"
	"github.com/rclone/rclone/lib/rest"
	"github.com/rclone/rclone/lib/pacer"
)

const (
	// 云盘类型常量
	MetaPersonal    = "personal"     // 个人云
	MetaFamily      = "family"       // 家庭云
	MetaGroup       = "group"        // 群组云
	MetaPersonalNew = "personal_new" // 新版个人云

	// API相关常量
	apiURL = "https://yun.139.com"
	personalAPIURL = "https://personal-kd-njs.yun.139.com"

	// Pacer相关常量
	minSleep        = 100 * time.Millisecond
	maxSleep        = 2 * time.Second
	decayConstant   = 2 // bigger for slower decay, exponential
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "139",
		Description: "139 Cloud Storage",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "type",
			Help:     "Type of 139 cloud storage (personal/family/group/personal_new)",
			Required: true,
			Examples: []fs.OptionExample{{
				Value: "personal",
				Help:  "Personal cloud storage",
			}, {
				Value: "family",
				Help:  "Family cloud storage",
			}, {
				Value: "group",
				Help:  "Group cloud storage",
			}, {
				Value: "personal_new",
				Help:  "New personal cloud storage",
			}},
		}, {
			Name:     "authorization",
			Help:     "Authorization token",
			Required: true,
			IsPassword: true,
		}, {
			Name:     "root_folder_id",
			Help:     "Root folder ID",
			Required: false,
		}, {
			Name:     "cloud_id",
			Help:     "Cloud ID (required for family/group)",
			Required: false,
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	Type          string `config:"type"`
	Authorization string `config:"authorization"`
	RootFolderID  string `config:"root_folder_id"`
	CloudID       string `config:"cloud_id"`
}

// Fs represents a remote 139 cloud storage
type Fs struct {
	name     string
	root     string
	opt      *Options
	features *fs.Features
	srv      *rest.Client
	account  string
	pacer    *fs.Pacer
}

// Object describes a 139 cloud storage object
type Object struct {
	fs          *Fs
	remote      string
	id          string
	modTime     time.Time
	size        int64
	hash        string
	mimeType    string
	isDirectory bool
}

// BaseResp represents the base response from 139 cloud API
type BaseResp struct {
	Success bool   `json:"success"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// FileItem represents a file or directory item
type FileItem struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Size        int64     `json:"size"`
	Type        string    `json:"type"`
	CreatedAt   string    `json:"createdAt"`
	UpdatedAt   string    `json:"updatedAt"`
	IsDirectory bool      `json:"isDirectory"`
	Hash        string    `json:"hash"`
	MimeType    string    `json:"mimeType"`
	Path        string    `json:"path"`
}

// ListResp represents the response of list directory
type ListResp struct {
	BaseResp
	Data struct {
		Items          []FileItem `json:"items"`
		NextPageCursor string     `json:"nextPageCursor"`
	} `json:"data"`
}

// RefreshTokenResp represents the response of token refresh
type RefreshTokenResp struct {
	Return      string `xml:"return"`
	Token       string `xml:"token"`
	Expiretime  int32  `xml:"expiretime"`
	AccessToken string `xml:"accessToken"`
	Desc        string `xml:"desc"`
}

// encodeURIComponent encodes a string for use in URI
func encodeURIComponent(str string) string {
	r := url.QueryEscape(str)
	r = strings.Replace(r, "+", "%20", -1)
	r = strings.Replace(r, "%21", "!", -1)
	r = strings.Replace(r, "%27", "'", -1)
	r = strings.Replace(r, "%28", "(", -1)
	r = strings.Replace(r, "%29", ")", -1)
	r = strings.Replace(r, "%2A", "*", -1)
	return r
}

// getMD5EncodeStr returns MD5 hash of string
func getMD5EncodeStr(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}

// calSign calculates the signature for API request
func calSign(body, ts, randStr string) string {
	body = encodeURIComponent(body)
	strs := strings.Split(body, "")
	sort.Strings(strs)
	body = strings.Join(strs, "")
	body = base64.StdEncoding.EncodeToString([]byte(body))
	res := getMD5EncodeStr(body) + getMD5EncodeStr(ts+":"+randStr)
	res = strings.ToUpper(getMD5EncodeStr(res))
	return res
}

// refreshToken refreshes the authorization token
func (f *Fs) refreshToken(ctx context.Context) error {
	decode, err := base64.StdEncoding.DecodeString(f.opt.Authorization)
	if err != nil {
		return fmt.Errorf("authorization decode failed: %v", err)
	}

	decodeStr := string(decode)
	splits := strings.Split(decodeStr, ":")
	if len(splits) < 3 {
		return fmt.Errorf("authorization is invalid, splits < 3")
	}

	strs := strings.Split(splits[2], "|")
	if len(strs) < 4 {
		return fmt.Errorf("authorization is invalid, strs < 4")
	}

	expiration, err := strconv.ParseInt(strs[3], 10, 64)
	if err != nil {
		return fmt.Errorf("authorization is invalid")
	}

	expiration -= time.Now().UnixMilli()
	if expiration > 1000*60*60*24*15 {
		// Token有效期大于15天无需刷新
		return nil
	}
	if expiration < 0 {
		return fmt.Errorf("authorization has expired")
	}

	url := "https://aas.caiyun.feixin.10086.cn:443/tellin/authTokenRefresh.do"
	var resp RefreshTokenResp
	reqBody := fmt.Sprintf("<root><token>%s</token><account>%s</account><clienttype>656</clienttype></root>", splits[2], splits[1])
	
	opts := rest.Opts{
		Method: "POST",
		Path:   url,
		Body:   strings.NewReader(reqBody),
		ExtraHeaders: map[string]string{
			"Content-Type": "application/xml",
		},
	}

	err = f.pacer.Call(func() (bool, error) {
		_, err := f.srv.CallXML(ctx, &opts, nil, &resp)
		if err != nil {
			retry, err := shouldRetry(err)
			return retry, err
		}
		return false, nil
	})

	if err != nil {
		return err
	}

	if resp.Return != "0" {
		return fmt.Errorf("failed to refresh token: %s", resp.Desc)
	}

	f.opt.Authorization = base64.StdEncoding.EncodeToString([]byte(splits[0] + ":" + splits[1] + ":" + resp.Token))
	return nil
}

// request makes an API request to 139 cloud
func (f *Fs) request(ctx context.Context, opts *rest.Opts) ([]byte, error) {
	// Add common headers
	randStr := random.String(16)
	ts := time.Now().Format("2006-01-02 15:04:05")
	
	body, err := json.Marshal(opts.Body)
	if err != nil {
		return nil, err
	}
	
	sign := calSign(string(body), ts, randStr)
	svcType := "1"
	if f.opt.Type == MetaFamily {
		svcType = "2"
	}

	opts.ExtraHeaders = map[string]string{
		"Accept":               "application/json, text/plain, */*",
		"Authorization":        "Basic " + f.opt.Authorization,
		"CMS-DEVICE":          "default",
		"mcloud-channel":      "1000101",
		"mcloud-client":       "10701",
		"mcloud-sign":         fmt.Sprintf("%s,%s,%s", ts, randStr, sign),
		"mcloud-version":      "7.14.0",
		"Origin":              "https://yun.139.com",
		"Referer":             "https://yun.139.com/w/",
		"x-DeviceInfo":        "||9|7.14.0|chrome|120.0.0.0|||windows 10||zh-CN|||",
		"x-huawei-channelSrc": "10000034",
		"x-inner-ntwk":        "2",
		"x-m4c-caller":        "PC",
		"x-m4c-src":           "10002",
		"x-SvcType":           svcType,
		"Inner-Hcy-Router-Https": "1",
	}

	var result []byte
	var resp BaseResp
	err = f.pacer.Call(func() (bool, error) {
		_, err := f.srv.CallJSON(ctx, opts, &resp, &result)
		if err != nil {
			retry, err := shouldRetry(err)
			return retry, err
		}
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("API error: %s", resp.Message)
	}

	return result, nil
}

// shouldRetry determines if the error should be retried
func shouldRetry(err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	
	// Check if it's a temporary error
	if fserrors.ShouldRetry(err) {
		return true, err
	}

	// Check for specific 139 cloud errors
	if strings.Contains(err.Error(), "token expired") {
		return true, err
	}

	return false, err
}

// listDir lists the directory contents
func (f *Fs) listDir(ctx context.Context, dirID string) ([]FileItem, error) {
	var items []FileItem
	nextPageCursor := ""

	for {
		body := map[string]interface{}{
			"imageThumbnailStyleList": []string{"Small", "Large"},
			"orderBy":                 "updated_at",
			"orderDirection":          "DESC",
			"pageInfo": map[string]interface{}{
				"pageCursor": nextPageCursor,
				"pageSize":   100,
			},
			"parentFileId": dirID,
		}

		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}

		opts := rest.Opts{
			Method: "POST",
			Path:   "/hcy/file/list",
			Body:   bytes.NewReader(bodyBytes),
		}

		var resp ListResp
		result, err := f.request(ctx, &opts)
		if err != nil {
			return nil, err
		}

		err = json.Unmarshal(result, &resp)
		if err != nil {
			return nil, err
		}

		items = append(items, resp.Data.Items...)
		nextPageCursor = resp.Data.NextPageCursor

		if nextPageCursor == "" {
			break
		}
	}

	return items, nil
}

// List implements fs.Fs
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	// Get directory ID from path
	dirID := f.opt.RootFolderID
	if dir != "" {
		// TODO: Implement path to ID mapping
		return nil, fmt.Errorf("path mapping not implemented yet")
	}

	// List directory contents
	items, err := f.listDir(ctx, dirID)
	if err != nil {
		return nil, err
	}

	// Convert to fs.DirEntries
	entries = make(fs.DirEntries, 0, len(items))
	for _, item := range items {
		remote := path.Join(dir, item.Name)
		if item.IsDirectory {
			entries = append(entries, fs.NewDir(remote, time.Now()))
		} else {
			modTime, _ := time.Parse(time.RFC3339, item.UpdatedAt)
			entries = append(entries, &Object{
				fs:       f,
				remote:   remote,
				id:       item.ID,
				modTime:  modTime,
				size:     item.Size,
				hash:     item.Hash,
				mimeType: item.MimeType,
			})
		}
	}

	return entries, nil
}

// NewObject implements fs.Fs
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	// Get parent directory and filename
	parentDir := path.Dir(remote)
	if parentDir == "." {
		parentDir = ""
	}
	fileName := path.Base(remote)

	// List parent directory
	entries, err := f.List(ctx, parentDir)
	if err != nil {
		return nil, err
	}

	// Find the object
	for _, entry := range entries {
		if entry.Remote() == remote {
			if obj, ok := entry.(fs.Object); ok {
				return obj, nil
			}
			return nil, fs.ErrorIsDir
		}
	}

	return nil, fs.ErrorObjectNotFound
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	// Create new Fs
	f := &Fs{
		name: name,
		root: root,
		opt:  opt,
	}

	// Create REST client
	f.srv = rest.NewClient(nil)
	f.srv.SetRoot(apiURL)

	// Create pacer
	f.pacer = fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant)))

	// Set up features
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		ReadMimeType:            true,
		WriteMimeType:           true,
	}).Fill(ctx, f)

	// Initialize the Fs
	err = f.initialize()
	if err != nil {
		return nil, err
	}

	return f, nil
}

// initialize initializes the Fs
func (f *Fs) initialize() error {
	// Decode authorization
	decode, err := base64.StdEncoding.DecodeString(f.opt.Authorization)
	if err != nil {
		return fmt.Errorf("authorization decode failed: %v", err)
	}

	decodeStr := string(decode)
	splits := strings.Split(decodeStr, ":")
	if len(splits) < 2 {
		return fmt.Errorf("authorization is invalid, splits < 2")
	}
	f.account = splits[1]

	// Set root folder ID based on type
	switch f.opt.Type {
	case MetaPersonalNew:
		if f.opt.RootFolderID == "" {
			f.opt.RootFolderID = "/"
		}
	case MetaPersonal:
		if f.opt.RootFolderID == "" {
			f.opt.RootFolderID = "root"
		}
	case MetaGroup:
		if f.opt.RootFolderID == "" {
			f.opt.RootFolderID = f.opt.CloudID
		}
	case MetaFamily:
		// No default root folder ID for family cloud
	default:
		return fmt.Errorf("unsupported cloud type: %s", f.opt.Type)
	}

	return nil
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String returns a description of the Fs
func (f *Fs) String() string {
	return fmt.Sprintf("139 cloud storage %s", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Put in to the remote path with the modTime given of the given size
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// TODO: Implement upload
	return nil, nil
}

// Mkdir makes the directory (container, bucket)
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	// TODO: Implement mkdir
	return nil
}

// Rmdir removes the directory (container, bucket) if empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	// TODO: Implement rmdir
	return nil
}

// Precision of the ModTimes in this Fs
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Hashes returns the supported hash sets
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.MD5)
}

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// ModTime returns the modification time of the object
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// Size returns the size of the object
func (o *Object) Size() int64 {
	return o.size
}

// Fs returns the hash of an object
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t != hash.MD5 {
		return "", hash.ErrUnsupported
	}
	return o.hash, nil
}

// Open opens the file for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	// TODO: Implement download
	return nil, nil
}

// Update in to the object with the modTime given of the given size
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	// TODO: Implement update
	return nil
}

// Remove this object
func (o *Object) Remove(ctx context.Context) error {
	// TODO: Implement remove
	return nil
}

// MimeType of an Object if known, "" otherwise
func (o *Object) MimeType(ctx context.Context) string {
	return o.mimeType
}

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string {
	return o.id
}

// SetModTime implements fs.Object
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	// TODO: Implement set mod time
	return fs.ErrorCantSetModTime
}

// String returns a description of the Object
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Storable returns whether this object is storable
func (o *Object) Storable() bool {
	return true
}

// Check the interfaces are satisfied
var (
	_ fs.Fs        = (*Fs)(nil)
	_ fs.Object    = (*Object)(nil)
	_ fs.MimeTyper = (*Object)(nil)
	_ fs.IDer      = (*Object)(nil)
	_ fs.DirEntry  = (*Object)(nil)
)
