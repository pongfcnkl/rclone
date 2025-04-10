// Package alist implements an rclone backend for AList
package alist

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

const (
	minSleep      = 10 * time.Millisecond
	maxSleep      = 2 * time.Second
	decayConstant = 2 // bigger for slower decay, exponential

	// API endpoint constants.
	apiLogin  = "/api/auth/login/hash"
	apiList   = "/api/fs/list"
	apiPut    = "/api/fs/put"
	apiMkdir  = "/api/fs/mkdir"
	apiRemove = "/api/fs/remove"
	apiGet    = "/api/fs/get"
	apiMe     = "/api/me"
	apiCopy   = "/api/fs/copy"
	apiTaskInfo = "/api/task/copy/undone"  // 将 apiTaskInfo 改为未完成任务接口
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "alist",
		Description: "AList",
		NewFs:       NewFs,
		Options: []fs.Option{
			{
				Name:     "url",
				Help:     "URL of the AList server",
				Required: true,
			},
			{
				Name:     "username",
				Help:     "Username for AList",
				Required: false,
			},
			{
				Name:       "password",
				Help:       "Password for AList",
				Required:   false,
				IsPassword: true,
			},
			{
				Name:     "root_path",
				Help:     "Root path within the AList server",
				Required: false,
				Default:  "/",
			},
			{
				Name:     "cf_server",
				Help:     "URL of the Cloudflare server",
				Required: false,
				Default:  "",
			},
			{
				Name:     "otp_code",
				Help:     "Two-factor authentication code",
				Default:  "",
				Advanced: true,
			},
			{
				Name:     "meta_pass",
				Help:     "Meta password for listing",
				Default:  "",
				Advanced: true,
			},
			{
				Name:     config.ConfigEncoding,
				Help:     config.ConfigEncodingHelp,
				Advanced: true,
				Default: (encoder.EncodeLtGt |
					encoder.EncodeLeftSpace |
					encoder.EncodeCtl |
					encoder.EncodeSlash |
					encoder.EncodeRightSpace |
					encoder.EncodeInvalidUtf8),
			},
			{
				Name:     "user_agent",
				Help:     "Custom User-Agent string to use (overridden by Cloudflare if cf_server is set)",
				Advanced: true,
				Default:  "",
			},
		},
	})
}

// Options defines the configuration for this backend.
type Options struct {
	URL       string `config:"url"`
	Username  string `config:"username"`
	Password  string `config:"password"`
	OTPCode   string `config:"otp_code"`
	MetaPass  string `config:"meta_pass"`
	RootPath  string `config:"root_path"`
	CfServer  string `config:"cf_server"`
	UserAgent string `config:"user_agent"` // 无需逗号
}

// Fs represents a remote AList server.
type Fs struct {
	name            string
	root            string
	opt             Options
	features        *fs.Features
	token           string
	tokenMu         sync.Mutex
	srv             *rest.Client
	pacer           *fs.Pacer
	fileListCacheMu sync.Mutex
	fileListCache   map[string]listResponse

	userPermission int
	// cfCookies and cfCookieExpiry store Cloudflare cookies per host.
	cfCookies      map[string]*http.Cookie
	cfCookieExpiry map[string]time.Time
	cfUserAgent    string
	cfMu           sync.Mutex

	// The underlying HTTP client used to build rest.Client.
	httpClient *http.Client
}

// API response structures.
type loginResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Token string `json:"token"`
	} `json:"data"`
}

type meResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Permission int `json:"permission"`
	} `json:"data"`
}

type fileInfo struct {
	Name     string    `json:"name"`
	Size     int64     `json:"size"`
	IsDir    bool      `json:"is_dir"`
	Modified time.Time `json:"modified"`
	HashInfo *struct {
		MD5    string `json:"md5,omitempty"`
		SHA1   string `json:"sha1,omitempty"`
		SHA256 string `json:"sha256,omitempty"`
	} `json:"hash_info"`
	RawURL string `json:"raw_url"`
}

type listResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Content []fileInfo `json:"content"`
		Total   int        `json:"total"`
	} `json:"data"`
}

type requestResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type copyTask struct {
	ID        string  `json:"id"`
	State     string  `json:"state"` 
	Progress  float64 `json:"progress"`
	Error     string  `json:"error"`
}

// Object describes an AList object.
type Object struct {
	fs        *Fs
	remote    string
	size      int64
	modTime   time.Time
	md5sum    string
	sha1sum   string
	sha256sum string
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// Features returns the Fs features
func (f *Fs) Features() *fs.Features {
	return f.features
}

func (o *Object) Fs() fs.Info {
	return o.fs
}

// newClientWithPacer creates an HTTP client using fs.AddConfig to override the
// User-Agent from Options.
func newClientWithPacer(ctx context.Context, opt *Options) *http.Client {
	newCtx, ci := fs.AddConfig(ctx)
	ci.UserAgent = opt.UserAgent
	return fshttp.NewClient(newCtx)
}

// NewFs constructs an Fs from the path, container:path.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}

	// Ensure URL does not end with '/'
	opt.URL = strings.TrimSuffix(opt.URL, "/")
	// Ensure root starts with '/'
	if !strings.HasPrefix(root, "/") {
		root = "/" + root
	}
	// Incorporate root_path if provided.
	if opt.RootPath != "" && opt.RootPath != "/" {
		root = path.Join(root, opt.RootPath)
	}

	f := &Fs{
		name:           name,
		root:           root,
		opt:            *opt,
		fileListCache:  make(map[string]listResponse),
		cfCookies:      make(map[string]*http.Cookie),
		cfCookieExpiry: make(map[string]time.Time),
	}

	// --- Early UA setting ---
	// If a CF server is configured, attempt to update the user agent early
	// by calling the /get-ua endpoint. If that fails, log a warning and proceed.
	if f.opt.CfServer != "" {
		if err := f.fetchUserAgent(ctx); err != nil {
			fs.Infof(ctx, "Warning: failed to fetch CF user agent: %v", err)
		} else {
			fs.Infof(ctx, "Using CF user agent: %s", f.cfUserAgent)
		}
	}

	client := newClientWithPacer(ctx, opt)
	f.httpClient = client
	f.srv = rest.NewClient(client).SetRoot(opt.URL)
	f.pacer = fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant)))

	// Login if credentials are provided.
	if f.opt.Username != "" && f.opt.Password != "" {
		if err := f.login(ctx); err != nil {
			return nil, err
		}
	} else {
		f.token = ""
	}

	// Retrieve user permissions.
	var meResp meResponse
	err := f.doCFRequestMust(ctx, "GET", apiMe, nil, &meResp)
	// If that fails and a CF server is set, try to fetch CF cookie and retry.
	if err != nil && f.opt.CfServer != "" {
		if fetchErr := f.fetchCloudflare(ctx, f.opt.URL); fetchErr == nil {
			err = f.doCFRequestMust(ctx, "GET", apiMe, nil, &meResp)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve user permissions: %w", err)
	}
	f.userPermission = meResp.Data.Permission

	// Set supported hash types.
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f)

	// Check if root directory exists, create if not
	err = f.checkRoot(ctx)
	if err != nil {
		return nil, fmt.Errorf("error checking root directory: %w", err) 
	}

	return f, nil
}

// makePasswordHash returns a sha256 hash of the password with a fixed suffix.
func (f *Fs) makePasswordHash(password string) string {
	password += "-https://github.com/alist-org/alist"
	hash := sha256.Sum256([]byte(password))
	return hex.EncodeToString(hash[:])
}

// login performs authentication and stores the token.
func (f *Fs) login(ctx context.Context) error {
	if f.opt.Username == "" || f.opt.Password == "" {
		return nil
	}
	pw, err := obscure.Reveal(f.opt.Password)
	if err != nil {
		return fmt.Errorf("password decode failed - did you obscure it?: %w", err)
	}
	data := map[string]string{
		"username": f.opt.Username,
		"password": f.makePasswordHash(pw),
		"otpcode":  f.opt.OTPCode,
	}
	var loginResp loginResponse
	if err := f.doCFRequestMust(ctx, "POST", apiLogin, data, &loginResp); err != nil {
		return err
	}
	f.token = loginResp.Data.Token
	return nil
}

// ----------------------------------------------------------------------------
// doCFRequest is our single function for all HTTP requests. It:
// - Adds an Authorization token if the request URL host matches the API host.
// - Adds any available Cloudflare cookie for the request host.
// - Chooses the proper client: if the target host equals the API host use f.srv, otherwise use f.httpClient.
// - Checks for Cloudflare challenge responses and, if detected, refreshes the cookie and retries the request.
// ----------------------------------------------------------------------------
func (f *Fs) doCFRequest(req *http.Request) (*http.Response, error) {
	// If the request URL host is the same as our API host, add the token (if available).
	apiBase, err := url.Parse(f.opt.URL)
	if err == nil && req.URL.Host == apiBase.Host && f.token != "" {
		req.Header.Set("Authorization", f.token)
	}

	// Add Cloudflare cookie for this request's host if available.
	if f.opt.CfServer != "" {
		f.cfMu.Lock()
		host := req.URL.Host
		if cookie, ok := f.cfCookies[host]; ok {
			expiry := f.cfCookieExpiry[host]
			if time.Now().After(expiry.Add(-1 * time.Minute)) {
				if err := f.fetchCloudflare(req.Context(), req.URL.String()); err != nil {
					f.cfMu.Unlock()
					return nil, fmt.Errorf("failed to refresh CF cookies: %w", err)
				}
				cookie = f.cfCookies[host]
			}
			req.AddCookie(cookie)
		}
		f.cfMu.Unlock()
	}

	// Choose the appropriate HTTP client.
	apiBase, err = url.Parse(f.opt.URL)
	var clientFunc func(*http.Request) (*http.Response, error)
	if err == nil && req.URL.Host == apiBase.Host {
		clientFunc = f.srv.Do
	} else {
		clientFunc = f.httpClient.Do
	}

	// Perform the request.
	resp, err := clientFunc(req)
	if err != nil {
		return nil, err
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	_ = resp.Body.Close()

	// Check for a Cloudflare challenge page.
	if resp.StatusCode == 403 {
		if f.opt.CfServer != "" {
			f.cfMu.Lock()
			if err := f.fetchCloudflare(req.Context(), req.URL.String()); err != nil {
				f.cfMu.Unlock()
				return nil, fmt.Errorf("failed to refresh CF cookies on 403: %w", err)
			}
			f.cfMu.Unlock()
			// Recreate the request (for GET requests no body is required).
			newReq, err := http.NewRequestWithContext(req.Context(), req.Method, req.URL.String(), nil)
			if err != nil {
				return nil, err
			}
			// Copy headers.
			for k, v := range req.Header {
				newReq.Header[k] = v
			}
			return f.doCFRequest(newReq)
		}
	}

	resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	return resp, nil
}

// doCFRequestMust is a helper that performs an HTTP request via doCFRequest and unmarshals the JSON response into "response".
// It returns an error if the HTTP request or JSON unmarshalling fails or if the response indicates an error.
func (f *Fs) doCFRequestMust(ctx context.Context, method, endpoint string, data interface{}, response interface{}) error {
	var reqBody io.Reader
	if data != nil {
		jsonData, err := json.Marshal(data)
		if err != nil {
			return err
		}
		reqBody = bytes.NewBuffer(jsonData)
	}
	req, err := http.NewRequestWithContext(ctx, method, f.opt.URL+endpoint, reqBody)
	if err != nil {
		return err
	}
	// Set common headers.
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := f.doCFRequest(req)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fs.Errorf(ctx, "Failed to close response body: %v", err)
		}
	}()
	// 读取响应体
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// 检查响应内容类型
	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		// 记录非JSON响应内容用于调试
		fs.Debugf(nil, "Unexpected response type %q with body: %s", contentType, string(bodyBytes))
		return fmt.Errorf("unexpected content type: %s", contentType)
	}

	// 尝试解析JSON
	if err := json.Unmarshal(bodyBytes, response); err != nil {
		return fmt.Errorf("failed to parse JSON response: %w, body: %s", err, string(bodyBytes))
	}

	// 检查API错误
	if err := f.handleResponse(response); err != nil {
		if err.Error() == "unauthorized access" {
			f.tokenMu.Lock()
			defer f.tokenMu.Unlock()
			if err := f.login(ctx); err != nil {
				return fmt.Errorf("token renewal failed: %w", err)
			}
			return f.doCFRequestMust(ctx, method, endpoint, data, response)
		}
		return err
	}
	return nil
}

// handleResponse examines the API response for error codes.
func (f *Fs) handleResponse(response interface{}) error {
	switch response.(type) {
	case *loginResponse, *listResponse, *requestResponse:
		v := reflect.ValueOf(response).Elem()
		code := v.FieldByName("Code").Int()
		message := v.FieldByName("Message").String()
		if code != 200 {
			if code == 401 {
				return fmt.Errorf("unauthorized access")
			}
			return fmt.Errorf("request failed: %s", message)
		}
	default:
		// No action needed.
	}
	return nil
}

// fileInfoToDirEntry converts a fileInfo instance to a fs.DirEntry.
func (f *Fs) fileInfoToDirEntry(item fileInfo, dir string) fs.DirEntry {
	remote := path.Join(dir, item.Name)
	if item.IsDir {
		return fs.NewDir(remote, item.Modified)
	}
	var md5sum, sha1sum, sha256sum string
	if item.HashInfo != nil {
		md5sum = item.HashInfo.MD5
		sha1sum = item.HashInfo.SHA1
		sha256sum = item.HashInfo.SHA256
	}
	return &Object{
		fs:        f,
		remote:    remote,
		size:      item.Size,
		modTime:   item.Modified,
		md5sum:    md5sum,
		sha1sum:   sha1sum,
		sha256sum: sha256sum,
	}
}

// List lists the objects and directories in dir.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	// 检查缓存
	if cached, ok := f.getCachedList(dir); ok {
		return f.processListResponse(dir, cached), nil
	}

	data := map[string]interface{}{
		"path":     path.Join(f.root, dir),
		"per_page": 1000,
		"page":     1,
		"password": f.opt.MetaPass,
		"refresh":  f.userPermission == 2,
	}

	var resp listResponse
	err = f.pacer.Call(func() (bool, error) {
		err := f.doCFRequestMust(ctx, "POST", apiList, data, &resp)
		if err != nil {
			if strings.Contains(err.Error(), "object not found") {
				return false, fs.ErrorDirNotFound
			}
			return shouldRetry(err), err
		}
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	// 缓存结果
	f.setCachedList(dir, resp)
	
	return f.processListResponse(dir, resp), nil
}

// Put uploads an object to the remote.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	remote := src.Remote()
	size := src.Size()
	modTime := src.ModTime(ctx)
	putURL := f.opt.URL + "/api/fs/put"
	req, err := http.NewRequestWithContext(ctx, "PUT", putURL, in)
	if err != nil {
		return nil, err
	}
	encodedFilePath := url.PathEscape(path.Join(f.root, remote))
	req.Header.Set("File-Path", encodedFilePath)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", fmt.Sprintf("%d", size))
	req.Header.Set("last-modified", fmt.Sprintf("%d", modTime.UnixMilli()))
	req.ContentLength = size
	resp, err := f.doCFRequest(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fs.Errorf(ctx, "Failed to close response body: %v", err)
		}
	}()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var uploadResp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(bodyBytes, &uploadResp); err != nil {
		return nil, err
	}
	if uploadResp.Code != 200 {
		return nil, fmt.Errorf("upload failed: %s", uploadResp.Message)
	}
	parentDir := path.Dir(src.Remote())
	f.invalidateCache(parentDir)
	return &Object{
		fs:      f,
		remote:  remote,
		size:    size,
		modTime: modTime,
	}, nil
}

// Mkdir creates a directory.
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	// 检查目录名是否合法
	if strings.Contains(dir, "..") {
		return fmt.Errorf("invalid directory name containing ..")
	}
	
	// 规范化路径
	dir = path.Clean(dir)
	
	data := map[string]string{
		"path": path.Join(f.root, dir),
	}
	
	var resp requestResponse
	err := f.pacer.Call(func() (bool, error) {
		err := f.doCFRequestMust(ctx, "POST", apiMkdir, data, &resp)
		return shouldRetry(err), err
	})
	
	if err != nil {
		return fmt.Errorf("mkdir failed: %w", err)
	}
	
	// 清除目录缓存
	parentDir := path.Dir(dir)
	f.invalidateCache(parentDir)
	
	return nil
}

// Rmdir removes an empty directory.
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return f.purgeDir(ctx, dir, false)
}

// purgeDir removes the directory.
func (f *Fs) purgeDir(ctx context.Context, dir string, recursive bool) error {
	removeURL := "/api/fs/remove"
	names := []string{"."}
	data := map[string]interface{}{
		"dir":   path.Join(f.root, dir),
		"names": names,
	}
	var removeResp requestResponse
	if err := f.doCFRequestMust(ctx, "POST", removeURL, data, &removeResp); err != nil {
		return err
	}
	f.fileListCacheMu.Lock()
	delete(f.fileListCache, dir)
	f.fileListCacheMu.Unlock()
	return nil
}

// Object methods.

func (o *Object) Remote() string {
	return o.remote
}

func (o *Object) Size() int64 {
	return o.size
}

func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return fs.ErrorCantSetModTime
}

func (o *Object) Hashes() hash.Set {
	return hash.NewHashSet(hash.MD5, hash.SHA1, hash.SHA256)
}

func (o *Object) Storable() bool {
	return true
}

// Open retrieves the raw download URL and uses doCFRequest to handle CF challenges.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	getURL := "/api/fs/get"
	data := map[string]string{
		"path": path.Join(o.fs.root, o.remote),
	}
	var getResp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			RawURL string `json:"raw_url"`
		} `json:"data"`
	}
	if err := o.fs.doCFRequestMust(ctx, "POST", getURL, data, &getResp); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "GET", getResp.Data.RawURL, nil)
	if err != nil {
		return nil, err
	}
	fs.FixRangeOption(options, o.size)
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	if o.size == 0 {
		delete(req.Header, "Range")
	}
	response, err := o.fs.doCFRequest(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != 200 && response.StatusCode != 206 {
		_ = response.Body.Close()
		return nil, fmt.Errorf("failed to open object: status code %d", response.StatusCode)
	}
	return response.Body, nil
}

func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	_, err := o.fs.Put(ctx, in, src, options...)
	return err
}

func (o *Object) Remove(ctx context.Context) error {
	removeURL := "/api/fs/remove"
	data := map[string]interface{}{
		"dir":   path.Dir(path.Join(o.fs.root, o.remote)),
		"names": []string{path.Base(o.remote)},
	}
	var removeResp requestResponse
	if err := o.fs.doCFRequestMust(ctx, "POST", removeURL, data, &removeResp); err != nil {
		return err
	}
	o.fs.invalidateCache(path.Dir(o.remote))
	return nil
}

func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	switch ty {
	case hash.MD5:
		return o.md5sum, nil
	case hash.SHA1:
		return o.sha1sum, nil
	case hash.SHA256:
		return o.sha256sum, nil
	default:
		return "", hash.ErrUnsupported
	}
}

func (o *Object) String() string {
	return fmt.Sprintf("AList Object: %s", o.remote)
}

func (f *Fs) Hashes() hash.Set {
	return hash.NewHashSet(hash.MD5, hash.SHA1, hash.SHA256)
}

func (f *Fs) Precision() time.Duration {
	return time.Second
}

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	dir := path.Dir(remote)
	entries, err := f.List(ctx, dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.Remote() == remote {
			if obj, ok := entry.(*Object); ok {
				return obj, nil
			}
		}
	}
	return nil, fs.ErrorObjectNotFound
}

func (f *Fs) String() string {
	return f.name
}

// getCachedList retrieves a cached directory listing.
func (f *Fs) getCachedList(dir string) (listResponse, bool) {
	f.fileListCacheMu.Lock()
	defer f.fileListCacheMu.Unlock()
	cached, ok := f.fileListCache[dir]
	return cached, ok
}

// setCachedList caches a directory listing.
func (f *Fs) setCachedList(dir string, resp listResponse) {
	f.fileListCacheMu.Lock()
	defer f.fileListCacheMu.Unlock()
	f.fileListCache[dir] = resp
}

// invalidateCache deletes the cached listing for a directory.
func (f *Fs) invalidateCache(dir string) {
	f.fileListCacheMu.Lock()
	defer f.fileListCacheMu.Unlock()
	delete(f.fileListCache, dir)
}

// fetchCloudflare contacts the CF server to obtain cookies (and optionally a user agent)
// for the given targetURL.
func (f *Fs) fetchCloudflare(ctx context.Context, targetURL string) error {
	reqURL := fmt.Sprintf("%s/get-cookies?url=%s", f.opt.CfServer, url.QueryEscape(targetURL))
	resp, err := http.Get(reqURL)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fs.Errorf(ctx, "Failed to close response body in fetchCloudflare: %v", err)
		}
	}()

	// Decode the JSON response into a map.
	// For example, the response might be: {"cf_clearance": "cookie_value"}
	var cookieMap map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&cookieMap); err != nil {
		return err
	}
	if len(cookieMap) == 0 {
		return fmt.Errorf("no cookies received from cf_server")
	}

	// Retrieve the first (and presumably only) cookie from the map.
	var cookieName, cookieValue string
	for k, v := range cookieMap {
		cookieName = k
		cookieValue = v
		break
	}

	// Parse the target URL to extract the host (for use as the cookie domain).
	parsed, err := url.Parse(targetURL)
	if err != nil {
		return err
	}
	host := parsed.Host

	// Set the cookie's expiry to 30 minutes from now.
	expiry := time.Now().Add(30 * time.Minute)

	// Create the cookie with default Domain (the host), Path "/", and the computed expiry.
	cfCookie := &http.Cookie{
		Name:    cookieName,
		Value:   cookieValue,
		Domain:  host,
		Path:    "/",
		Expires: expiry,
	}

	// Save the cookie and its expiry keyed by host.
	f.cfCookies[host] = cfCookie
	f.cfCookieExpiry[host] = expiry

	return nil
}

// fetchUserAgent retrieves the user agent from the CF server's /get-ua endpoint.
// It uses http.DefaultClient because this is a one-off call during initialization.
func (f *Fs) fetchUserAgent(ctx context.Context) error {
	reqURL := fmt.Sprintf("%s/get-ua", f.opt.CfServer)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fs.Errorf(ctx, "Failed to close response body in fetchUserAgent: %v", err)
		}
	}()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var data struct {
		UserAgent string `json:"user_agent"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal(bodyBytes, &data); err != nil {
		return err
	}
	if data.Error != "" {
		return fmt.Errorf("cfserver error: %s", data.Error)
	}
	f.cfUserAgent = data.UserAgent
	f.opt.UserAgent = data.UserAgent
	return nil
}

// Copy 函数修改
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
    srcObj, ok := src.(*Object)
    if !ok {  // 修复括号语法
        return nil, fs.ErrorObjectNotFound
    }

    // 获取源和目标的相对路径
    srcPath := path.Join(srcObj.fs.root, srcObj.remote)
    dstPath := path.Join(f.root, remote)

    fs.Debugf(nil, "Copying from %q to %q", srcPath, dstPath)

    // 检查源和目标是否相同
    if srcPath == dstPath {
        fs.Debugf(nil, "Source and destination are identical: %s", srcPath)
        return srcObj, nil
    }

    // 确保目标目录存在
    dstDir := path.Dir(dstPath)
    if err := f.checkPath(ctx, dstDir); err != nil {
        return nil, fmt.Errorf("failed to check destination path: %w", err)
    }

    // 构造复制请求
    data := map[string]interface{}{
        "src_dir": path.Dir(srcPath),
        "dst_dir": dstDir,
        "names":   []string{path.Base(srcPath)},
    }

    fs.Debugf(nil, "Copy request data: %+v", data)

    // 发送复制请求
    var resp struct {
        Code    int    `json:"code"`
        Message string `json:"message"`
        Data    string `json:"data"`
    }

    err := f.pacer.Call(func() (bool, error) {
        err := f.doCFRequestMust(ctx, "POST", apiCopy, data, &resp)
        if err != nil {
            fs.Debugf(nil, "Copy request failed: %v", err)
            return shouldRetry(err), err
        }
        if resp.Code != 200 {
            return false, fmt.Errorf("copy failed with code %d: %s", resp.Code, resp.Message)
        }
        return false, nil
    })

    if err != nil {
        return nil, fmt.Errorf("failed to copy: %w", err)
    }

    // 清除缓存
    f.invalidateCache(path.Dir(remote))

    // 等待并验证文件复制完成
    startTime := time.Now()
    const maxWaitTime = 30 * time.Second

    for time.Since(startTime) < maxWaitTime {
        obj, err := f.NewObject(ctx, remote)
        if err == nil {
            if obj.Size() == srcObj.size || srcObj.size == 0 {
                return obj, nil
            }
            fs.Debugf(nil, "Size mismatch for %s: expected %d got %d",
                remote, srcObj.size, obj.Size())
        }
        time.Sleep(time.Second)
    }

    return nil, fmt.Errorf("timeout waiting for copy completion of %s", remote)
}

// CopyDir 实现目录复制
func (f *Fs) CopyDir(ctx context.Context, srcFs fs.Fs, srcRemote, dstRemote string) error {
    // 确保源和目标都是 AList 类型
    srcAlist, ok := srcFs.(*Fs)
    if !ok {  // 修复括号语法
        return fs.ErrorCantCopy
    }

    // 创建目标目录
    err := f.Mkdir(ctx, dstRemote)
    if err != nil {  // 修复括号语法
        return fmt.Errorf("failed to create destination directory: %w", err)
    }

    // 获取源目录中的所有文件
    entries, err := srcAlist.List(ctx, srcRemote)
    if (err != nil) {
        return fmt.Errorf("failed to list source directory: %w", err)
    }

    // 获取配置的并发传输数
    ci := fs.GetConfig(ctx)
    
    // 使用信号量控制并发复制
    sem := make(chan struct{}, ci.Transfers)
    var wg sync.WaitGroup
    var copyErrors []error
    var errMu sync.Mutex

    // 并发复制所有文件
    for _, entry := range entries {
        if obj, ok := entry.(fs.Object); ok {
            wg.Add(1)
            go func(obj fs.Object) {
                defer wg.Done()
                sem <- struct{}{} // 获取信号量
                defer func() { <-sem }() // 释放信号量

                relPath := obj.Remote()[len(srcRemote):]
                newRemote := path.Join(dstRemote, relPath)

                _, err := f.Copy(ctx, obj, newRemote)
                if (err != nil) {
                    errMu.Lock()
                    copyErrors = append(copyErrors, fmt.Errorf("failed to copy %s: %w", obj.Remote(), err))
                    errMu.Unlock()
                }
            }(obj)
        }
    }

    // 等待所有复制操作完成
    wg.Wait()

    // 检查是否有错误发生
    if len(copyErrors) > 0 {
        return fmt.Errorf("multiple copy errors: %v", copyErrors)
    }

    return nil
}

// Add new helper function
func (f *Fs) checkRoot(ctx context.Context) error {
    // 分解root路径
    parts := strings.Split(strings.Trim(f.root, "/"), "/")
    currentPath := ""
    
    // 逐级检查并创建目录
    for _, part := range parts {
        if (part == "") {
            continue
        }
        
        currentPath = path.Join(currentPath, part)
        
        // 尝试列出当前目录
        _, err := f.List(ctx, currentPath)
        if (err != nil) {
            // 如果目录不存在则创建
            if (err == fs.ErrorDirNotFound) {
                createErr := f.Mkdir(ctx, currentPath)
                if (createErr != nil) { // 修复这里的语法错误
                    return fmt.Errorf("failed to create directory %s: %w", currentPath, createErr)
                }
                fs.Debugf(nil, "Created directory: %s", currentPath)
                continue
            }
            return fmt.Errorf("failed to check directory %s: %w", currentPath, err) 
        }
    }
    
    return nil
}

// Add new helper function
func (f *Fs) checkPath(ctx context.Context, dirPath string) error {
    if (dirPath == "") {
        return nil
    }

    // Check if directory exists
    _, err := f.List(ctx, dirPath)
    if (err == nil) {
        return nil
    }

    // If parent doesn't exist, create it first
    parent := path.Dir(dirPath) // 使用 path 包的 Dir 函数
    if (parent != "." && parent != "/") {
        err = f.checkPath(ctx, parent)
        if (err != nil) {
            return err
        }
    }

    // Create directory
    return f.Mkdir(ctx, dirPath)
}

// Add helper function for retry decisions
func shouldRetry(err error) bool {
    if (err == nil) {
        return false
    }
    
    // Add specific error types that should trigger retry
    if (strings.Contains(err.Error(), "directory not found")) {
        return true
    }
    if (strings.Contains(err.Error(), "network error")) {
        return true
    }
    // Add other retryable error conditions as needed
    
    return false
}

// 添加辅助函数处理List响应
func (f *Fs) processListResponse(dir string, resp listResponse) fs.DirEntries {
    entries := make(fs.DirEntries, 0, len(resp.Data.Content))
    for _, item := range resp.Data.Content {
        entries = append(entries, f.fileInfoToDirEntry(item, dir))
    }
    return entries
}

// 添加新的辅助函数来等待和验证文件
func (f *Fs) waitForFile(ctx context.Context, remote string, expectedSize int64) error {
    startTime := time.Now()
    const maxWaitTime = 30 * time.Second
    const checkInterval = time.Second

    for {
        if (time.Since(startTime) > maxWaitTime) {
            return fmt.Errorf("timeout waiting for file to appear: %s", remote)
        }

        obj, err := f.NewObject(ctx, remote)
        if (err == nil) {
            // 验证文件大小
            if (obj.Size() == expectedSize) {
                return nil
            }
            return fmt.Errorf("copied file size mismatch, expected %d got %d", expectedSize, obj.Size())
        }

        // 等待一段时间后重试
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(checkInterval):
        }
    }
}

// 修改等待任务完成的函数
func (f *Fs) waitForCopyTask(ctx context.Context, taskID string) error {
    const (
        checkInterval = time.Second
        maxRetries = 3
        timeout = 5 * time.Minute
    )

    timer := time.NewTimer(timeout)
    defer timer.Stop()

    ticker := time.NewTicker(checkInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-timer.C:
            return fmt.Errorf("copy task timeout after %v", timeout)
        case <-ticker.C:
            var resp struct {
                Code    int    `json:"code"`
                Message string `json:"message"`
                Data    []struct {
                    ID         string    `json:"id"`
                    State      int       `json:"state"`
                    Progress   float64   `json:"progress"`
                    Error      string    `json:"error"`
                    TotalBytes int64     `json:"total_bytes"`
                    StartTime  time.Time `json:"start_time"`
                    EndTime    time.Time `json:"end_time"`
                } `json:"data"`
            }

            err := f.doCFRequestMust(ctx, "GET", apiTaskInfo, nil, &resp)
            if (err != nil) {
                fs.Debugf(nil, "Failed to check task status: %v", err)
                continue
            }

            // 查找目标任务
            for _, task := range resp.Data {
                if (task.ID == taskID) {
                    fs.Debugf(nil, "Task %s: state=%d progress=%.2f%% bytes=%d", 
                        taskID, task.State, task.Progress, task.TotalBytes)

                    switch task.State {
                    case 2: // 成功完成
                        return nil
                    case 1: // 正在进行
                        continue
                    case 3: // 暂停
                        continue
                    case 4: // 失败
                        return fmt.Errorf("copy task failed: %s", task.Error)
                    case 5: // 取消
                        return fmt.Errorf("copy task canceled")
                    case 0: // 等待中
                        continue
                    default:
                        fs.Debugf(nil, "Unknown task state: %d", task.State)
                    }
                }
            }
        }
    }
}

// 添加新的等待函数
func (f *Fs) waitForFileCompletion(ctx context.Context, remote string, expectedSize int64) (fs.Object, error) {
    startTime := time.Now()
    const (
        maxWaitTime = 5 * time.Minute  // 增加等待时间
        checkInterval = 5 * time.Second // 增加检查间隔
        maxRetries = 3
    )

    for {
        if (time.Since(startTime) > maxWaitTime) {
            return nil, fmt.Errorf("timeout waiting for file completion: %s", remote)
        }

        var retryCount int
        var obj fs.Object
        var err error

        // 添加重试逻辑
        for retryCount < maxRetries {
            obj, err = f.NewObject(ctx, remote)
            if (err == nil) {
                break
            }
            retryCount++
            if (retryCount < maxRetries) {
                time.Sleep(time.Second)
            }
        }

        if (err == nil) {
            if (obj.Size() == expectedSize || expectedSize == 0) {
                fs.Debugf(nil, "File copy completed successfully: %s", remote)
                return obj, nil
            }
            fs.Debugf(nil, "File size mismatch for %s: expected %d, got %d", 
                remote, expectedSize, obj.Size())
        } else {
            fs.Debugf(nil, "File not found yet: %s (attempt %d)", remote, retryCount)
        }

        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-time.After(checkInterval):
            continue
        }
    }
}
