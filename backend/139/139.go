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
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/random"
	"github.com/rclone/rclone/lib/rest"
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
			Name:     "cloud_type",
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
			Help:     "Root folder ID (empty string for personal_new, 'root' for personal)",
			Required: false,
			Default:  "", // 默认值为空字符串
		}, {
			Name:     "cloud_id",
			Help:     "Cloud ID (required for family/group)",
			Required: false,
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	CloudType      string `config:"cloud_type"`
	Authorization  string `config:"authorization"`
	RootFolderID   string `config:"root_folder_id"`
	CloudID        string `config:"cloud_id"`
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
	// 添加目录 ID 缓存
	dirCache map[string]string // 路径到目录 ID 的映射
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

// GetDiskResp 个人云获取目录列表响应
type GetDiskResp struct {
	BaseResp
	Data struct {
		Result struct {
			ResultCode string      `json:"resultCode"`
			ResultDesc interface{} `json:"resultDesc"`
		} `json:"result"`
		GetDiskResult struct {
			ParentCatalogID string    `json:"parentCatalogID"`
			NodeCount       int       `json:"nodeCount"`
			CatalogList     []Catalog `json:"catalogList"`
			ContentList     []Content `json:"contentList"`
			IsCompleted     int       `json:"isCompleted"`
		} `json:"getDiskResult"`
	} `json:"data"`
}

// Catalog 目录信息
type Catalog struct {
	CatalogID   string `json:"catalogID"`
	CatalogName string `json:"catalogName"`
	CreateTime  string `json:"createTime"`
	UpdateTime  string `json:"updateTime"`
}

// Content 文件信息
type Content struct {
	ContentID   string `json:"contentID"`
	ContentName string `json:"contentName"`
	ContentSize int64  `json:"contentSize"`
	CreateTime  string `json:"createTime"`
	UpdateTime  string `json:"updateTime"`
	Digest      string `json:"digest"`
}

// QueryContentListResp 家庭云和群组云获取目录列表响应
type QueryContentListResp struct {
	BaseResp
	Data struct {
		Result struct {
			ResultCode string `json:"resultCode"`
			ResultDesc string `json:"resultDesc"`
		} `json:"result"`
		Path             string         `json:"path"`
		CloudContentList []CloudContent `json:"cloudContentList"`
		CloudCatalogList []CloudCatalog `json:"cloudCatalogList"`
		TotalCount       int            `json:"totalCount"`
	} `json:"data"`
}

// CloudContent 家庭云和群组云文件信息
type CloudContent struct {
	ContentID       string `json:"contentID"`
	ContentName     string `json:"contentName"`
	ContentSize     int64  `json:"contentSize"`
	CreateTime      string `json:"createTime"`
	LastUpdateTime  string `json:"lastUpdateTime"`
}

// CloudCatalog 家庭云和群组云目录信息
type CloudCatalog struct {
	CatalogID       string `json:"catalogID"`
	CatalogName     string `json:"catalogName"`
	CreateTime      string `json:"createTime"`
	LastUpdateTime  string `json:"lastUpdateTime"`
}

// PersonalListResp 新版个人云获取目录列表响应
type PersonalListResp struct {
	BaseResp
	Data struct {
		Items          []PersonalFileItem `json:"items"`
		NextPageCursor string            `json:"nextPageCursor"`
	} `json:"data"`
}

// PersonalFileItem 新版个人云文件项
type PersonalFileItem struct {
	FileId    string `json:"fileId"`
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	Type      string `json:"type"`
	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
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
	// 检查令牌格式
	if !strings.HasPrefix(f.opt.Authorization, "Basic ") {
		f.opt.Authorization = "Basic " + f.opt.Authorization
	}

	// 移除 "Basic " 前缀
	auth := strings.TrimPrefix(f.opt.Authorization, "Basic ")

	// 尝试解码
	decode, err := base64.StdEncoding.DecodeString(auth)
	if err != nil {
		return fmt.Errorf("authorization decode failed: %v", err)
	}

	decodeStr := string(decode)
	splits := strings.Split(decodeStr, ":")
	if len(splits) < 3 {
		return fmt.Errorf("authorization is invalid: expected format 'client_id:account:token|version|type|expire_time|...', got %d parts", len(splits))
	}

	tokenParts := strings.Split(splits[2], "|")
	if len(tokenParts) < 5 {
		return fmt.Errorf("authorization token is invalid: expected format 'token|version|type|expire_time|...', got %d parts", len(tokenParts))
	}

	// 解析过期时间（使用第四个字段）
	expiration, err := strconv.ParseInt(tokenParts[3], 10, 64)
	if err != nil {
		return fmt.Errorf("authorization token expiration time is invalid: %v", err)
	}

	now := time.Now().UnixMilli()

	// 如果令牌还有超过15天的有效期，不需要刷新
	if expiration-now > 1000*60*60*24*15 {
		return nil
	}

	// 如果令牌还有超过1小时的有效期，不需要刷新
	if expiration-now > 1000*60*60 {
		return nil
	}

	// 尝试刷新令牌
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
		return fmt.Errorf("failed to refresh token: %v", err)
	}

	if resp.Return != "0" {
		return fmt.Errorf("failed to refresh token: %s (return code: %s)", resp.Desc, resp.Return)
	}

	// 更新令牌
	newToken := fmt.Sprintf("%s:%s:%s", splits[0], splits[1], resp.Token)
	newTokenBase64 := base64.StdEncoding.EncodeToString([]byte(newToken))
	f.opt.Authorization = "Basic " + newTokenBase64

	// 验证新令牌
	decode, err = base64.StdEncoding.DecodeString(newTokenBase64)
	if err != nil {
		return fmt.Errorf("new token validation failed: %v", err)
	}

	decodeStr = string(decode)
	splits = strings.Split(decodeStr, ":")
	if len(splits) < 3 {
		return fmt.Errorf("new token validation failed: invalid format")
	}

	tokenParts = strings.Split(splits[2], "|")
	if len(tokenParts) < 5 {
		return fmt.Errorf("new token validation failed: invalid token format")
	}

	expiration, err = strconv.ParseInt(tokenParts[3], 10, 64)
	if err != nil {
		return fmt.Errorf("new token validation failed: invalid expiration time")
	}

	if expiration < time.Now().UnixMilli() {
		return fmt.Errorf("new token validation failed: token already expired")
	}

	return nil
}

// request makes an API request to 139 cloud
func (f *Fs) request(ctx context.Context, opts *rest.Opts) ([]byte, error) {
	// Try to refresh token if needed
	err := f.refreshToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("token refresh failed: %v", err)
	}

	// 根据云盘类型选择不同的 API 端点
	var baseURL string
	switch f.opt.CloudType {
	case MetaPersonalNew:
		baseURL = "https://personal-kd-njs.yun.139.com"
	default:
		baseURL = "https://yun.139.com"
	}

	// 创建一个新的 opts 对象，避免修改原始对象
	newOpts := *opts
	if newOpts.ExtraHeaders == nil {
		newOpts.ExtraHeaders = make(map[string]string)
	}

	// 准备请求体
	var bodyBytes []byte
	if opts.Body != nil {
		// 如果 Body 是 io.Reader，直接读取内容
		if reader, ok := opts.Body.(io.Reader); ok {
			var err error
			bodyBytes, err = io.ReadAll(reader)
			if err != nil {
				return nil, fmt.Errorf("failed to read request body: %v", err)
			}
			// 重新设置 Body 为新的 Reader
			newOpts.Body = bytes.NewReader(bodyBytes)
		} else {
			// 如果是其他类型，尝试 JSON 序列化
			var err error
			bodyBytes, err = json.Marshal(opts.Body)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal request body: %v", err)
			}
			newOpts.Body = bytes.NewReader(bodyBytes)
		}
	}

	// 生成签名
	randStr := random.String(16)
	ts := time.Now().Format("2006-01-02 15:04:05")
	sign := calSign(string(bodyBytes), ts, randStr)

	// 根据云盘类型选择不同的请求头
	if f.opt.CloudType == MetaPersonalNew {
		newOpts.ExtraHeaders = map[string]string{
			"Accept":               "application/json, text/plain, */*",
			"Authorization":        f.opt.Authorization,
			"Caller":               "web",
			"Cms-Device":           "default",
			"Content-Type":         "application/json;charset=UTF-8",
			"Mcloud-Channel":       "1000101",
			"Mcloud-Client":        "10701",
			"Mcloud-Route":         "001",
			"Mcloud-Sign":          fmt.Sprintf("%s,%s,%s", ts, randStr, sign),
			"Mcloud-Version":       "7.14.0",
			"Origin":               "https://yun.139.com",
			"Referer":              "https://yun.139.com/w/",
			"x-DeviceInfo":         "||9|7.14.0|chrome|120.0.0.0|||windows 10||zh-CN|||",
			"x-huawei-channelSrc":  "10000034",
			"x-inner-ntwk":         "2",
			"x-m4c-caller":         "PC",
			"x-m4c-src":            "10002",
			"x-SvcType":            "1",
			"X-Yun-Api-Version":    "v1",
			"X-Yun-App-Channel":    "10000034",
			"X-Yun-Channel-Source": "10000034",
			"X-Yun-Client-Info":    "||9|7.14.0|chrome|120.0.0.0|||windows 10||zh-CN|||dW5kZWZpbmVk||",
			"X-Yun-Module-Type":    "100",
			"X-Yun-Svc-Type":       "1",
		}
	} else {
		svcType := "1"
		if f.opt.CloudType == MetaFamily {
			svcType = "2"
		}
		newOpts.ExtraHeaders = map[string]string{
			"Accept":               "application/json, text/plain, */*",
			"Authorization":        f.opt.Authorization,
			"CMS-DEVICE":           "default",
			"Content-Type":         "application/json;charset=UTF-8",
			"mcloud-channel":       "1000101",
			"mcloud-client":        "10701",
			"mcloud-sign":          fmt.Sprintf("%s,%s,%s", ts, randStr, sign),
			"mcloud-version":       "7.14.0",
			"Origin":               "https://yun.139.com",
			"Referer":              "https://yun.139.com/w/",
			"x-DeviceInfo":         "||9|7.14.0|chrome|120.0.0.0|||windows 10||zh-CN|||",
			"x-huawei-channelSrc":  "10000034",
			"x-inner-ntwk":         "2",
			"x-m4c-caller":         "PC",
			"x-m4c-src":            "10002",
			"x-SvcType":            svcType,
			"Inner-Hcy-Router-Https": "1",
		}
	}

	// 设置基础 URL
	f.srv.SetRoot(baseURL)

	var result []byte
	var apiResp BaseResp

	// 使用 pacer 进行请求重试
	err = f.pacer.Call(func() (bool, error) {
		// 发送请求
		httpResp, err := f.srv.Call(ctx, &newOpts)
		if err != nil {
			retry, err := shouldRetry(err)
			return retry, err
		}

		// 检查响应状态码
		if httpResp.StatusCode >= 400 {
			return true, fmt.Errorf("HTTP error %d: %s", httpResp.StatusCode, httpResp.Status)
		}

		// 读取响应体
		result, err = io.ReadAll(httpResp.Body)
		if err != nil {
			return false, fmt.Errorf("failed to read response body: %v", err)
		}
		httpResp.Body.Close()

		// 检查响应内容类型
		contentType := httpResp.Header.Get("Content-Type")
		if strings.Contains(contentType, "application/json") {
			// 尝试解析 JSON 响应
			if err := json.Unmarshal(result, &apiResp); err != nil {
				return false, fmt.Errorf("failed to parse JSON response: %v", err)
			}
			if !apiResp.Success {
				return false, fmt.Errorf("%s", apiResp.Message)
			}
		} else if strings.Contains(contentType, "application/xml") || strings.Contains(contentType, "text/xml") {
			// XML 响应，直接返回原始内容
			return false, nil
		} else {
			// 其他类型的响应，尝试作为 JSON 解析
			if err := json.Unmarshal(result, &apiResp); err != nil {
				// 如果不是 JSON，返回原始内容
				return false, nil
			}
			if !apiResp.Success {
				return false, fmt.Errorf("%s", apiResp.Message)
			}
		}

		return false, nil
	})

	if err != nil {
		return nil, err
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

// List implements fs.Fs
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	// 打印调试信息
	fmt.Printf("Debug - List called with dir: %q\n", dir)
	fmt.Printf("Debug - Current root_folder_id: %q\n", f.opt.RootFolderID)
	fmt.Printf("Debug - Cloud type: %s\n", f.opt.CloudType)
	fmt.Printf("Debug - Current root: %q\n", f.root)

	// 获取目录 ID
	var dirID string
	if dir == "" {
		// 如果是根目录，需要逐级查找目标目录
		parts := strings.Split(strings.Trim(f.root, "/"), "/")
		fmt.Printf("Debug - Looking for path parts: %v\n", parts)

		// 从根目录开始
		currentID := "/"
		currentPath := ""

		// 逐级查找目录
		for i, part := range parts {
			currentPath = path.Join(currentPath, part)
			fmt.Printf("Debug - Looking for part %d: %q in path %q\n", i, part, currentPath)

			// 检查缓存
			if cachedID, ok := f.dirCache[currentPath]; ok {
				fmt.Printf("Debug - Found cached ID for %q: %q\n", currentPath, cachedID)
				currentID = cachedID
				continue
			}

			// 获取当前目录的内容
			items, err := f.listDir(ctx, currentID)
			if err != nil {
				return nil, fmt.Errorf("failed to list directory %q: %v", currentPath, err)
			}

			// 在当前目录中查找目标目录
			found := false
			for _, item := range items {
				fmt.Printf("Debug - Checking item: %q (ID: %q) in %q\n", item.Name, item.ID, currentPath)
				if item.Name == part {
					if item.IsDirectory {
						currentID = item.ID
						// 缓存目录 ID
						f.dirCache[currentPath] = currentID
						fmt.Printf("Debug - Found directory ID for %q: %q\n", currentPath, currentID)
						found = true
						break
					} else {
						fmt.Printf("Debug - Found %q but it's not a directory\n", part)
						return nil, fs.ErrorIsFile
					}
				}
			}

			if !found {
				fmt.Printf("Debug - Directory not found: %q\n", currentPath)
				return nil, fs.ErrorDirNotFound
			}
		}

		dirID = currentID
	} else {
		// 如果不是根目录，需要从当前目录开始查找
		parentDir := path.Dir(dir)
		dirName := path.Base(dir)
		
		// 获取父目录的 ID
		var parentID string
		if parentDir == "." {
			// 如果父目录是当前目录，使用当前目录的 ID
			parentID = dirID
		} else {
			// 否则递归获取父目录的 ID
			parentEntries, err := f.List(ctx, parentDir)
			if err != nil {
				return nil, fmt.Errorf("failed to list parent directory: %v", err)
			}
			// 从父目录的条目中找到当前目录
			for _, entry := range parentEntries {
				if entry.Remote() == dir {
					if d, ok := entry.(fs.Directory); ok {
						parentID = d.ID()
						break
					}
				}
			}
		}

		// 在父目录中查找目标目录
		parentItems, err := f.listDir(ctx, parentID)
		if err != nil {
			return nil, fmt.Errorf("failed to list parent directory: %v", err)
		}

		found := false
		for _, item := range parentItems {
			fmt.Printf("Debug - Checking parent item: %q (ID: %q)\n", item.Name, item.ID)
			if item.Name == dirName {
				if item.IsDirectory {
					dirID = item.ID
					f.dirCache[dir] = dirID
					fmt.Printf("Debug - Found directory ID for %q: %q\n", dir, dirID)
					found = true
					break
				} else {
					fmt.Printf("Debug - Found %q but it's not a directory\n", dir)
					return nil, fs.ErrorIsFile
				}
			}
		}
		if !found {
			fmt.Printf("Debug - Directory not found: %q\n", dir)
			return nil, fs.ErrorDirNotFound
		}
	}

	// List directory contents
	fmt.Printf("Debug - Listing directory with ID: %q\n", dirID)
	items, err := f.listDir(ctx, dirID)
	if err != nil {
		return nil, fmt.Errorf("list directory failed: %v (dir: %q, dirID: %q)", err, dir, dirID)
	}

	// Convert to fs.DirEntries
	entries = make(fs.DirEntries, 0, len(items))
	for _, item := range items {
		remote := path.Join(dir, item.Name)
		fmt.Printf("Debug - Processing item: %q (ID: %q, IsDir: %v)\n", item.Name, item.ID, item.IsDirectory)
		
		if item.IsDirectory {
			// 缓存目录 ID
			f.dirCache[remote] = item.ID
			fmt.Printf("Debug - Caching directory ID for %q: %q\n", remote, item.ID)
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

// getRootFolderID 获取新版个人云的根目录ID
func (f *Fs) getRootFolderID(ctx context.Context) (string, error) {
	fmt.Printf("Debug - getRootFolderID called\n")

	// 设置基础 URL
	f.srv.SetRoot("https://personal-kd-njs.yun.139.com")
	pathname := "/hcy/file/list"

	// 处理路径，保留 "/" 前缀
	var rootID string
	if f.root == "" {
		rootID = "/"  // 根目录必须使用 "/"
	} else {
		rootID = f.root  // 保持原始路径，包括 "/" 前缀
	}

	fmt.Printf("Debug - Using root ID: %q for path: %q\n", rootID, f.root)

	// 构建请求体
	body := map[string]interface{}{
		"parentFileId": rootID,  // 使用处理后的路径
		"imageThumbnailStyleList": []string{"Small", "Large"},
		"orderBy":                 "updated_at",
		"orderDirection":          "DESC",
		"pageInfo": map[string]interface{}{
			"pageCursor": "",
			"pageSize":   100,
		},
	}

	// 打印请求详情
	fmt.Printf("Debug - Request details:\n")
	fmt.Printf("  URL: %s%s\n", "https://personal-kd-njs.yun.139.com", pathname)
	fmt.Printf("  Method: POST\n")
	fmt.Printf("  Parent File ID: %q\n", rootID)
	fmt.Printf("  Request body: %+v\n", body)

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request body: %v", err)
	}

	opts := rest.Opts{
		Method: "POST",
		Path:   pathname,
		Body:   bytes.NewReader(bodyBytes),
	}

	var resp PersonalListResp
	result, err := f.request(ctx, &opts)
	if err != nil {
		return "", err
	}

	if err := json.Unmarshal(result, &resp); err != nil {
		return "", fmt.Errorf("parse response failed: %v (response: %s)", err, string(result))
	}

	if !resp.Success {
		return "", fmt.Errorf("%s", resp.Message)
	}

	// 返回根目录的ID
	return rootID, nil
}

// getDirID 根据路径获取目录ID
func (f *Fs) getDirID(ctx context.Context, dir string) (string, error) {
	fmt.Printf("Debug - getDirID called with dir: %q\n", dir)

	// 如果是根目录，获取根目录ID
	if dir == "" {
		rootID, err := f.getRootFolderID(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get root folder ID: %v", err)
		}
		return rootID, nil
	}

	// 检查缓存中是否有该目录的ID
	if cachedID, ok := f.dirCache[dir]; ok {
		fmt.Printf("Debug - Using cached directory ID for %q: %q\n", dir, cachedID)
		return cachedID, nil
	}

	// 分割路径
	parts := strings.Split(strings.Trim(dir, "/"), "/")
	currentPath := ""
	
	// 获取根目录ID作为起始点
	parentID, err := f.getRootFolderID(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get root folder ID: %v", err)
	}

	// 逐级查找目录ID
	for _, part := range parts {
		currentPath = path.Join(currentPath, part)
		fmt.Printf("Debug - Looking up directory ID for path: %q (parent ID: %q)\n", currentPath, parentID)

		// 设置请求参数
		f.srv.SetRoot("https://personal-kd-njs.yun.139.com")
		pathname := "/hcy/file/list"
		body := map[string]interface{}{
			"parentFileId": parentID,
			"imageThumbnailStyleList": []string{"Small", "Large"},
			"orderBy":                 "updated_at",
			"orderDirection":          "DESC",
			"pageInfo": map[string]interface{}{
				"pageCursor": "",
				"pageSize":   100,
			},
		}

		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return "", fmt.Errorf("failed to marshal request body: %v", err)
		}

		opts := rest.Opts{
			Method: "POST",
			Path:   pathname,
			Body:   bytes.NewReader(bodyBytes),
		}

		var resp PersonalListResp
		result, err := f.request(ctx, &opts)
		if err != nil {
			return "", fmt.Errorf("request failed: %v", err)
		}

		err = json.Unmarshal(result, &resp)
		if err != nil {
			return "", fmt.Errorf("parse response failed: %v", err)
		}

		if !resp.Success {
			return "", fmt.Errorf("API error: %s", resp.Message)
		}

		// 在当前目录中查找目标目录
		found := false
		for _, item := range resp.Data.Items {
			if item.Name == part && item.Type == "folder" {
				parentID = item.FileId
				// 缓存目录ID
				f.dirCache[currentPath] = parentID
				fmt.Printf("Debug - Found directory ID for %q: %q\n", currentPath, parentID)
				found = true
				break
			}
		}

		if !found {
			return "", fmt.Errorf("directory not found: %q", currentPath)
		}
	}

	return parentID, nil
}

// listDir lists the directory contents
func (f *Fs) listDir(ctx context.Context, dirID string) ([]FileItem, error) {
	fmt.Printf("Debug - listDir called with dirID: %q\n", dirID)

	// 设置基础 URL
	f.srv.SetRoot("https://personal-kd-njs.yun.139.com")
	pathname := "/hcy/file/list"
	body := map[string]interface{}{
		"parentFileId": dirID,  // 使用目录的 fileId
		"imageThumbnailStyleList": []string{"Small", "Large"},
		"orderBy":                 "updated_at",
		"orderDirection":          "DESC",
		"pageInfo": map[string]interface{}{
			"pageCursor": "",
			"pageSize":   100,
		},
	}

	// 打印请求详情
	fmt.Printf("Debug - MetaPersonalNew request details:\n")
	fmt.Printf("  URL: %s%s\n", "https://personal-kd-njs.yun.139.com", pathname)
	fmt.Printf("  Method: POST\n")
	fmt.Printf("  Parent File ID: %q\n", dirID)
	fmt.Printf("  Page cursor: %s\n", body["pageInfo"].(map[string]interface{})["pageCursor"])
	fmt.Printf("  Request body: %+v\n", body)

	// 将请求体转换为 JSON
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %v", err)
	}
	fmt.Printf("  Request body JSON:\n%s\n", string(bodyBytes))

	// 发送请求
	opts := rest.Opts{
		Method: "POST",
		Path:   pathname,
		Body:   bytes.NewReader(bodyBytes),  // 使用 bytes.NewReader 包装 []byte
	}

	var resp PersonalListResp  // 使用 PersonalListResp 而不是 ListResp
	result, err := f.request(ctx, &opts)  // 移除多余的参数
	if err != nil {
		return nil, fmt.Errorf("request failed: %v", err)
	}

	// 解析响应
	err = json.Unmarshal(result, &resp)
	if err != nil {
		return nil, fmt.Errorf("parse response failed: %v", err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("API error: %s", resp.Message)
	}

	// 打印响应内容
	fmt.Printf("Debug - Response body: %s\n", string(result))

	// 处理响应
	items := make([]FileItem, 0, len(resp.Data.Items))
	seenItems := make(map[string]bool) // 用于去重

	for _, item := range resp.Data.Items {
		// 创建唯一键，使用文件名和类型组合
		key := item.Name + ":" + item.Type
		if seenItems[key] {
			fmt.Printf("Debug - Skipping duplicate item: %s (%s)\n", item.Name, item.Type)
			continue
		}
		seenItems[key] = true

		// 转换 PersonalFileItem 到 FileItem
		fileItem := FileItem{
			ID:          item.FileId,  // 使用 PersonalFileItem 的字段
			Name:        item.Name,
			Size:        item.Size,
			Type:        item.Type,
			CreatedAt:   item.CreatedAt,
			UpdatedAt:   item.UpdatedAt,
			IsDirectory: item.Type == "folder",
			Hash:        "",  // PersonalFileItem 没有 ContentHash 字段
			MimeType:    "",  // PersonalFileItem 没有 FileExtension 字段
		}

		fmt.Printf("Debug - Added item: %s (%s)\n", fileItem.Name, fileItem.Type)
		items = append(items, fileItem)
	}

	return items, nil
}

// NewObject implements fs.Fs
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	// Get parent directory and filename
	parentDir := path.Dir(remote)
	if parentDir == "." {
		parentDir = ""
	}

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

// NewFs constructs an Fs from the path, container: and optional path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// 打印调试信息
	fmt.Printf("Debug - NewFs called with name: %q, root: %q\n", name, root)

	// 解析配置
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	// 创建 Fs 对象
	f := &Fs{
		name:     name,
		root:     root,  // 保存 root 路径
		opt:      opt,
		dirCache: make(map[string]string),
	}

	// 创建 REST 客户端
	client := fshttp.NewClient(ctx)
	client.Timeout = 30 * time.Second
	f.srv = rest.NewClient(client)
	f.srv.SetRoot(apiURL)
	f.srv.SetHeader("User-Agent", "rclone/139cloud")

	// 创建 pacer
	f.pacer = fs.NewPacer(ctx, pacer.NewDefault(
		pacer.MinSleep(minSleep),
		pacer.MaxSleep(maxSleep),
		pacer.DecayConstant(decayConstant),
	))

	// 设置特性
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		ReadMimeType:            true,
		WriteMimeType:           true,
	}).Fill(ctx, f)

	// 初始化
	err = f.initialize()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize: %v", err)
	}

	return f, nil
}

// initialize initializes the Fs
func (f *Fs) initialize() error {
	// Decode authorization
	if f.opt.Authorization == "" {
		return fmt.Errorf("authorization is empty")
	}

	// 检查令牌格式
	if !strings.HasPrefix(f.opt.Authorization, "Basic ") {
		f.opt.Authorization = "Basic " + f.opt.Authorization
	}

	// 移除 "Basic " 前缀
	auth := strings.TrimPrefix(f.opt.Authorization, "Basic ")

	// 尝试解码
	decode, err := base64.StdEncoding.DecodeString(auth)
	if err != nil {
		// 提供更详细的错误信息
		return fmt.Errorf("authorization decode failed: %v (please check if the token is valid base64 encoded and starts with 'Basic ')", err)
	}

	decodeStr := string(decode)
	fmt.Printf("Decoded token: %s\n", decodeStr)

	splits := strings.Split(decodeStr, ":")
	if len(splits) < 3 {
		return fmt.Errorf("authorization is invalid: expected format 'client_id:account:token|version|type|expire_time|...', got %d parts", len(splits))
	}

	fmt.Printf("Token parts:\n")
	fmt.Printf("  Client ID: %s\n", splits[0])
	fmt.Printf("  Account: %s\n", splits[1])
	fmt.Printf("  Token: %s\n", splits[2])

	tokenParts := strings.Split(splits[2], "|")
	if len(tokenParts) < 5 {
		return fmt.Errorf("authorization token is invalid: expected format 'token|version|type|expire_time|...', got %d parts", len(tokenParts))
	}

	fmt.Printf("Token components:\n")
	fmt.Printf("  Token: %s\n", tokenParts[0])
	fmt.Printf("  Version: %s\n", tokenParts[1])
	fmt.Printf("  Type: %s\n", tokenParts[2])
	fmt.Printf("  Expire time: %s\n", tokenParts[3])
	fmt.Printf("  Other info: %s\n", tokenParts[4])

	// 解析过期时间（使用第四个字段）
	expiration, err := strconv.ParseInt(tokenParts[3], 10, 64)
	if err != nil {
		return fmt.Errorf("authorization token expiration time is invalid: %v (expected a valid timestamp)", err)
	}

	// 检查过期时间格式
	now := time.Now().UnixMilli()
	
	// 检查时间戳的位数
	expStr := tokenParts[3]
	fmt.Printf("Expiration time analysis:\n")
	fmt.Printf("  Raw expiration string: %s\n", expStr)
	fmt.Printf("  Expiration timestamp: %d\n", expiration)
	fmt.Printf("  Timestamp length: %d\n", len(expStr))

	// 根据时间戳长度调整
	if len(expStr) == 10 {
		// 秒级时间戳
		expiration = expiration * 1000
		fmt.Printf("  Converted to milliseconds: %d\n", expiration)
	} else if len(expStr) == 13 {
		// 毫秒级时间戳
		fmt.Printf("  Already in milliseconds\n")
	} else {
		fmt.Printf("  Warning: Unexpected timestamp length\n")
	}

	expTime := time.UnixMilli(expiration)
	
	// 如果过期时间小于当前时间，但差值小于1小时，可能是时区问题，尝试调整
	if expiration < now {
		// 检查是否是秒级时间戳（而不是毫秒级）
		if expiration > 1000000000 && expiration < 1000000000000 {
			expiration = expiration * 1000 // 转换为毫秒
			expTime = time.UnixMilli(expiration)
			fmt.Printf("  Adjusted to milliseconds: %d\n", expiration)
		}
		
		// 如果仍然过期，返回错误
		if expiration < now {
			return fmt.Errorf("authorization token has expired (expired at %s, current time: %s)", 
				expTime.Format("2006-01-02 15:04:05"),
				time.UnixMilli(now).Format("2006-01-02 15:04:05"))
		}
	}

	// 打印调试信息
	fmt.Printf("Token timing info:\n")
	fmt.Printf("  Expiration time: %s\n", expTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("  Current time: %s\n", time.UnixMilli(now).Format("2006-01-02 15:04:05"))
	fmt.Printf("  Time remaining: %v\n", expTime.Sub(time.UnixMilli(now)))

	f.account = splits[1]

	// 检查必要的参数
	switch f.opt.CloudType {
	case MetaGroup:
		if f.opt.CloudID == "" {
			return fmt.Errorf("cloud_id is required for group cloud type")
		}
	case MetaFamily:
		if f.opt.CloudID == "" {
			return fmt.Errorf("cloud_id is required for family cloud type")
		}
	case MetaPersonalNew:
		// 对于新版个人云，如果 root_folder_id 为空，设置为 "/"
		if f.opt.RootFolderID == "" {
			f.opt.RootFolderID = "/"
		}
	case MetaPersonal:
		// 对于旧版个人云，如果 root_folder_id 为空，设置为 "root"
		if f.opt.RootFolderID == "" {
			f.opt.RootFolderID = "root"
		}
	default:
		return fmt.Errorf("unsupported cloud type: %s (supported types: personal, family, group, personal_new)", f.opt.CloudType)
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

// Mkdir implements fs.Fs
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	fmt.Printf("Debug - Mkdir called with dir: %q\n", dir)
	fmt.Printf("Debug - Current root_folder_id: %q\n", f.opt.RootFolderID)
	fmt.Printf("Debug - Cloud type: %s\n", f.opt.CloudType)
	fmt.Printf("Debug - Current root: %q\n", f.root)

	// 如果 dir 为空，说明要创建的目录就是 root
	if dir == "" {
		dir = f.root
	}

	// 分割路径为各个部分
	parts := strings.Split(strings.Trim(dir, "/"), "/")
	fmt.Printf("Debug - Path parts: %v\n", parts)

	// 从根目录开始逐级创建
	currentID := "/"
	currentPath := ""

	for i, part := range parts {
		if part == "" {
			continue
		}

		currentPath = path.Join(currentPath, part)
		fmt.Printf("Debug - Creating directory part %d: %q in path %q\n", i, part, currentPath)

		// 检查目录是否已存在
		entries, err := f.List(ctx, currentPath)
		if err == nil {
			// 目录已存在，获取其 ID
			for _, entry := range entries {
				if entry.Remote() == currentPath {
					if d, ok := entry.(fs.Directory); ok {
						currentID = d.ID()
						f.dirCache[currentPath] = currentID
						fmt.Printf("Debug - Directory %q already exists with ID: %q\n", currentPath, currentID)
						break
					}
				}
			}
			continue
		}

		// 目录不存在，需要创建
		fmt.Printf("Debug - Creating new directory: %q under parent ID: %q\n", part, currentID)

		// 根据云类型选择不同的创建目录 API
		switch f.opt.CloudType {
		case MetaPersonalNew:
			// 新个人云 API
			data := map[string]interface{}{
				"parentFileId":   currentID,
				"name":           part,
				"description":    "",
				"type":           "folder",
				"fileRenameMode": "force_rename",
			}
			fmt.Printf("Debug - Creating directory with data: %+v\n", data)
			
			// 将 data 转换为 JSON
			bodyBytes, err := json.Marshal(data)
			if err != nil {
				return fmt.Errorf("failed to marshal request body: %v", err)
			}
			
			opts := &rest.Opts{
				Method:     "POST",
				Path:       "/hcy/file/create",
				Body:       bytes.NewReader(bodyBytes),
				NoResponse: true,
			}
			_, err = f.request(ctx, opts)
			if err != nil {
				return fmt.Errorf("failed to create directory %q: %v", currentPath, err)
			}

			// 获取新创建的目录的 ID
			entries, err := f.List(ctx, currentPath)
			if err != nil {
				return fmt.Errorf("failed to list directory after creation: %v", err)
			}
			for _, entry := range entries {
				if entry.Remote() == currentPath {
					if d, ok := entry.(fs.Directory); ok {
						currentID = d.ID()
						f.dirCache[currentPath] = currentID
						fmt.Printf("Debug - Created directory %q with ID: %q\n", currentPath, currentID)
						break
					}
				}
			}

		case MetaPersonal:
			// 旧个人云 API
			data := map[string]interface{}{
				"createCatalogExtReq": map[string]interface{}{
					"parentCatalogID": currentID,
					"newCatalogName":  part,
					"commonAccountInfo": map[string]interface{}{
						"account":     f.account,
						"accountType": 1,
					},
				},
			}
			fmt.Printf("Debug - Creating directory with data: %+v\n", data)
			
			bodyBytes, err := json.Marshal(data)
			if err != nil {
				return fmt.Errorf("failed to marshal request body: %v", err)
			}
			
			opts := &rest.Opts{
				Method:     "POST",
				Path:       "/orchestration/personalCloud/catalog/v1.0/createCatalogExt",
				Body:       bytes.NewReader(bodyBytes),
				NoResponse: true,
			}
			_, err = f.request(ctx, opts)
			if err != nil {
				return fmt.Errorf("failed to create directory %q: %v", currentPath, err)
			}

			// 获取新创建的目录的 ID
			entries, err := f.List(ctx, currentPath)
			if err != nil {
				return fmt.Errorf("failed to list directory after creation: %v", err)
			}
			for _, entry := range entries {
				if entry.Remote() == currentPath {
					if d, ok := entry.(fs.Directory); ok {
						currentID = d.ID()
						f.dirCache[currentPath] = currentID
						fmt.Printf("Debug - Created directory %q with ID: %q\n", currentPath, currentID)
						break
					}
				}
			}

		case MetaFamily:
			// 家庭云 API
			data := map[string]interface{}{
				"cloudID": f.opt.CloudID,
				"commonAccountInfo": map[string]interface{}{
					"account":     f.account,
					"accountType": 1,
				},
				"docLibName": part,
				"path":       path.Join(currentPath, currentID),
			}
			fmt.Printf("Debug - Creating directory with data: %+v\n", data)
			
			bodyBytes, err := json.Marshal(data)
			if err != nil {
				return fmt.Errorf("failed to marshal request body: %v", err)
			}
			
			opts := &rest.Opts{
				Method:     "POST",
				Path:       "/orchestration/familyCloud-rebuild/cloudCatalog/v1.0/createCloudDoc",
				Body:       bytes.NewReader(bodyBytes),
				NoResponse: true,
			}
			_, err = f.request(ctx, opts)
			if err != nil {
				return fmt.Errorf("failed to create directory %q: %v", currentPath, err)
			}

			// 获取新创建的目录的 ID
			entries, err := f.List(ctx, currentPath)
			if err != nil {
				return fmt.Errorf("failed to list directory after creation: %v", err)
			}
			for _, entry := range entries {
				if entry.Remote() == currentPath {
					if d, ok := entry.(fs.Directory); ok {
						currentID = d.ID()
						f.dirCache[currentPath] = currentID
						fmt.Printf("Debug - Created directory %q with ID: %q\n", currentPath, currentID)
						break
					}
				}
			}

		case MetaGroup:
			// 群组云 API
			data := map[string]interface{}{
				"catalogName": part,
				"commonAccountInfo": map[string]interface{}{
					"account":     f.account,
					"accountType": 1,
				},
				"groupID":      f.opt.CloudID,
				"parentFileId": currentID,
				"path":         path.Join(currentPath, currentID),
			}
			fmt.Printf("Debug - Creating directory with data: %+v\n", data)
			
			bodyBytes, err := json.Marshal(data)
			if err != nil {
				return fmt.Errorf("failed to marshal request body: %v", err)
			}
			
			opts := &rest.Opts{
				Method:     "POST",
				Path:       "/orchestration/group-rebuild/catalog/v1.0/createGroupCatalog",
				Body:       bytes.NewReader(bodyBytes),
				NoResponse: true,
			}
			_, err = f.request(ctx, opts)
			if err != nil {
				return fmt.Errorf("failed to create directory %q: %v", currentPath, err)
			}

			// 获取新创建的目录的 ID
			entries, err := f.List(ctx, currentPath)
			if err != nil {
				return fmt.Errorf("failed to list directory after creation: %v", err)
			}
			for _, entry := range entries {
				if entry.Remote() == currentPath {
					if d, ok := entry.(fs.Directory); ok {
						currentID = d.ID()
						f.dirCache[currentPath] = currentID
						fmt.Printf("Debug - Created directory %q with ID: %q\n", currentPath, currentID)
						break
					}
				}
			}

		default:
			return fmt.Errorf("unsupported cloud type: %s", f.opt.CloudType)
		}
	}

	return nil
}

// Rmdir removes the directory (container, bucket) if empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	// TODO: Implement rmdir
	return nil
}

// Hashes returns the supported hash sets
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.MD5)
}

// Precision of the ModTimes in this Fs
func (f *Fs) Precision() time.Duration {
	return time.Second
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

// Hash returns the hash of an object
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
