package _139

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

// ParallelHashCtx 分片哈希上下文
type ParallelHashCtx struct {
	PartOffset int64 `json:"partOffset"`
}

// PartInfo 分片信息
type PartInfo struct {
	PartNumber      int64           `json:"partNumber"`
	PartSize        int64           `json:"partSize"`
	ParallelHashCtx ParallelHashCtx `json:"parallelHashCtx"`
}

// PersonalPartInfo 个人云分片信息
type PersonalPartInfo struct {
	PartNumber int    `json:"partNumber"`
	UploadUrl  string `json:"uploadUrl"`
}

// PersonalUploadResp 个人云上传响应
type PersonalUploadResp struct {
	BaseResp
	Data struct {
		FileId      string             `json:"fileId"`
		FileName    string             `json:"fileName"`
		PartInfos   []PersonalPartInfo `json:"partInfos"`
		Exist       bool               `json:"exist"`
		RapidUpload bool               `json:"rapidUpload"`
		UploadId    string             `json:"uploadId"`
	} `json:"data"`
}

// PersonalUploadUrlResp 个人云获取上传地址响应
type PersonalUploadUrlResp struct {
	BaseResp
	Data struct {
		FileId    string             `json:"fileId"`
		UploadId  string             `json:"uploadId"`
		PartInfos []PersonalPartInfo `json:"partInfos"`
	} `json:"data"`
}

// encodeURIComponent 对字符串进行 URL 编码
func encodeURIComponent(str string) string {
	var result strings.Builder
	for i := 0; i < len(str); i++ {
		c := str[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '!' || c == '~' || c == '*' || c == '\'' || c == '(' || c == ')' {
			result.WriteByte(c)
		} else {
			fmt.Fprintf(&result, "%%%02X", c)
		}
	}
	return result.String()
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
	// 获取目录ID
	dirID, err := f.getDirID(ctx, dir, "")
	if err != nil {
		return nil, fmt.Errorf("failed to get directory ID: %v", err)
	}

	// 获取目录内容
	items, err := f.listDir(ctx, dirID)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory: %v", err)
	}

	// 转换为 DirEntries
	for _, item := range items {
		remote := path.Join(dir, item.Name)
		if item.IsDirectory {
			entries = append(entries, fs.NewDir(remote, time.Time{}))
		} else {
			entries = append(entries, &Object{
				fs:          f,
				remote:      remote,
				id:          item.ID,
				modTime:     time.Time{},
				size:        item.Size,
				hash:        item.Hash,
				mimeType:    item.MimeType,
				isDirectory: false,
			})
		}
	}

	return entries, nil
}

// getRootFolderID 获取根目录 ID
func (f *Fs) getRootFolderID(ctx context.Context) (string, error) {
	if f.opt.RootFolderID != "" {
		return f.opt.RootFolderID, nil
	}

	if f.opt.CloudType == "personal_new" {
		f.opt.RootFolderID = "/"
	} else if f.opt.CloudType == "personal" {
		f.opt.RootFolderID = "root"
	} else if f.opt.CloudType == "group" {
		if f.opt.CloudID == "" {
			return "", fmt.Errorf("cloud_id is required for group cloud type")
		}
		f.opt.RootFolderID = f.opt.CloudID
	} else if f.opt.CloudType == "family" {
		// 家庭云不需要特殊处理
	} else {
		return "", fmt.Errorf("unsupported cloud type: %s", f.opt.CloudType)
	}

	return f.opt.RootFolderID, nil
}

// getDirID 获取目录 ID
func (f *Fs) getDirID(ctx context.Context, dir string, destPath string) (string, error) {
	// 如果目录为空，返回根目录ID
	if dir == "" {
		return f.getRootFolderID(ctx)
	}

	// 检查缓存
	if id, ok := f.dirCache[dir]; ok {
		return id, nil
	}

	// 分割路径
	cleanParts := strings.Split(strings.Trim(dir, "/"), "/")
	currentID := f.opt.RootFolderID
	currentPath := ""

	// 逐级查找目录
	for i, part := range cleanParts {
		// 跳过空目录名
		if part == "" {
			continue
		}

		if currentPath == "" {
			currentPath = part
		} else {
			currentPath = currentPath + "/" + part
		}

		// 获取当前目录下的文件列表
		items, err := f.listDir(ctx, currentID)
		if err != nil {
			return "", fmt.Errorf("failed to list directory %q: %v", currentPath, err)
		}

		// 查找目标目录
		found := false
		for _, item := range items {
			if item.IsDirectory && item.Name == part {
				currentID = item.ID
				f.dirCache[currentPath] = currentID
				found = true
				break
			}
		}

		if !found {
			// 如果是在目标路径中，创建新目录
			if destPath != "" {
				// 目录不存在，创建新目录
				createData := map[string]interface{}{
					"parentFileId": currentID,
					"name":         part,
					"type":         "folder",
				}

				var createResp struct {
					BaseResp
					Data struct {
						FileId string `json:"fileId"`
					} `json:"data"`
				}

				_, err := f.personalPost(ctx, "/hcy/file/create", createData, &createResp)
				if err != nil {
					return "", fmt.Errorf("create dir failed: %v", err)
				}

				if !createResp.Success {
					return "", fmt.Errorf("create dir failed: %s", createResp.Message)
				}

				currentID = createResp.Data.FileId
				f.dirCache[currentPath] = currentID
			} else {
				// 如果不在目标路径中，返回错误
				return "", fmt.Errorf("directory not found: %s", currentPath)
			}
		}

		// 如果是最后一个部分，确保更新缓存
		if i == len(cleanParts)-1 {
			f.dirCache[dir] = currentID
		}
	}

	return currentID, nil
}

// listDir lists the directory contents
func (f *Fs) listDir(ctx context.Context, dirID string) ([]FileItem, error) {
	if dirID == "" {
		dirID = "/"
	}

	data := map[string]interface{}{
		"parentFileId": dirID,
		"pageInfo": map[string]interface{}{
			"pageSize":    100,
			"pageCursor":  "",
		},
		"orderBy":        "updated_at",
		"orderDirection": "DESC",
	}

	var resp PersonalListResp
	_, err := f.personalPost(ctx, "/hcy/file/list", data, &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory: %v", err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("failed to list directory: %s", resp.Message)
	}

	// 转换 PersonalFileItem 到 FileItem
	items := make([]FileItem, 0, len(resp.Data.Items))
	for _, item := range resp.Data.Items {
		fileItem := FileItem{
			ID:          item.FileId,
			Name:        item.Name,
			Size:        item.Size,
			Type:        item.Type,
			CreatedAt:   item.CreatedAt,
			UpdatedAt:   item.UpdatedAt,
			IsDirectory: item.Type == "folder",
			Hash:        "",
			MimeType:    "",
			Path:        "",
		}
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
//	fmt.Printf("Decoded token: %s\n", decodeStr)

	splits := strings.Split(decodeStr, ":")
	if len(splits) < 3 {
		return fmt.Errorf("authorization is invalid: expected format 'client_id:account:token|version|type|expire_time|...', got %d parts", len(splits))
	}

//	fmt.Printf("Token parts:\n")
//	fmt.Printf("  Client ID: %s\n", splits[0])
//	fmt.Printf("  Account: %s\n", splits[1])
//	fmt.Printf("  Token: %s\n", splits[2])

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

// Put implements fs.Fs
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// 获取目标路径
	destPath := f.root
	if destPath == "" {
		destPath = src.Remote()
	}

	// 获取源文件的相对路径
	srcPath := src.Remote()
	
	// 获取完整的远程路径，保持源文件的目录结构
	remotePath := path.Join(destPath, srcPath)
	
	// 获取父目录路径
	parentDir := path.Dir(remotePath)
	if parentDir == "." {
		parentDir = ""
	}

	// 分割父目录路径
	parentParts := strings.Split(strings.Trim(parentDir, "/"), "/")
	currentID := f.opt.RootFolderID

	// 逐级创建目录
	for _, part := range parentParts {
		if part == "" {
			continue
		}

		// 获取当前目录下的文件列表
		items, err := f.listDir(ctx, currentID)
		if err != nil {
			return nil, fmt.Errorf("failed to list directory: %v", err)
		}

		// 查找目录
		found := false
		for _, item := range items {
			if item.IsDirectory && item.Name == part {
				currentID = item.ID
				found = true
				break
			}
		}

		// 如果目录不存在，创建它
		if !found {
			createData := map[string]interface{}{
				"parentFileId": currentID,
				"name":         part,
				"type":         "folder",
			}

			var createResp struct {
				BaseResp
				Data struct {
					FileId string `json:"fileId"`
				} `json:"data"`
			}

			_, err := f.personalPost(ctx, "/hcy/file/create", createData, &createResp)
			if err != nil {
				return nil, fmt.Errorf("create dir failed: %v", err)
			}

			if !createResp.Success {
				return nil, fmt.Errorf("create dir failed: %s", createResp.Message)
			}

			currentID = createResp.Data.FileId
		}
	}

	// 获取文件名
	fileName := path.Base(remotePath)

	// 检查文件是否已存在
	items, err := f.listDir(ctx, currentID)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory: %v", err)
	}

	// 查找文件
	for _, item := range items {
		if !item.IsDirectory && item.Name == fileName {
			// 文件已存在，返回现有对象
			return &Object{
				fs:          f,
				remote:      remotePath,
				id:          item.ID,
				modTime:     time.Time{},
				size:        item.Size,
				hash:        item.Hash,
				mimeType:    item.MimeType,
				isDirectory: false,
			}, nil
		}
	}

	// 文件不存在，创建新文件
	// ... 继续原有的文件上传逻辑 ...

	// 计算文件哈希
	var fullHash string
	if hash, err := src.Hash(ctx, hash.SHA256); err == nil && hash != "" {
		fullHash = hash
	} else {
		// 如果没有哈希,需要计算
		hash := sha256.New()
		_, err := io.Copy(hash, in)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate file hash: %v", err)
		}
		fullHash = hex.EncodeToString(hash.Sum(nil))
		// 重置 reader
		if seeker, ok := in.(io.Seeker); ok {
			_, err = seeker.Seek(0, io.SeekStart)
			if err != nil {
				return nil, fmt.Errorf("failed to reset reader: %v", err)
			}
		} else {
			return nil, fmt.Errorf("reader does not support seeking")
		}
	}

	// 计算分片信息
	partSize := f.getPartSize(src.Size())
	part := (src.Size() + partSize - 1) / partSize
	if part == 0 {
		part = 1
	}

	partInfos := make([]PartInfo, 0, part)
	for i := int64(0); i < part; i++ {
		start := i * partSize
		byteSize := src.Size() - start
		if byteSize > partSize {
			byteSize = partSize
		}
		partNumber := i + 1
		partInfo := PartInfo{
			PartNumber: partNumber,
			PartSize:   byteSize,
			ParallelHashCtx: ParallelHashCtx{
				PartOffset: start,
			},
		}
		partInfos = append(partInfos, partInfo)
	}

	// 筛选出前 100 个 partInfos
	firstPartInfos := partInfos
	if len(firstPartInfos) > 100 {
		firstPartInfos = firstPartInfos[:100]
	}

	// 创建上传任务
	data := map[string]interface{}{
		"contentHash":          fullHash,
		"contentHashAlgorithm": "SHA256",
		"contentType":          "application/oct-stream",
		"parallelUpload":       false,
		"partInfos":            firstPartInfos,
		"size":                 src.Size(),
		"parentFileId":         currentID,
		"name":                 fileName,
		"type":                 "file",
		"fileRenameMode":       "auto_rename",
	}

	opts := rest.Opts{
		Method: "POST",
		Path:   "/hcy/file/create",
		Body:   bytes.NewReader(mustJSON(data)),
	}

	var resp PersonalUploadResp
	result, err := f.request(ctx, &opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create upload task: %v", err)
	}

	err = json.Unmarshal(result, &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse upload response: %v", err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("failed to create upload task: %s", resp.Message)
	}

	// 如果文件已存在且支持秒传
	if resp.Data.Exist {
		return &Object{
			fs:          f,
			remote:      remotePath,
			id:          resp.Data.FileId,
			modTime:     src.ModTime(ctx),
			size:        src.Size(),
			isDirectory: false,
		}, nil
	}

	// 获取上传地址
	uploadPartInfos := resp.Data.PartInfos

	// 获取后续分片的上传地址
	for i := 101; i < len(partInfos); i += 100 {
		end := i + 100
		if end > len(partInfos) {
			end = len(partInfos)
		}
		batchPartInfos := partInfos[i:end]

		uploadData := map[string]interface{}{
			"fileId":   resp.Data.FileId,
			"uploadId": resp.Data.UploadId,
			"partInfos": batchPartInfos,
		}

		opts = rest.Opts{
			Method: "POST",
			Path:   "/hcy/file/getUploadUrl",
			Body:   bytes.NewReader(mustJSON(uploadData)),
		}

		var uploadUrlResp PersonalUploadUrlResp
		result, err = f.request(ctx, &opts)
		if err != nil {
			return nil, fmt.Errorf("failed to get upload URL: %v", err)
		}

		err = json.Unmarshal(result, &uploadUrlResp)
		if err != nil {
			return nil, fmt.Errorf("failed to parse upload URL response: %v", err)
		}

		if !uploadUrlResp.Success {
			return nil, fmt.Errorf("failed to get upload URL: %s", uploadUrlResp.Message)
		}

		uploadPartInfos = append(uploadPartInfos, uploadUrlResp.Data.PartInfos...)
	}

	// 只有在成功获取到分片上传地址并且上传了分片后，才会调用完成上传的接口
	if len(uploadPartInfos) > 0 {
		// 分片上传
		for i, uploadPartInfo := range uploadPartInfos {
			index := uploadPartInfo.PartNumber - 1
			partSize := partInfos[index].PartSize

			// 读取分片数据
			partData := make([]byte, partSize)
			n, err := io.ReadFull(in, partData)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				return nil, fmt.Errorf("failed to read part %d: %v", i+1, err)
			}
			partData = partData[:n]

			// 创建 HTTP 请求
			req, err := http.NewRequestWithContext(ctx, "PUT", uploadPartInfo.UploadUrl, bytes.NewReader(partData))
			if err != nil {
				return nil, fmt.Errorf("failed to create request for part %d: %v", i+1, err)
			}

			// 设置请求头
			req.Header.Set("Content-Type", "application/octet-stream")
			req.Header.Set("Content-Length", fmt.Sprint(partSize))
			req.Header.Set("Origin", "https://yun.139.com")
			req.Header.Set("Referer", "https://yun.139.com/")

			// 发送请求
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return nil, fmt.Errorf("failed to upload part %d: %v", i+1, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("failed to upload part %d: unexpected status code %d", i+1, resp.StatusCode)
			}
		}

		// 完成上传
		completeData := map[string]interface{}{
			"contentHash":          fullHash,
			"contentHashAlgorithm": "SHA256",
			"fileId":               resp.Data.FileId,
			"uploadId":             resp.Data.UploadId,
		}

		opts = rest.Opts{
			Method: "POST",
			Path:   "/hcy/file/complete",
			Body:   bytes.NewReader(mustJSON(completeData)),
		}

		var completeResp BaseResp
		result, err = f.request(ctx, &opts)
		if err != nil {
			return nil, fmt.Errorf("failed to complete upload: %v", err)
		}

		err = json.Unmarshal(result, &completeResp)
		if err != nil {
			return nil, fmt.Errorf("failed to parse complete response: %v", err)
		}

		if !completeResp.Success {
			return nil, fmt.Errorf("failed to complete upload: %s", completeResp.Message)
		}
	}

	// 创建对象并返回
	return &Object{
		fs:          f,
		remote:      remotePath,
		id:          resp.Data.FileId,
		modTime:     src.ModTime(ctx),
		size:        src.Size(),
		isDirectory: false,
	}, nil
}

// getPartSize 计算分片大小
func (f *Fs) getPartSize(size int64) int64 {
	// 默认分片大小为 100MB
	partSize := int64(100 * 1024 * 1024)
	// 如果文件大于 30GB，使用 512MB 的分片大小
	if size > 30*1024*1024*1024 {
		partSize = int64(512 * 1024 * 1024)
	}
	return partSize
}

// mustJSON 将数据转换为 JSON 字节
func mustJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// Mkdir creates a directory
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	// 如果 dir 为空，使用 f.root
	if dir == "" {
		dir = f.root
	}

	// 分割路径
	parts := strings.Split(dir, "/")

	// 当前目录 ID，初始为根目录 ID
	currentDirID := f.opt.RootFolderID
	if currentDirID == "" {
		currentDirID = "/"
	}

	// 逐级创建目录
	for _, part := range parts {
		if part == "" {
			continue
		}

		// 检查目录是否已存在
		items, err := f.listDir(ctx, currentDirID)
		if err != nil {
			return fmt.Errorf("failed to list directory: %v", err)
		}

		// 查找目录
		var foundDirID string
		for _, item := range items {
			if item.Name == part && item.IsDirectory {
				foundDirID = item.ID
				break
			}
		}

		// 如果目录不存在，创建它
		if foundDirID == "" {
			// 根据云类型选择不同的创建目录 API
			var pathname string
			var data map[string]interface{}

			switch f.opt.CloudType {
			case "personal_new":
				pathname = "/hcy/file/create"
				data = map[string]interface{}{
					"name":            part,
					"parentFileId":    currentDirID,
					"type":            "folder",
					"fileRenameMode":  "force_rename",
					"description":     "",
				}
			case "personal":
				pathname = "/orchestration/personalCloud/catalog/v1.0/createCatalogExt"
				data = map[string]interface{}{
					"catalogName":     part,
					"parentCatalogID": currentDirID,
				}
			case "family":
				pathname = "/orchestration/familyCloud-rebuild/cloudCatalog/v1.0/createCloudDoc"
				data = map[string]interface{}{
					"catalogName":     part,
					"parentCatalogID": currentDirID,
				}
			case "group":
				pathname = "/orchestration/group-rebuild/catalog/v1.0/createGroupCatalog"
				data = map[string]interface{}{
					"catalogName":     part,
					"parentCatalogID": currentDirID,
				}
			default:
				return fmt.Errorf("unsupported cloud type: %s", f.opt.CloudType)
			}

			// 设置基础 URL
			f.srv.SetRoot("https://personal-kd-njs.yun.139.com")

			// 将请求体转换为 JSON
			bodyBytes, err := json.Marshal(data)
			if err != nil {
				return fmt.Errorf("failed to marshal request body: %v", err)
			}

			// 发送请求
			opts := rest.Opts{
				Method: "POST",
				Path:   pathname,
				Body:   bytes.NewReader(bodyBytes),
			}

			var resp BaseResp
			result, err := f.request(ctx, &opts)
			if err != nil {
				return fmt.Errorf("failed to create directory %q: %v", part, err)
			}

			// 解析响应
			err = json.Unmarshal(result, &resp)
			if err != nil {
				return fmt.Errorf("failed to parse response: %v (response: %s)", err, string(result))
			}

			if !resp.Success {
				return fmt.Errorf("failed to create directory %q: %s", part, resp.Message)
			}

			// 获取新创建的目录 ID
			var newDirID string
			if f.opt.CloudType == "personal_new" {
				var createResp struct {
					BaseResp
					Data struct {
						FileId string `json:"fileId"`
					} `json:"data"`
				}
				err = json.Unmarshal(result, &createResp)
				if err != nil {
					return fmt.Errorf("failed to parse create response: %v", err)
				}
				newDirID = createResp.Data.FileId
			} else {
				var createResp struct {
					BaseResp
					Data struct {
						CatalogID string `json:"catalogID"`
					} `json:"data"`
				}
				err = json.Unmarshal(result, &createResp)
				if err != nil {
					return fmt.Errorf("failed to parse create response: %v", err)
				}
				newDirID = createResp.Data.CatalogID
			}

			if newDirID == "" {
				return fmt.Errorf("failed to get new directory ID for %q", part)
			}

			foundDirID = newDirID
		}

		// 更新当前目录 ID 为刚找到或创建的目录 ID
		currentDirID = foundDirID
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

// personalPost 发送 POST 请求到个人云 API
func (f *Fs) personalPost(ctx context.Context, pathname string, data interface{}, resp interface{}) ([]byte, error) {
	bodyBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %v", err)
	}

	opts := rest.Opts{
		Method: "POST",
		Path:   pathname,
		Body:   bytes.NewReader(bodyBytes),
		ExtraHeaders: map[string]string{
			"Content-Type": "application/json",
			"Origin":      "https://yun.139.com",
			"Referer":     "https://yun.139.com/",
		},
	}

	result, err := f.request(ctx, &opts)
	if err != nil {
		return nil, err
	}

	if resp != nil {
		err = json.Unmarshal(result, resp)
		if err != nil {
			return nil, fmt.Errorf("failed to parse response: %v", err)
		}
	}

	return result, nil
}

// IsDir returns true if the object is a directory
func (o *Object) IsDir() bool {
	return o.isDirectory
}
