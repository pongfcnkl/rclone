package open123

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/lib/rest"
)

const (
	apiBaseURL     = "https://open-api.123pan.com"
	accessTokenURL = apiBaseURL + "/api/v1/access_token"
)

type tokenManager struct {
	mu           sync.Mutex
	expiredAt    time.Time
	blockRefresh bool
}

type BaseResp struct {
	Code     int    `json:"code"`
	Message  string `json:"message"`
	XTraceID string `json:"x-traceID"`
}

type AccessTokenResp struct {
	BaseResp
	Data struct {
		AccessToken string `json:"accessToken"`
		ExpiredAt   string `json:"expiredAt"`
	} `json:"data"`
}

type RefreshTokenResp struct {
	AccessToken      string `json:"access_token"`
	RefreshToken     string `json:"refresh_token"`
	ExpiresIn        int64  `json:"expires_in"`
	Code             int    `json:"code"`
	Message          string `json:"message"`
	ErrorDescription string `json:"error_description"`
	Error            string `json:"error"`
	Text             string `json:"text"`
}

type File struct {
	FileName string `json:"filename"`
	Size     int64  `json:"size"`
	CreateAt string `json:"createAt"`
	UpdateAt string `json:"updateAt"`
	FileID   int64  `json:"fileId"`
	Type     int    `json:"type"`
	Etag     string `json:"etag"`
	Trashed  int    `json:"trashed"`
	SHA1     string `json:"-"`
}

func (f File) CreateTime() time.Time {
	return parseAPITime(f.CreateAt)
}

func (f File) ModTime() time.Time {
	return parseAPITime(f.UpdateAt)
}

type UserInfoResp struct {
	BaseResp
	Data struct {
		UID            uint64 `json:"uid"`
		SpaceUsed      int64  `json:"spaceUsed"`
		SpacePermanent int64  `json:"spacePermanent"`
		SpaceTemp      int64  `json:"spaceTemp"`
	} `json:"data"`
}

type FileListResp struct {
	BaseResp
	Data struct {
		LastFileID int64  `json:"lastFileId"`
		FileList   []File `json:"fileList"`
	} `json:"data"`
}

type DownloadInfoResp struct {
	BaseResp
	Data struct {
		DownloadURL string `json:"downloadUrl"`
	} `json:"data"`
}

type DirectLinkResp struct {
	BaseResp
	Data struct {
		URL string `json:"url"`
	} `json:"data"`
}

type UploadCreateResp struct {
	BaseResp
	Data struct {
		FileID      int64    `json:"fileID"`
		PreuploadID string   `json:"preuploadID"`
		Reuse       bool     `json:"reuse"`
		SliceSize   int64    `json:"sliceSize"`
		Servers     []string `json:"servers"`
	} `json:"data"`
}

type UploadCompleteResp struct {
	BaseResp
	Data struct {
		Completed bool  `json:"completed"`
		FileID    int64 `json:"fileID"`
	} `json:"data"`
}

type SHA1ReuseResp struct {
	BaseResp
	Data struct {
		FileID int64 `json:"fileID"`
		Reuse  bool  `json:"reuse"`
	} `json:"data"`
}

func parseAPITime(value string) time.Time {
	loc := time.FixedZone("UTC+8", 8*60*60)
	t, err := time.ParseInLocation("2006-01-02 15:04:05", value, loc)
	if err != nil {
		return time.Time{}
	}
	return t
}

func expiresInToExpiredAt(expiresIn int64) (time.Time, error) {
	if expiresIn <= 0 {
		return time.Time{}, errors.New("invalid expires_in from renew API")
	}
	return time.Now().UTC().Add(time.Duration(expiresIn) * time.Second), nil
}

func (f *Fs) getAccessToken(ctx context.Context, forceRefresh bool) (string, error) {
	f.tm.mu.Lock()
	defer f.tm.mu.Unlock()
	if f.tm.blockRefresh {
		return "", errors.New("authentication expired")
	}
	if !forceRefresh && f.opt.AccessToken != "" {
		// Cached tokens from config may not carry an expiry timestamp. Treat a zero
		// timestamp as "usable until proven otherwise" and refresh only after a 401.
		if f.tm.expiredAt.IsZero() || time.Now().Before(f.tm.expiredAt.Add(-5*time.Minute)) {
			return f.opt.AccessToken, nil
		}
	}
	if !forceRefresh && f.opt.AccessToken != "" && time.Now().Before(f.tm.expiredAt.Add(-5*time.Minute)) {
		return f.opt.AccessToken, nil
	}
	if err := f.refreshAccessToken(ctx); err != nil {
		f.tm.blockRefresh = true
		return "", err
	}
	return f.opt.AccessToken, nil
}

func (f *Fs) refreshAccessToken(ctx context.Context) error {
	if f.opt.UseOnlineAPI && f.opt.RefreshToken != "" && f.opt.APIURLAddress != "" {
		var resp RefreshTokenResp
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.opt.APIURLAddress, nil)
		if err != nil {
			return err
		}
		q := req.URL.Query()
		q.Set("refresh_ui", f.opt.RefreshToken)
		q.Set("server_use", "true")
		q.Set("driver_txt", "123cloud_oa")
		req.URL.RawQuery = q.Encode()
		httpResp, err := f.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer httpResp.Body.Close()
		body, err := io.ReadAll(httpResp.Body)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(body, &resp); err != nil {
			return err
		}
		if resp.AccessToken == "" || resp.RefreshToken == "" {
			msg := resp.ErrorDescription
			if msg == "" {
				msg = resp.Text
			}
			if msg == "" {
				msg = resp.Message
			}
			if msg == "" {
				msg = resp.Error
			}
			if msg == "" {
				msg = string(body)
			}
			return fmt.Errorf("failed to refresh token: %s", msg)
		}
		expiredAt, err := expiresInToExpiredAt(resp.ExpiresIn)
		if err != nil {
			return err
		}
		f.opt.AccessToken = resp.AccessToken
		f.opt.RefreshToken = resp.RefreshToken
		f.tm.expiredAt = expiredAt
		f.tm.blockRefresh = false
		_ = config.SetValueAndSave(f.originalName, "access_token", resp.AccessToken)
		_ = config.SetValueAndSave(f.originalName, "refresh_token", resp.RefreshToken)
		return nil
	}
	if f.opt.ClientID != "" && f.opt.ClientSecret != "" {
		payload := map[string]string{
			"clientID":     f.opt.ClientID,
			"clientSecret": f.opt.ClientSecret,
		}
		bodyBytes, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, accessTokenURL, strings.NewReader(string(bodyBytes)))
		if err != nil {
			return err
		}
		req.Header.Set("Platform", "open_platform")
		req.Header.Set("Content-Type", "application/json")
		httpResp, err := f.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer httpResp.Body.Close()
		body, err := io.ReadAll(httpResp.Body)
		if err != nil {
			return err
		}
		var resp AccessTokenResp
		if err = json.Unmarshal(body, &resp); err != nil {
			return err
		}
		if resp.Code != 0 {
			return fmt.Errorf("get access token failed: %s", resp.Message)
		}
		if resp.Data.AccessToken == "" || resp.Data.ExpiredAt == "" {
			return errors.New("invalid token payload from developer API")
		}
		expiredAt, err := time.Parse(time.RFC3339, resp.Data.ExpiredAt)
		if err != nil {
			return fmt.Errorf("parse expire time failed: %w", err)
		}
		f.opt.AccessToken = resp.Data.AccessToken
		f.tm.expiredAt = expiredAt.UTC()
		f.tm.blockRefresh = false
		_ = config.SetValueAndSave(f.originalName, "access_token", resp.Data.AccessToken)
		return nil
	}
	if f.opt.AccessToken != "" {
		if f.tm.expiredAt.IsZero() {
			f.tm.expiredAt = time.Time{}
		}
		f.tm.blockRefresh = false
		return nil
	}
	return errors.New("no valid authentication method available")
}

func (f *Fs) call(ctx context.Context, method, endpoint string, params url.Values, body any, out any) error {
	for {
		token, err := f.getAccessToken(ctx, false)
		if err != nil {
			return err
		}
		opts := &rest.Opts{
			Method:     method,
			RootURL:    strings.TrimRight(apiBaseURL, "/") + endpoint,
			Parameters: params,
			ExtraHeaders: map[string]string{
				"Authorization": "Bearer " + token,
				"Platform":      "open_platform",
				"Content-Type":  "application/json",
			},
		}
		if body != nil {
			buf, err := json.Marshal(body)
			if err != nil {
				return err
			}
			opts.Body = strings.NewReader(string(buf))
		}
		resp, err := f.client.Call(ctx, opts)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		var baseResp BaseResp
		if err = json.Unmarshal(data, &baseResp); err != nil {
			return err
		}
		switch baseResp.Code {
		case 0:
			if out != nil {
				if err = json.Unmarshal(data, out); err != nil {
					return err
				}
			}
			return nil
		case 401:
			if _, err = f.getAccessToken(ctx, true); err != nil {
				return err
			}
		case 429:
			time.Sleep(500 * time.Millisecond)
		default:
			if baseResp.Message == "" {
				baseResp.Message = string(data)
			}
			return errors.New(baseResp.Message)
		}
	}
}

func (f *Fs) apiUserInfo(ctx context.Context) (*UserInfoResp, error) {
	var resp UserInfoResp
	if err := f.call(ctx, http.MethodGet, "/api/v1/user/info", nil, nil, &resp); err != nil {
		return nil, err
	}
	f.uid = resp.Data.UID
	return &resp, nil
}

func (f *Fs) getUID(ctx context.Context) (uint64, error) {
	if f.uid != 0 {
		return f.uid, nil
	}
	info, err := f.apiUserInfo(ctx)
	if err != nil {
		return 0, err
	}
	return info.Data.UID, nil
}

func (f *Fs) apiListFiles(ctx context.Context, parentFileID int64) ([]File, error) {
	lastFileID := int64(0)
	files := make([]File, 0)
	for lastFileID != -1 {
		var resp FileListResp
		params := url.Values{
			"parentFileId": {strconv.FormatInt(parentFileID, 10)},
			"limit":        {"100"},
			"lastFileId":   {strconv.FormatInt(lastFileID, 10)},
			"trashed":      {"false"},
			"searchMode":   {""},
			"searchData":   {""},
		}
		if err := f.call(ctx, http.MethodGet, "/api/v2/file/list", params, nil, &resp); err != nil {
			return nil, err
		}
		for _, item := range resp.Data.FileList {
			if item.Trashed == 0 {
				files = append(files, item)
			}
		}
		lastFileID = resp.Data.LastFileID
	}
	return files, nil
}

func (f *Fs) apiDownloadInfo(ctx context.Context, fileID int64) (string, error) {
	var resp DownloadInfoResp
	if err := f.call(ctx, http.MethodGet, "/api/v1/file/download_info", url.Values{
		"fileId": {strconv.FormatInt(fileID, 10)},
	}, nil, &resp); err != nil {
		return "", err
	}
	return resp.Data.DownloadURL, nil
}

func (f *Fs) apiDirectLink(ctx context.Context, fileID int64) (string, error) {
	var resp DirectLinkResp
	if err := f.call(ctx, http.MethodGet, "/api/v1/direct-link/url", url.Values{
		"fileID": {strconv.FormatInt(fileID, 10)},
	}, nil, &resp); err != nil {
		return "", err
	}
	return resp.Data.URL, nil
}

func (f *Fs) apiMkdir(ctx context.Context, parentID int64, name string) error {
	return f.call(ctx, http.MethodPost, "/upload/v1/file/mkdir", nil, map[string]any{
		"parentID": strconv.FormatInt(parentID, 10),
		"name":     name,
	}, nil)
}

func (f *Fs) apiMove(ctx context.Context, fileID, toParentFileID int64) error {
	return f.call(ctx, http.MethodPost, "/api/v1/file/move", nil, map[string]any{
		"fileIDs":        []int64{fileID},
		"toParentFileID": toParentFileID,
	}, nil)
}

func (f *Fs) apiRename(ctx context.Context, fileID int64, newName string) error {
	return f.call(ctx, http.MethodPut, "/api/v1/file/name", nil, map[string]any{
		"fileId":   fileID,
		"fileName": newName,
	}, nil)
}

func (f *Fs) apiTrash(ctx context.Context, fileID int64) error {
	return f.call(ctx, http.MethodPost, "/api/v1/file/trash", nil, map[string]any{
		"fileIDs": []int64{fileID},
	}, nil)
}

func (f *Fs) apiCreateFile(ctx context.Context, parentFileID int64, filename, etag string, size int64, duplicate int, containDir bool) (*UploadCreateResp, error) {
	var resp UploadCreateResp
	err := f.call(ctx, http.MethodPost, "/upload/v2/file/create", nil, map[string]any{
		"parentFileId": parentFileID,
		"filename":     filename,
		"etag":         strings.ToLower(etag),
		"size":         size,
		"duplicate":    duplicate,
		"containDir":   containDir,
	}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (f *Fs) apiCompleteUpload(ctx context.Context, preuploadID string) (*UploadCompleteResp, error) {
	var resp UploadCompleteResp
	err := f.call(ctx, http.MethodPost, "/upload/v2/file/upload_complete", nil, map[string]any{
		"preuploadID": preuploadID,
	}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (f *Fs) apiSHA1Reuse(ctx context.Context, parentFileID int64, filename, sha1Hash string, size int64, duplicate int) (*SHA1ReuseResp, error) {
	var resp SHA1ReuseResp
	err := f.call(ctx, http.MethodPost, "/upload/v2/file/sha1_reuse", nil, map[string]any{
		"parentFileID": parentFileID,
		"filename":     filename,
		"sha1":         strings.ToLower(sha1Hash),
		"size":         size,
		"duplicate":    duplicate,
	}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func signURL(originURL, privateKey string, uid uint64, validDuration time.Duration) (string, error) {
	ts := time.Now().Add(validDuration).Unix()
	randPart := strings.ReplaceAll(uuid.New().String(), "-", "")
	objURL, err := url.Parse(originURL)
	if err != nil {
		return "", err
	}
	unsigned := fmt.Sprintf("%s-%d-%s-%d-%s", objURL.Path, ts, randPart, uid, privateKey)
	sum := md5.Sum([]byte(unsigned))
	authKey := fmt.Sprintf("%d-%s-%d-%x", ts, randPart, uid, sum)
	query := objURL.Query()
	query.Set("auth_key", authKey)
	objURL.RawQuery = query.Encode()
	return objURL.String(), nil
}
