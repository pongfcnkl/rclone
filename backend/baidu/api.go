package baidu

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/rclone/rclone/backend/baidu/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/rest"
)

func (f *Fs) refreshToken(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	form := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {f.opt.RefreshToken},
		"client_id":     {f.opt.ClientID},
		"client_secret": {f.opt.ClientSecret},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, openAPIURL+"/oauth/2.0/token?"+form.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var out api.TokenResp
	if err = json.Unmarshal(body, &out); err != nil {
		return err
	}
	if out.AccessToken == "" || out.RefreshToken == "" {
		return fmt.Errorf("baidu: refresh token returned empty tokens: %s", string(body))
	}
	f.opt.AccessToken = out.AccessToken
	f.opt.RefreshToken = out.RefreshToken
	_ = config.SetValueAndSave(f.originalName, "access_token", out.AccessToken)
	_ = config.SetValueAndSave(f.originalName, "refresh_token", out.RefreshToken)
	return nil
}

func (f *Fs) call(ctx context.Context, method, pathOrURL string, params, form url.Values, out any) ([]byte, error) {
	var bodyBytes []byte
	err := f.pacer.Call(func() (bool, error) {
		rootURL := pathOrURL
		if !strings.HasPrefix(rootURL, "http") {
			rootURL = strings.TrimRight(baseURL, "/") + pathOrURL
		}
		reqOpts := &rest.Opts{
			Method:     method,
			RootURL:    rootURL,
			Parameters: url.Values{},
			Options: []fs.OpenOption{
				&fs.HTTPOption{Key: "User-Agent", Value: defaultUserAgent},
				&fs.HTTPOption{Key: "Referer", Value: "https://pan.baidu.com/"},
				&fs.HTTPOption{Key: "Origin", Value: "https://pan.baidu.com"},
				&fs.HTTPOption{Key: "Accept", Value: "application/json, text/plain, */*"},
				&fs.HTTPOption{Key: "X-Requested-With", Value: "XMLHttpRequest"},
			},
		}
		for k, v := range params {
			for _, item := range v {
				reqOpts.Parameters.Add(k, item)
			}
		}
		reqOpts.Parameters.Set("access_token", f.opt.AccessToken)
		if form != nil {
			reqOpts.Body = bytes.NewBufferString(form.Encode())
			reqOpts.ContentType = "application/x-www-form-urlencoded"
		}
		resp, err := f.client.Call(ctx, reqOpts)
		if err != nil {
			return shouldRetry(ctx, resp, err)
		}
		defer resp.Body.Close()
		bodyBytes, err = io.ReadAll(resp.Body)
		if err != nil {
			return shouldRetry(ctx, resp, err)
		}
		var errno struct {
			Errno int `json:"errno"`
		}
		_ = json.Unmarshal(bodyBytes, &errno)
		if errno.Errno != 0 {
			fs.Debugf(f, "baidu api error: method=%s url=%s errno=%d body=%s", method, rootURL, errno.Errno, string(bodyBytes))
		}
		if errno.Errno == 111 || errno.Errno == -6 {
			if refreshErr := f.refreshToken(ctx); refreshErr != nil {
				return false, refreshErr
			}
			return true, fmt.Errorf("baidu: token refreshed")
		}
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return shouldRetry(ctx, resp, fmt.Errorf("baidu: http %d: %s", resp.StatusCode, string(bodyBytes)))
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	if out != nil {
		if err := json.Unmarshal(bodyBytes, out); err != nil {
			return nil, err
		}
		if apiErr, ok := out.(api.ErrorInterface); ok {
			if err := apiErr.Err(); err != nil {
				return nil, err
			}
		}
	}
	return bodyBytes, nil
}

func shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	if fserrors.ShouldRetry(err) {
		return true, err
	}
	if resp != nil {
		switch resp.StatusCode {
		case 408, 429, 500, 502, 503, 504:
			return true, err
		}
	}
	return false, err
}

func (f *Fs) apiUserInfo(ctx context.Context) (int, error) {
	var resp struct {
		api.ErrorAPI
		VipType int `json:"vip_type"`
	}
	_, err := f.call(ctx, http.MethodGet, "/rest/2.0/xpan/nas", url.Values{"method": {"uinfo"}}, nil, &resp)
	if err != nil {
		return 0, err
	}
	return resp.VipType, nil
}

func (f *Fs) apiList(ctx context.Context, dir string) ([]api.File, error) {
	start := 0
	limit := 1000
	params := url.Values{
		"method": {"list"},
		"dir":    {dir},
		"web":    {"web"},
	}
	if f.opt.OrderBy != "" {
		params.Set("order", f.opt.OrderBy)
		if strings.EqualFold(f.opt.OrderDirection, "desc") {
			params.Set("desc", "1")
		}
	}
	var items []api.File
	for {
		params.Set("start", strconv.Itoa(start))
		params.Set("limit", strconv.Itoa(limit))
		var resp api.ListResp
		if _, err := f.call(ctx, http.MethodGet, "/rest/2.0/xpan/file", params, nil, &resp); err != nil {
			return nil, err
		}
		items = append(items, resp.List...)
		if len(resp.List) < limit {
			break
		}
		start += limit
	}
	return items, nil
}

func (f *Fs) apiItemInfo(ctx context.Context, fullPath string, dlink bool) (*api.File, error) {
	var resp api.FileMetaResp
	params := url.Values{
		"target": {fmt.Sprintf("[\"%s\"]", fullPath)},
		"dlink":  {"0"},
	}
	if dlink {
		params.Set("dlink", "1")
	}
	_, err := f.call(ctx, http.MethodGet, "https://pan.baidu.com/api/filemetas", params, nil, &resp)
	if err != nil {
		return nil, err
	}
	if len(resp.Info) == 0 {
		return nil, fs.ErrorObjectNotFound
	}
	if err := resp.Info[0].Err(); err != nil {
		return nil, err
	}
	return &resp.Info[0].File, nil
}

func (f *Fs) apiDownloadLink(ctx context.Context, id int64) (string, error) {
	var resp api.DownloadResp
	params := url.Values{
		"method": {"filemetas"},
		"fsids":  {fmt.Sprintf("[%d]", id)},
		"dlink":  {"1"},
	}
	if _, err := f.call(ctx, http.MethodGet, "/rest/2.0/xpan/multimedia", params, nil, &resp); err != nil {
		return "", err
	}
	if len(resp.List) == 0 || resp.List[0].Dlink == "" {
		return "", fs.ErrorObjectNotFound
	}
	return resp.List[0].Dlink + "&access_token=" + url.QueryEscape(f.opt.AccessToken), nil
}

func (f *Fs) apiManage(ctx context.Context, opera string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	form := url.Values{
		"async":    {"0"},
		"ondup":    {"fail"},
		"filelist": {string(data)},
	}
	var resp api.ErrorAPI
	_, err = f.call(ctx, http.MethodPost, "/rest/2.0/xpan/file", url.Values{
		"method": {"filemanager"},
		"opera":  {opera},
	}, form, &resp)
	return err
}

func (f *Fs) apiDelete(ctx context.Context, paths []string) error {
	return f.apiManage(ctx, "delete", paths)
}

func (f *Fs) apiCreate(ctx context.Context, fullPath string, size int64, isDir int, uploadID, blockList string, mtime, ctime int64) error {
	form := url.Values{
		"path":  {fullPath},
		"size":  {strconv.FormatInt(size, 10)},
		"isdir": {strconv.Itoa(isDir)},
		"rtype": {"3"},
	}
	if uploadID != "" {
		form.Set("uploadid", uploadID)
	}
	if blockList != "" {
		form.Set("block_list", blockList)
	}
	if mtime > 0 {
		form.Set("local_mtime", strconv.FormatInt(mtime, 10))
		form.Set("local_ctime", strconv.FormatInt(ctime, 10))
	}
	fs.Debugf(f, "baidu create request: path=%q isdir=%d size=%d uploadid=%q block_list=%s", fullPath, isDir, size, uploadID, blockList)
	var resp api.ErrorAPI
	_, err := f.call(ctx, http.MethodPost, "/rest/2.0/xpan/file", url.Values{"method": {"create"}}, form, &resp)
	return err
}

func (f *Fs) apiQuota(ctx context.Context) (*api.QuotaResp, error) {
	var resp api.QuotaResp
	_, err := f.call(ctx, http.MethodGet, "https://pan.baidu.com/api/quota", nil, nil, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (f *Fs) getUploadURL(ctx context.Context, fullPath, uploadID string) (string, error) {
	if !f.opt.UseDynamicUploadAPI || uploadID == "" {
		return f.opt.UploadAPI, nil
	}
	var resp api.UploadServerResp
	_, err := f.call(ctx, http.MethodGet, "https://d.pcs.baidu.com/rest/2.0/pcs/file", url.Values{
		"method":         {"locateupload"},
		"appid":          {"250528"},
		"path":           {fullPath},
		"uploadid":       {uploadID},
		"upload_version": {"2.0"},
	}, nil, &resp)
	if err != nil {
		return f.opt.UploadAPI, nil
	}
	if len(resp.Servers) > 0 && resp.Servers[0].Server != "" {
		return resp.Servers[0].Server, nil
	}
	if len(resp.BakServers) > 0 && resp.BakServers[0].Server != "" {
		return resp.BakServers[0].Server, nil
	}
	return f.opt.UploadAPI, nil
}

func (f *Fs) apiPrecreate(ctx context.Context, fullPath string, size int64, blockList, contentMD5, sliceMD5 string, mtime, ctime int64) (*api.PrecreateResp, error) {
	form := url.Values{
		"path":       {fullPath},
		"size":       {strconv.FormatInt(size, 10)},
		"isdir":      {"0"},
		"autoinit":   {"1"},
		"rtype":      {"3"},
		"block_list": {blockList},
	}
	if contentMD5 != "" && sliceMD5 != "" {
		form.Set("content-md5", contentMD5)
		form.Set("slice-md5", sliceMD5)
	}
	if mtime > 0 {
		form.Set("local_mtime", strconv.FormatInt(mtime, 10))
		form.Set("local_ctime", strconv.FormatInt(ctime, 10))
	}
	fs.Debugf(f, "baidu precreate request: path=%q size=%d content_md5=%q slice_md5=%q block_list=%s", fullPath, size, contentMD5, sliceMD5, blockList)
	var resp api.PrecreateResp
	if _, err := f.call(ctx, http.MethodPost, "/rest/2.0/xpan/file", url.Values{"method": {"precreate"}}, form, &resp); err != nil {
		return nil, err
	}
	if resp.ReturnType == 1 {
		resp.UploadURL, _ = f.getUploadURL(ctx, fullPath, resp.UploadID)
	}
	return &resp, nil
}

func (f *Fs) getSliceSize(filesize int64) int64 {
	if f.vipType == 0 {
		return int64(defaultSliceSize)
	}
	custom := int64(f.opt.CustomUploadPartSize)
	if custom > 0 {
		maxSize := int64(vipSliceSize)
		if f.vipType >= 2 {
			maxSize = int64(svipSliceSize)
		}
		if custom < int64(defaultSliceSize) {
			return int64(defaultSliceSize)
		}
		if custom > maxSize {
			return maxSize
		}
		return custom
	}
	maxSize := int64(defaultSliceSize)
	if f.vipType == 1 {
		maxSize = int64(vipSliceSize)
	} else if f.vipType >= 2 {
		maxSize = int64(svipSliceSize)
	}
	if f.opt.LowBandwidthMode {
		size := int64(defaultSliceSize)
		for size <= maxSize {
			if filesize <= int64(maxSliceNum)*size {
				return size
			}
			size += int64(sliceStep)
		}
	}
	return maxSize
}

func (f *Fs) uploadSlice(ctx context.Context, uploadURL, fullPath, uploadID string, partSeq int, fileName string, section io.Reader, size int64, options []fs.OpenOption) (string, error) {
	reqBody, contentType, overhead, err := rest.MultipartUpload(ctx, section, nil, "file", fileName)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(uploadURL, "/")+"/rest/2.0/pcs/superfile2", reqBody)
	if err != nil {
		return "", err
	}
	q := req.URL.Query()
	q.Set("method", "upload")
	q.Set("access_token", f.opt.AccessToken)
	q.Set("type", "tmpfile")
	q.Set("path", fullPath)
	q.Set("uploadid", uploadID)
	q.Set("partseq", strconv.Itoa(partSeq))
	req.URL.RawQuery = q.Encode()
	req.ContentLength = overhead + size
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Referer", "https://pan.baidu.com/")
	req.Header.Set("Origin", "https://pan.baidu.com")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	client := *f.httpClient
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", fmt.Errorf("baidu: upload http %d: %s", resp.StatusCode, string(body))
	}
	lower := strings.ToLower(string(body))
	if strings.Contains(lower, "uploadid") && (strings.Contains(lower, "invalid") || strings.Contains(lower, "expired") || strings.Contains(lower, "not found")) {
		return "", errUploadIDExpired
	}
	var out api.UploadSliceResp
	_ = json.Unmarshal(body, &out)
	if out.ErrorCode != 0 || out.Errno != 0 {
		fs.Debugf(f, "baidu upload api error: url=%s uploadid=%s part=%d errno=%d error_code=%d body=%s", req.URL.String(), uploadID, partSeq, out.Errno, out.ErrorCode, string(body))
	}
	if out.ErrorCode != 0 || out.Errno != 0 {
		return "", fmt.Errorf("baidu: upload failed: %s", string(body))
	}
	return out.MD5, nil
}
