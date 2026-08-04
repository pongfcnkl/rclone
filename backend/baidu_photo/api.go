package baiduphoto

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
)

const (
	apiURL       = "https://photo.baidu.com/youai"
	userAPIURL   = apiURL + "/user/v1"
	albumAPIURL  = apiURL + "/album/v1"
	fileAPIURLV1 = apiURL + "/file/v1"
	fileAPIURLV2 = apiURL + "/file/v2"
)

func (f *Fs) call(ctx context.Context, method, rawURL string, query, form url.Values, out any) ([]byte, error) {
	var body []byte
	err := f.pacer.Call(func() (bool, error) {
		u, err := url.Parse(rawURL)
		if err != nil {
			return false, err
		}
		q := u.Query()
		for k, vals := range query {
			for _, v := range vals {
				q.Add(k, v)
			}
		}
		u.RawQuery = q.Encode()
		var reqBody io.Reader
		if form != nil {
			reqBody = bytes.NewBufferString(form.Encode())
		}
		req, err := http.NewRequestWithContext(ctx, method, u.String(), reqBody)
		if err != nil {
			return false, err
		}
		req.Header.Set("Cookie", f.opt.Cookie)
		req.Header.Set("User-Agent", defaultUserAgent)
		req.Header.Set("Referer", "https://photo.baidu.com/")
		req.Header.Set("Origin", "https://photo.baidu.com")
		req.Header.Set("Accept", "application/json, text/plain, */*")
		if form != nil {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
		resp, err := f.httpClient.Do(req)
		if err != nil {
			return shouldRetry(ctx, resp, err)
		}
		defer resp.Body.Close()
		body, err = io.ReadAll(resp.Body)
		if err != nil {
			return shouldRetry(ctx, resp, err)
		}
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return shouldRetry(ctx, resp, fmt.Errorf("baidu_photo: http %d: %s", resp.StatusCode, string(body)))
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	if out != nil {
		if err := json.Unmarshal(body, out); err != nil {
			return nil, err
		}
		if v, ok := out.(interface{ Err() error }); ok {
			if err := v.Err(); err != nil {
				fs.Debugf(f, "baidu_photo api error: method=%s url=%s body=%s", method, rawURL, string(body))
				return nil, err
			}
		}
	}
	return body, nil
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

func (f *Fs) apiUserInfo(ctx context.Context) (*userInfo, error) {
	var info userInfo
	_, err := f.call(ctx, http.MethodGet, userAPIURL+"/getuinfo", nil, nil, &info)
	return &info, err
}

func (f *Fs) apiBDSToken(ctx context.Context) (string, error) {
	var info struct {
		apiError
		Result struct {
			Bdstoken string `json:"bdstoken"`
			Token    string `json:"token"`
			Uk       int64  `json:"uk"`
		} `json:"result"`
	}
	_, err := f.call(ctx, http.MethodGet, "https://pan.baidu.com/api/gettemplatevariable?fields=[%22bdstoken%22,%22token%22,%22uk%22]", nil, nil, &info)
	if err != nil {
		return "", err
	}
	return info.Result.Bdstoken, nil
}

func (f *Fs) apiAllFiles(ctx context.Context) ([]file, error) {
	var files []file
	cursor := ""
	for {
		var resp fileListResp
		_, err := f.call(ctx, http.MethodGet, fileAPIURLV1+"/list", url.Values{
			"need_thumbnail":     {"1"},
			"need_filter_hidden": {"0"},
			"cursor":             {cursor},
		}, nil, &resp)
		if err != nil {
			return nil, err
		}
		files = append(files, resp.List...)
		if !resp.hasNext() {
			return files, nil
		}
		cursor = resp.Cursor
	}
}

func (f *Fs) apiAllAlbums(ctx context.Context) ([]album, error) {
	var albums []album
	cursor := ""
	for {
		var resp albumListResp
		_, err := f.call(ctx, http.MethodGet, albumAPIURL+"/list", url.Values{
			"need_amount": {"1"},
			"limit":       {"100"},
			"cursor":      {cursor},
		}, nil, &resp)
		if err != nil {
			return nil, err
		}
		albums = append(albums, resp.List...)
		if !resp.hasNext() {
			return albums, nil
		}
		cursor = resp.Cursor
	}
}

func (f *Fs) apiAlbumFiles(ctx context.Context, a *album) ([]albumFile, error) {
	var files []albumFile
	cursor := ""
	for {
		var resp albumFileListResp
		_, err := f.call(ctx, http.MethodGet, albumAPIURL+"/listfile", url.Values{
			"album_id":    {a.AlbumID},
			"need_amount": {"1"},
			"limit":       {"1000"},
			"cursor":      {cursor},
		}, nil, &resp)
		if err != nil {
			return nil, err
		}
		files = append(files, resp.List...)
		if !resp.hasNext() {
			return files, nil
		}
		cursor = resp.Cursor
	}
}

func (f *Fs) apiAlbumDetail(ctx context.Context, albumID string) (*album, error) {
	var a album
	_, err := f.call(ctx, http.MethodGet, albumAPIURL+"/detail", url.Values{"album_id": {albumID}}, nil, &a)
	if err != nil {
		return nil, err
	}
	if a.AlbumID == "" {
		return nil, fs.ErrorDirNotFound
	}
	return &a, nil
}

func (f *Fs) apiCreateAlbum(ctx context.Context, name string) (*album, error) {
	var resp joinOrCreateAlbumResp
	_, err := f.call(ctx, http.MethodPost, albumAPIURL+"/create", url.Values{
		"title":  {name},
		"tid":    {tid()},
		"source": {"0"},
	}, nil, &resp)
	if err != nil {
		return nil, err
	}
	if resp.AlbumID != "" {
		a, err := f.apiAlbumDetail(ctx, resp.AlbumID)
		if err == nil {
			return a, nil
		}
		fs.Debugf(f, "baidu_photo: album detail lookup failed after create: album_id=%q title=%q: %v", resp.AlbumID, name, err)
	}
	a, err := f.findAlbumByTitle(ctx, name)
	if err == nil {
		return a, nil
	}
	if resp.AlbumID == "" {
		return nil, fmt.Errorf("baidu_photo: created album %q but response did not include album_id", name)
	}
	return nil, err
}

func (f *Fs) apiDeleteAlbum(ctx context.Context, a *album) error {
	var resp apiError
	_, err := f.call(ctx, http.MethodPost, albumAPIURL+"/delete", nil, url.Values{
		"album_id":            {a.AlbumID},
		"tid":                 {strconv.FormatInt(a.Tid, 10)},
		"delete_origin_image": {boolInt(f.opt.DeleteOrigin)},
	}, &resp)
	return err
}

func (f *Fs) apiDeleteFile(ctx context.Context, item file) error {
	var resp apiError
	_, err := f.call(ctx, http.MethodGet, fileAPIURLV1+"/delete", url.Values{
		"fsid_list": {fmt.Sprintf("[%d]", item.Fsid)},
	}, nil, &resp)
	return err
}

func (f *Fs) apiDeleteAlbumFile(ctx context.Context, item albumFile) error {
	var resp apiError
	_, err := f.call(ctx, http.MethodPost, albumAPIURL+"/delfile", nil, url.Values{
		"album_id":   {item.AlbumID},
		"tid":        {strconv.FormatInt(item.Tid, 10)},
		"list":       {fmt.Sprintf(`[{"fsid":%d,"uk":%d}]`, item.Fsid, item.Uk)},
		"del_origin": {boolInt(f.opt.DeleteOrigin)},
	}, &resp)
	return err
}

func (f *Fs) apiAddAlbumFile(ctx context.Context, a *album, item file) (*albumFile, error) {
	var resp apiError
	_, err := f.call(ctx, http.MethodGet, albumAPIURL+"/addfile", url.Values{
		"album_id": {a.AlbumID},
		"tid":      {strconv.FormatInt(a.Tid, 10)},
		"list":     {fmt.Sprintf(`[{"fsid":%d}]`, item.Fsid)},
	}, nil, &resp)
	if err != nil {
		return nil, err
	}
	return &albumFile{file: item, AlbumID: a.AlbumID, Tid: a.Tid, Uk: f.uk}, nil
}

func (f *Fs) apiCopyAlbumFile(ctx context.Context, item albumFile) (*file, error) {
	var resp copyFileResp
	_, err := f.call(ctx, http.MethodPost, albumAPIURL+"/copyfile", nil, url.Values{
		"album_id": {item.AlbumID},
		"tid":      {strconv.FormatInt(item.Tid, 10)},
		"uk":       {strconv.FormatInt(item.Uk, 10)},
		"list":     {fmt.Sprintf(`[{"fsid":%d}]`, item.Fsid)},
	}, &resp)
	if err != nil {
		return nil, err
	}
	if len(resp.List) == 0 {
		return nil, fs.ErrorObjectNotFound
	}
	copied := resp.List[0]
	return &file{Fsid: copied.Fsid, Path: copied.Path, Ctime: copied.Ctime, Mtime: copied.Ctime, Size: item.Size, Thumburl: item.Thumburl, MD5: item.MD5}, nil
}

func (f *Fs) apiDownloadLink(ctx context.Context, id int64) (string, error) {
	var resp struct {
		apiError
		Dlink string `json:"dlink"`
	}
	_, err := f.call(ctx, http.MethodGet, fileAPIURLV2+"/download", url.Values{"fsid": {strconv.FormatInt(id, 10)}}, nil, &resp)
	if err != nil {
		return "", err
	}
	if resp.Dlink == "" {
		return "", fs.ErrorObjectNotFound
	}
	return resp.Dlink, nil
}

func tid() string {
	return fmt.Sprintf("3%d%.0f", time.Now().Unix(), math.Floor(9000000*rand.Float64()+1000000))
}

func boolInt(v bool) string {
	if v {
		return "1"
	}
	return "0"
}

func trimAlbumID(v string) string {
	if i := strings.IndexByte(v, '|'); i >= 0 {
		return v[:i]
	}
	return v
}
