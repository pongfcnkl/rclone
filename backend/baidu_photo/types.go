package baiduphoto

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type apiError struct {
	Errno     int `json:"errno"`
	RequestID int `json:"request_id"`
}

func (e apiError) Err() error {
	switch e.Errno {
	case 0:
		return nil
	case 50805:
		return fmt.Errorf("baidu_photo: album already joined")
	case 50820:
		return fmt.Errorf("baidu_photo: no shared albums found")
	case 50100:
		return fmt.Errorf("baidu_photo: illegal title, only supports 50 characters")
	default:
		return fmt.Errorf("baidu_photo: errno %d", e.Errno)
	}
}

type page struct {
	HasMore int    `json:"has_more"`
	Cursor  string `json:"cursor"`
}

func (p page) hasNext() bool { return p.HasMore == 1 }

type userInfo struct {
	YouaID string `json:"youa_id"`
}

type fileListResp struct {
	apiError
	page
	List []file `json:"list"`
}

type file struct {
	Fsid     int64    `json:"fsid"`
	Path     string   `json:"path"`
	Size     int64    `json:"size"`
	Ctime    int64    `json:"ctime"`
	Mtime    int64    `json:"mtime"`
	Thumburl []string `json:"thumburl"`
	MD5      string   `json:"md5"`
}

func (f file) name() string {
	i := strings.LastIndex(f.Path, "/")
	if i < 0 {
		return f.Path
	}
	return f.Path[i+1:]
}

type albumListResp struct {
	apiError
	page
	List       []album `json:"list"`
	Reset      int64   `json:"reset"`
	TotalCount int64   `json:"total_count"`
}

type album struct {
	AlbumID      string `json:"album_id"`
	Tid          int64  `json:"tid"`
	Title        string `json:"title"`
	JoinTime     int64  `json:"join_time"`
	CreationTime int64  `json:"create_time"`
	Mtime        int64  `json:"mtime"`
}

type albumFileListResp struct {
	apiError
	page
	List       []albumFile `json:"list"`
	Reset      int64       `json:"reset"`
	TotalCount int64       `json:"total_count"`
}

type albumFile struct {
	file
	AlbumID string `json:"album_id"`
	Tid     int64  `json:"tid"`
	Uk      int64  `json:"uk"`
}

type copyFileResp struct {
	apiError
	List []copyFile `json:"list"`
}

type copyFile struct {
	FromFsid  int64  `json:"from_fsid"`
	Ctime     int64  `json:"ctime"`
	Fsid      int64  `json:"fsid"`
	Path      string `json:"path"`
	ShootTime int    `json:"shoot_time"`
}

type uploadFile struct {
	FsID           int64  `json:"fs_id"`
	Size           int64  `json:"size"`
	MD5            string `json:"md5"`
	ServerFilename string `json:"server_filename"`
	Path           string `json:"path"`
	Ctime          int64  `json:"ctime"`
	Mtime          int64  `json:"mtime"`
	Isdir          int    `json:"isdir"`
	Category       int    `json:"category"`
	ServerMD5      string `json:"server_md5"`
	ShootTime      int    `json:"shoot_time"`
}

type createFileResp struct {
	apiError
	Data uploadFile `json:"data"`
}

type precreateResp struct {
	apiError
	ReturnType int `json:"return_type"`
	createFileResp
	Path      string `json:"path"`
	UploadID  string `json:"uploadid"`
	BlockList []int  `json:"block_list"`
}

type uploadSliceResp struct {
	apiError
	MD5       string `json:"md5"`
	RequestID int64  `json:"request_id"`
	ErrorCode int    `json:"error_code"`
}

type joinOrCreateAlbumResp struct {
	apiError
	AlbumID       string `json:"album_id"`
	AlreadyExists int    `json:"already_exists"`
}

type inviteResp struct {
	apiError
	Pdata struct {
		InviteCode string `json:"invite_code"`
		ExpireTime int    `json:"expire_time"`
		ShareID    string `json:"share_id"`
	} `json:"pdata"`
}

func decryptMD5(encryptMD5 string) string {
	if len(encryptMD5) != 32 {
		return encryptMD5
	}
	if _, err := strconv.ParseUint(encryptMD5[:16], 16, 64); err == nil {
		if _, err = strconv.ParseUint(encryptMD5[16:], 16, 64); err == nil {
			return encryptMD5
		}
	}
	var out strings.Builder
	out.Grow(len(encryptMD5))
	for i, n := 0, int64(0); i < len(encryptMD5); i++ {
		if i == 9 {
			n = int64(unicode.ToLower(rune(encryptMD5[i])) - 'g')
		} else {
			n, _ = strconv.ParseInt(encryptMD5[i:i+1], 16, 64)
		}
		out.WriteString(strconv.FormatInt(n^int64(15&i), 16))
	}
	encryptMD5 = out.String()
	return encryptMD5[8:16] + encryptMD5[:8] + encryptMD5[24:32] + encryptMD5[16:24]
}

func nonZeroTime(v, fallback int64) time.Time {
	if v != 0 {
		return time.Unix(v, 0)
	}
	return time.Unix(fallback, 0)
}
