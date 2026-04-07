package api

import "fmt"

var errorDescriptions = map[int]string{
	-9:      "file not found",
	-8:      "file already exists",
	-6:      "invalid access token",
	2:       "missing required parameter",
	12:      "partial operation failed",
	58:      "file exceeds allowed size",
	111:     "access token expired",
	9019:    "need verify",
	31021:   "network unavailable",
	31034:   "too many requests",
	31066:   "file content invalid",
	31299:   "first part must not be smaller than 4MB",
	31326:   "upload id invalid",
	31363:   "upload id expired",
	36009:   "insufficient user space",
	36010:   "file not found",
	4000023: "verification required",
	450016:  "verification required",
}

type ErrorInterface interface {
	ErrorNumber() int
	Error() string
	Err() error
}

type ErrorAPI struct {
	ErrNumber int `json:"errno"`
}

func (e ErrorAPI) ErrorNumber() int { return e.ErrNumber }

func (e ErrorAPI) Error() string {
	if msg, ok := errorDescriptions[e.ErrNumber]; ok {
		return msg
	}
	return fmt.Sprintf("baidu error %d", e.ErrNumber)
}

func (e ErrorAPI) Err() error {
	if e.ErrNumber == 0 {
		return nil
	}
	return e
}

func ErrIsNum(err error, numbers ...int) bool {
	e, ok := err.(ErrorInterface)
	if !ok {
		return false
	}
	for _, number := range numbers {
		if e.ErrorNumber() == number {
			return true
		}
	}
	return false
}

type TokenResp struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

type File struct {
	Category       int    `json:"category"`
	FsID           int64  `json:"fs_id"`
	Size           int64  `json:"size"`
	Path           string `json:"path"`
	ServerFilename string `json:"server_filename"`
	MD5            string `json:"md5"`
	IsDir          int    `json:"isdir"`
	ServerCTime    int64  `json:"server_ctime"`
	ServerMTime    int64  `json:"server_mtime"`
	LocalMTime     int64  `json:"local_mtime"`
	LocalCTime     int64  `json:"local_ctime"`
	CTime          int64  `json:"ctime"`
	MTime          int64  `json:"mtime"`
}

type ListResp struct {
	ErrorAPI
	List []File `json:"list"`
}

type DownloadResp struct {
	ErrorAPI
	List []struct {
		Dlink string `json:"dlink"`
	} `json:"list"`
}

type UploadSliceResp struct {
	MD5       string `json:"md5"`
	RequestID int64  `json:"request_id"`
	ErrorCode int    `json:"error_code"`
	Errno     int    `json:"errno"`
}

type PrecreateResp struct {
	ErrorAPI
	ReturnType int    `json:"return_type"`
	Path       string `json:"path"`
	UploadID   string `json:"uploadid"`
	BlockList  []int  `json:"block_list"`
	Info       File   `json:"info"`
	UploadURL  string `json:"-"`
}

type UploadServerResp struct {
	ErrorCode  int `json:"error_code"`
	Servers    []struct {
		Server string `json:"server"`
	} `json:"servers"`
	BakServers []struct {
		Server string `json:"server"`
	} `json:"bak_servers"`
}

type QuotaResp struct {
	ErrorAPI
	Total int64 `json:"total"`
	Used  int64 `json:"used"`
}

type FileMetaResp struct {
	ErrorAPI
	Info []struct {
		ErrorAPI
		File
	} `json:"info"`
}
