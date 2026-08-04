package baiduphoto

import (
	"net/url"
	"reflect"
	"testing"
)

func TestNewUploadForm(t *testing.T) {
	hashes := &uploadHashes{
		contentMD5:   "content-md5",
		sliceMD5:     "slice-md5",
		blockListStr: `["block-1","block-2"]`,
	}
	want := url.Values{
		"autoinit":    {"1"},
		"isdir":       {"0"},
		"rtype":       {"1"},
		"ctype":       {"11"},
		"path":        {"/photo.jpg"},
		"size":        {"12345"},
		"slice-md5":   {"slice-md5"},
		"content-md5": {"content-md5"},
		"block_list":  {`["block-1","block-2"]`},
	}

	if got := newUploadForm("/photo.jpg", 12345, hashes); !reflect.DeepEqual(got, want) {
		t.Fatalf("newUploadForm() = %#v, want %#v", got, want)
	}
}

func TestNewCreateFormReusesUploadParameters(t *testing.T) {
	hashes := &uploadHashes{
		contentMD5:   "content-md5",
		sliceMD5:     "slice-md5",
		blockListStr: `["block-1","block-2"]`,
	}
	want := newUploadForm("/photo.jpg", 12345, hashes)
	want.Set("uploadid", "upload-id")

	if got := newCreateForm("/photo.jpg", 12345, "upload-id", hashes); !reflect.DeepEqual(got, want) {
		t.Fatalf("newCreateForm() = %#v, want %#v", got, want)
	}
}
