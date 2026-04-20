package oneguangyapan

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
)

const (
	name           = "1guangyapan"
	description    = "1GuangYaPan"
	accountBaseURL = "https://account.guangyapan.com"
	apiBaseURL     = "https://api.guangyapan.com"
	defaultClient  = "aMe-8VSlkrbQXpUR"
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        name,
		Description: description,
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:      "access_token",
			Help:      "GuangYaPan access token.",
			Sensitive: true,
			Advanced:  true,
		}, {
			Name:      "refresh_token",
			Help:      "GuangYaPan refresh token.",
			Sensitive: true,
		}, {
			Name:      config.ConfigClientID,
			Help:      "GuangYaPan client ID.",
			Default:   defaultClient,
			Sensitive: true,
		}, {
			Name:     "phone_number",
			Help:     "Phone number used for SMS login, for example +86 13800000000.",
			Advanced: true,
		}, {
			Name:     "captcha_token",
			Help:     "Captcha token for SMS login.",
			Advanced: true,
		}, {
			Name:     "send_code",
			Help:     "Set to true to request an SMS code during backend initialization.",
			Advanced: true,
		}, {
			Name:     "verify_code",
			Help:     "SMS verification code for login.",
			Advanced: true,
		}, {
			Name:     "verification_id",
			Help:     "Verification ID cached after sending SMS code.",
			Advanced: true,
		}, {
			Name:     "device_id",
			Help:     "Optional device ID, 32 hex characters.",
			Advanced: true,
		}, {
			Name:     "page_size",
			Help:     "Page size for list requests.",
			Default:  100,
			Advanced: true,
		}, {
			Name:     "order_by",
			Help:     "List order: 0=name, 1=size, 2=create_time, 3=update_time.",
			Default:  3,
			Advanced: true,
		}, {
			Name:     "sort_type",
			Help:     "List sort order: 0=asc, 1=desc.",
			Default:  1,
			Advanced: true,
		}, {
			Name:     "upload_thread",
			Help:     "Preferred upload concurrency. The current implementation uploads sequentially.",
			Default:  3,
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			Default: (encoder.Display |
				encoder.EncodeBackQuote |
				encoder.EncodeDoubleQuote |
				encoder.EncodeLtGt |
				encoder.EncodeLeftSpace |
				encoder.EncodeInvalidUtf8),
		}},
	})
}

type Options struct {
	AccessToken    string               `config:"access_token"`
	RefreshToken   string               `config:"refresh_token"`
	ClientID       string               `config:"client_id"`
	PhoneNumber    string               `config:"phone_number"`
	CaptchaToken   string               `config:"captcha_token"`
	SendCode       bool                 `config:"send_code"`
	VerifyCode     string               `config:"verify_code"`
	VerificationID string               `config:"verification_id"`
	DeviceID       string               `config:"device_id"`
	PageSize       int                  `config:"page_size"`
	OrderBy        int                  `config:"order_by"`
	SortType       int                  `config:"sort_type"`
	UploadThread   int                  `config:"upload_thread"`
	Enc            encoder.MultiEncoder `config:"encoding"`
}

type Fs struct {
	name         string
	originalName string
	root         string
	rootMissing  bool
	opt          Options
	features     *fs.Features
	httpClient   *http.Client
	dirMu        sync.Mutex
}

type Object struct {
	fs      *Fs
	item    fileItem
	remote  string
	size    int64
	modTime time.Time
}

var (
	_ fs.Fs             = (*Fs)(nil)
	_ fs.Copier         = (*Fs)(nil)
	_ fs.Mover          = (*Fs)(nil)
	_ fs.Purger         = (*Fs)(nil)
	_ fs.PutUncheckeder = (*Fs)(nil)
	_ fs.Object         = (*Object)(nil)
)

func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}
	originalName := name
	if idx := strings.IndexRune(name, '{'); idx > 0 {
		originalName = name[:idx]
	}
	root = strings.Trim(root, "/")
	opt.ClientID = strings.TrimSpace(opt.ClientID)
	if opt.ClientID == "" {
		opt.ClientID = defaultClient
	}
	opt.AccessToken = strings.TrimSpace(opt.AccessToken)
	opt.RefreshToken = strings.TrimSpace(opt.RefreshToken)
	opt.PhoneNumber = strings.TrimSpace(opt.PhoneNumber)
	opt.CaptchaToken = strings.TrimSpace(opt.CaptchaToken)
	opt.VerifyCode = strings.TrimSpace(opt.VerifyCode)
	opt.VerificationID = strings.TrimSpace(opt.VerificationID)
	opt.DeviceID = normalizeDeviceID(opt.DeviceID)
	if opt.DeviceID == "" {
		opt.DeviceID = randomDeviceID()
	}
	if opt.PageSize <= 0 {
		opt.PageSize = 100
	}
	if opt.OrderBy < 0 {
		opt.OrderBy = 3
	}
	if opt.SortType != 0 && opt.SortType != 1 {
		opt.SortType = 1
	}
	if opt.UploadThread < 1 || opt.UploadThread > 32 {
		opt.UploadThread = 3
	}

	f := &Fs{
		name:         name,
		originalName: originalName,
		root:         root,
		opt:          *opt,
		httpClient:   fshttp.NewClient(ctx),
	}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		NoMultiThreading:        true,
		ReadMetadata:            true,
	}).Fill(ctx, f)
	if err := f.init(ctx); err != nil {
		return nil, err
	}
	if root == "" {
		return f, nil
	}
	actualRoot, item, err := f.findDeepestExistingAbsoluteRemote(ctx, root)
	if err != nil {
		return nil, err
	}
	if item == nil {
		f.root = root
		f.rootMissing = true
		return f, nil
	}
	f.root = actualRoot
	if !item.IsDir() {
		newRoot := path.Dir(actualRoot)
		if newRoot == "." {
			newRoot = ""
		}
		tempF := *f
		tempF.root = newRoot
		return &tempF, fs.ErrorIsFile
	}
	return f, nil
}

func (f *Fs) init(ctx context.Context) error {
	if f.opt.AccessToken != "" {
		if err := f.validateToken(ctx); err == nil {
			return nil
		}
		f.opt.AccessToken = ""
	}
	if f.opt.RefreshToken != "" {
		if err := f.refreshToken(ctx); err == nil {
			return f.validateToken(ctx)
		}
	}
	if f.opt.PhoneNumber != "" {
		if f.canSMSLogin() {
			if err := f.loginBySMSCode(ctx); err != nil {
				return err
			}
			return f.validateToken(ctx)
		}
		if f.opt.SendCode {
			if err := f.prepareSMSCode(ctx); err != nil {
				return err
			}
			return nil
		}
	}
	return errors.New("login failed: provide access_token, refresh_token, or phone_number + verify_code")
}

func (f *Fs) Name() string             { return f.name }
func (f *Fs) Root() string             { return f.root }
func (f *Fs) String() string           { return fmt.Sprintf("1GuangYaPan root '%s'", f.root) }
func (f *Fs) Precision() time.Duration { return time.Second }
func (f *Fs) Hashes() hash.Set         { return hash.Set(hash.None) }
func (f *Fs) Features() *fs.Features   { return f.features }

func (f *Fs) fullPath(remote string) string {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		return f.root
	}
	if f.root == "" {
		return remote
	}
	return path.Join(f.root, remote)
}

func (f *Fs) newObject(remote string, item fileItem) *Object {
	return &Object{
		fs:      f,
		item:    item,
		remote:  remote,
		size:    item.FileSize,
		modTime: item.ModTime(),
	}
}

func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	if f.rootMissing && dir == "" {
		return entries, nil
	}
	item, err := f.findDirByRemote(ctx, dir)
	if err != nil {
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return nil, fs.ErrorDirNotFound
		}
		return nil, err
	}
	items, err := f.listChildren(ctx, item.FileID)
	if err != nil {
		return nil, err
	}
	for _, it := range items {
		remote := path.Join(dir, f.opt.Enc.ToStandardName(it.FileName))
		if it.IsDir() {
			entries = append(entries, fs.NewDir(remote, it.ModTime()).SetID(it.FileID))
			continue
		}
		entries = append(entries, f.newObject(remote, it))
	}
	return entries, nil
}

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	if f.rootMissing {
		return nil, fs.ErrorObjectNotFound
	}
	item, err := f.findItemByRemote(ctx, remote)
	if err != nil {
		return nil, err
	}
	if item.IsDir() {
		return nil, fs.ErrorIsDir
	}
	return f.newObject(remote, item), nil
}

func (f *Fs) readBackObject(ctx context.Context, src fs.ObjectInfo) (fs.Object, error) {
	var lastErr error
	for attempt := 0; attempt < 6; attempt++ {
		obj, err := f.NewObject(ctx, src.Remote())
		if err == nil {
			return obj, nil
		}
		if !errors.Is(err, fs.ErrorObjectNotFound) {
			return nil, err
		}
		lastErr = err
		time.Sleep(time.Duration(attempt+1) * 300 * time.Millisecond)
	}
	return &Object{fs: f, remote: src.Remote(), size: src.Size(), modTime: src.ModTime(ctx)}, lastErr
}

func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	if src.Size() < 0 {
		return nil, errors.New("1guangyapan requires known size")
	}
	if err := f.upload(ctx, in, src, options...); err != nil {
		return nil, err
	}
	return f.readBackObject(ctx, src)
}

func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}

func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	err := f.ensureDir(ctx, dir)
	if err == nil {
		f.rootMissing = false
	}
	return err
}

func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	item, err := f.findDirByRemote(ctx, dir)
	if err != nil {
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return fs.ErrorDirNotFound
		}
		return err
	}
	children, err := f.listChildren(ctx, item.FileID)
	if err != nil {
		return err
	}
	if len(children) != 0 {
		return fs.ErrorDirectoryNotEmpty
	}
	return f.apiDelete(ctx, item.FileID)
}

func (f *Fs) Purge(ctx context.Context, dir string) error {
	item, err := f.findDirByRemote(ctx, dir)
	if err != nil {
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return fs.ErrorDirNotFound
		}
		return err
	}
	return f.apiDelete(ctx, item.FileID)
}

func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantCopy
	}
	parentRemote := path.Dir(remote)
	if parentRemote == "." {
		parentRemote = ""
	}
	if err := f.ensureDir(ctx, parentRemote); err != nil {
		return nil, err
	}
	dstParent, err := f.findDirByRemote(ctx, parentRemote)
	if err != nil {
		return nil, err
	}
	if err = f.apiCopy(ctx, srcObj.item.FileID, dstParent.FileID); err != nil {
		return nil, err
	}
	if path.Base(srcObj.remote) != path.Base(remote) {
		obj, err := f.NewObject(ctx, path.Join(parentRemote, path.Base(srcObj.remote)))
		if err != nil {
			return nil, err
		}
		return f.Move(ctx, obj, remote)
	}
	return f.NewObject(ctx, remote)
}

func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantMove
	}
	parentRemote := path.Dir(remote)
	if parentRemote == "." {
		parentRemote = ""
	}
	if err := f.ensureDir(ctx, parentRemote); err != nil {
		return nil, err
	}
	dstParent, err := f.findDirByRemote(ctx, parentRemote)
	if err != nil {
		return nil, err
	}
	srcParentRemote := path.Dir(srcObj.remote)
	if srcParentRemote == "." {
		srcParentRemote = ""
	}
	if srcParentRemote != parentRemote {
		if err = f.apiMove(ctx, srcObj.item.FileID, dstParent.FileID); err != nil {
			return nil, err
		}
	}
	if path.Base(srcObj.remote) != path.Base(remote) {
		if err = f.apiRename(ctx, srcObj.item.FileID, f.opt.Enc.FromStandardName(path.Base(remote))); err != nil {
			return nil, err
		}
	}
	return f.NewObject(ctx, remote)
}

func (f *Fs) findItemByRemote(ctx context.Context, remote string) (fileItem, error) {
	return f.findItemByAbsoluteRemote(ctx, f.fullPath(remote))
}

func (f *Fs) findDirByRemote(ctx context.Context, remote string) (fileItem, error) {
	item, err := f.findItemByRemote(ctx, remote)
	if err != nil {
		return fileItem{}, err
	}
	if !item.IsDir() {
		return fileItem{}, fs.ErrorIsFile
	}
	return item, nil
}

func (f *Fs) findItemByAbsoluteRemote(ctx context.Context, remote string) (fileItem, error) {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		return fileItem{ResType: 2}, nil
	}
	parts := strings.Split(remote, "/")
	parentID := ""
	var current fileItem
	for _, part := range parts {
		items, err := f.listChildren(ctx, parentID)
		if err != nil {
			return fileItem{}, err
		}
		found := false
		want := f.opt.Enc.FromStandardName(part)
		for _, item := range items {
			if item.FileName == want {
				current = item
				parentID = item.FileID
				found = true
				break
			}
		}
		if !found {
			return fileItem{}, fs.ErrorObjectNotFound
		}
	}
	return current, nil
}

func (f *Fs) findDeepestExistingAbsoluteRemote(ctx context.Context, remote string) (string, *fileItem, error) {
	remote = strings.Trim(remote, "/")
	if remote == "" {
		return "", nil, nil
	}
	parts := strings.Split(remote, "/")
	current := ""
	var last *fileItem
	for i, part := range parts {
		next := part
		if current != "" {
			next = path.Join(current, part)
		}
		item, err := f.findItemByAbsoluteRemote(ctx, next)
		if err != nil {
			if errors.Is(err, fs.ErrorObjectNotFound) {
				if i == len(parts)-1 {
					return remote, nil, nil
				}
				return current, nil, nil
			}
			return "", nil, err
		}
		current = next
		itemCopy := item
		last = &itemCopy
	}
	return current, last, nil
}

func (f *Fs) ensureDir(ctx context.Context, dir string) error {
	f.dirMu.Lock()
	defer f.dirMu.Unlock()

	if f.rootMissing {
		parts := strings.Split(strings.Trim(f.root, "/"), "/")
		current := ""
		parentID := ""
		for _, part := range parts {
			if part == "" {
				continue
			}
			next := part
			if current != "" {
				next = path.Join(current, part)
			}
			item, err := f.findItemByAbsoluteRemote(ctx, next)
			if err == nil {
				if !item.IsDir() {
					return fs.ErrorIsFile
				}
				current = next
				parentID = item.FileID
				continue
			}
			if !errors.Is(err, fs.ErrorObjectNotFound) {
				return err
			}
			created, err := f.ensureChildDir(ctx, parentID, part)
			if err != nil {
				return err
			}
			current = next
			parentID = created.FileID
		}
		f.rootMissing = false
	}
	dir = strings.Trim(dir, "/")
	if dir == "" {
		return nil
	}
	parts := strings.Split(dir, "/")
	current := ""
	parentID := ""
	if f.root != "" {
		rootItem, err := f.findDirByRemote(ctx, "")
		if err != nil {
			return err
		}
		parentID = rootItem.FileID
	}
	for _, part := range parts {
		next := part
		if current != "" {
			next = path.Join(current, part)
		}
		item, err := f.findItemByRemote(ctx, next)
		if err == nil {
			if !item.IsDir() {
				return fs.ErrorIsFile
			}
			current = next
			parentID = item.FileID
			continue
		}
		if !errors.Is(err, fs.ErrorObjectNotFound) {
			return err
		}
		created, err := f.ensureChildDir(ctx, parentID, f.opt.Enc.FromStandardName(part))
		if err != nil {
			return err
		}
		current = next
		parentID = created.FileID
	}
	return nil
}

func (f *Fs) ensureChildDir(ctx context.Context, parentID, dirName string) (fileItem, error) {
	items, err := f.listChildren(ctx, parentID)
	if err != nil {
		return fileItem{}, err
	}
	for _, item := range items {
		if item.IsDir() && item.FileName == dirName {
			return item, nil
		}
	}

	created, err := f.apiMkdir(ctx, parentID, dirName)
	if err != nil {
		return fileItem{}, err
	}
	if created.IsDir() && created.FileName == dirName && created.FileID != "" {
		return created, nil
	}

	for attempt := 0; attempt < 6; attempt++ {
		items, err = f.listChildren(ctx, parentID)
		if err != nil {
			return fileItem{}, err
		}
		for _, item := range items {
			if item.IsDir() && item.FileName == dirName {
				return item, nil
			}
		}
		select {
		case <-ctx.Done():
			return fileItem{}, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 150 * time.Millisecond):
		}
	}

	return created, nil
}

func (f *Fs) ensureAccessToken(ctx context.Context) error {
	if strings.TrimSpace(f.opt.AccessToken) != "" {
		return nil
	}
	if strings.TrimSpace(f.opt.RefreshToken) != "" {
		return f.refreshToken(ctx)
	}
	if f.canSMSLogin() {
		return f.loginBySMSCode(ctx)
	}
	return errors.New("access token is empty")
}

func (f *Fs) validateToken(ctx context.Context) error {
	var out userMeResp
	if _, _, err := f.doJSON(ctx, http.MethodGet, accountBaseURL+"/v1/user/me", headerMap{
		"Authorization": "Bearer " + f.opt.AccessToken,
	}, nil, &out); err != nil {
		return err
	}
	if strings.TrimSpace(out.Sub) == "" {
		return errors.New("validate token failed: empty user sub")
	}
	return nil
}

func (f *Fs) refreshToken(ctx context.Context) error {
	if f.opt.RefreshToken == "" {
		return errors.New("refresh_token is empty")
	}
	var out tokenResp
	_, _, err := f.doJSON(ctx, http.MethodPost, accountBaseURL+"/v1/auth/token", f.accountHeaders(), map[string]any{
		"client_id":     f.opt.ClientID,
		"grant_type":    "refresh_token",
		"refresh_token": f.opt.RefreshToken,
	}, &out)
	if err != nil {
		return err
	}
	if strings.TrimSpace(out.AccessToken) == "" {
		return fmt.Errorf("refresh token failed: %s", firstNonEmpty(out.ErrorDesc, out.Error, "empty access_token"))
	}
	f.opt.AccessToken = strings.TrimSpace(out.AccessToken)
	if strings.TrimSpace(out.RefreshToken) != "" {
		f.opt.RefreshToken = strings.TrimSpace(out.RefreshToken)
	}
	return f.saveTokens()
}

func (f *Fs) canSMSLogin() bool {
	return f.opt.PhoneNumber != "" && f.opt.VerifyCode != ""
}

func (f *Fs) loginBySMSCode(ctx context.Context) error {
	verificationID := strings.TrimSpace(f.opt.VerificationID)
	if verificationID == "" {
		var err error
		verificationID, err = f.requestVerificationID(ctx)
		if err != nil {
			return err
		}
	}
	var step2 verifyResp
	_, _, err := f.doJSON(ctx, http.MethodPost, accountBaseURL+"/v1/auth/verification/verify", f.accountHeaders(), map[string]any{
		"verification_id":   verificationID,
		"verification_code": f.opt.VerifyCode,
		"client_id":         f.opt.ClientID,
	}, &step2)
	if err != nil {
		return err
	}
	if strings.TrimSpace(step2.VerificationToken) == "" {
		return fmt.Errorf("verify code failed: %s", firstNonEmpty(step2.ErrorDesc, step2.Error, "empty verification_token"))
	}

	var out tokenResp
	_, _, err = f.doJSON(ctx, http.MethodPost, accountBaseURL+"/v1/auth/signin", f.accountHeaders(), map[string]any{
		"verification_code":  f.opt.VerifyCode,
		"verification_token": step2.VerificationToken,
		"username":           normalizePhoneE164(f.opt.PhoneNumber),
		"client_id":          f.opt.ClientID,
	}, &out)
	if err != nil {
		return err
	}
	if strings.TrimSpace(out.AccessToken) == "" {
		return fmt.Errorf("signin failed: %s", firstNonEmpty(out.ErrorDesc, out.Error, "empty access_token"))
	}
	f.opt.AccessToken = strings.TrimSpace(out.AccessToken)
	f.opt.RefreshToken = strings.TrimSpace(out.RefreshToken)
	f.opt.VerificationID = ""
	f.opt.VerifyCode = ""
	return f.saveTokens()
}

func (f *Fs) prepareSMSCode(ctx context.Context) error {
	f.opt.VerificationID = ""
	if err := f.ensureCaptchaToken(ctx, false); err != nil {
		return err
	}
	verificationID, err := f.requestVerificationID(ctx)
	if err != nil {
		return err
	}
	f.opt.VerificationID = verificationID
	f.opt.SendCode = false
	return f.saveSMSState()
}

func (f *Fs) requestVerificationID(ctx context.Context) (string, error) {
	headers := f.accountHeaders()
	if f.opt.CaptchaToken != "" {
		headers["X-Captcha-Token"] = f.opt.CaptchaToken
	}
	var out verificationResp
	_, _, err := f.doJSON(ctx, http.MethodPost, accountBaseURL+"/v1/auth/verification", headers, map[string]any{
		"phone_number": normalizePhoneE164(f.opt.PhoneNumber),
		"target":       "ANY",
		"client_id":    f.opt.ClientID,
	}, &out)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(out.VerificationID) == "" {
		if strings.Contains(out.Error, "captcha_invalid") || strings.Contains(out.ErrorDesc, "captcha_token expired") {
			if err := f.ensureCaptchaToken(ctx, true); err == nil {
				return f.requestVerificationID(ctx)
			}
		}
		return "", fmt.Errorf("request verification failed: %s", firstNonEmpty(out.ErrorDesc, out.Error, "empty verification_id"))
	}
	return strings.TrimSpace(out.VerificationID), nil
}

func (f *Fs) ensureCaptchaToken(ctx context.Context, force bool) error {
	if !force && f.opt.CaptchaToken != "" {
		return nil
	}
	var out captchaInitResp
	_, _, err := f.doJSON(ctx, http.MethodPost, accountBaseURL+"/v1/shield/captcha/init", f.accountHeaders(), map[string]any{
		"client_id": f.opt.ClientID,
		"action":    "POST:/v1/auth/verification",
		"device_id": f.opt.DeviceID,
		"meta": map[string]any{
			"username":           normalizePhoneE164(f.opt.PhoneNumber),
			"phone_number":       normalizePhoneE164(f.opt.PhoneNumber),
			"VERIFICATION_PHONE": normalizePhoneE164(f.opt.PhoneNumber),
		},
	}, &out)
	if err != nil {
		return err
	}
	if strings.TrimSpace(out.CaptchaToken) == "" {
		return fmt.Errorf("init captcha token failed: %s", firstNonEmpty(out.ErrorDesc, out.Error, "empty captcha_token"))
	}
	f.opt.CaptchaToken = strings.TrimSpace(out.CaptchaToken)
	return f.saveSMSState()
}

func (f *Fs) listChildren(ctx context.Context, parentID string) ([]fileItem, error) {
	if err := f.ensureAccessToken(ctx); err != nil {
		return nil, err
	}
	out := make([]fileItem, 0, f.opt.PageSize)
	for page := 0; ; page++ {
		var resp listResp
		err := f.postAPI(ctx, "/nd.bizuserres.s/v1/file/get_file_list", map[string]any{
			"parentId":  parentID,
			"page":      page,
			"pageSize":  f.opt.PageSize,
			"orderBy":   f.opt.OrderBy,
			"sortType":  f.opt.SortType,
			"fileTypes": []int{},
		}, &resp)
		if err != nil {
			return nil, err
		}
		out = append(out, resp.Data.List...)
		if len(resp.Data.List) < f.opt.PageSize {
			break
		}
		if resp.Data.Total > 0 && len(out) >= resp.Data.Total {
			break
		}
	}
	return out, nil
}

func (f *Fs) apiMkdir(ctx context.Context, parentID, dirName string) (fileItem, error) {
	var out createDirResp
	err := f.postAPI(ctx, "/nd.bizuserres.s/v1/file/create_dir", map[string]any{
		"parentId": parentID,
		"dirName":  dirName,
	}, &out)
	if err != nil {
		return fileItem{}, err
	}
	if !strings.EqualFold(strings.TrimSpace(out.Msg), "success") {
		return fileItem{}, fmt.Errorf("make dir failed: %s", strings.TrimSpace(out.Msg))
	}
	return out.Data, nil
}

func (f *Fs) apiRename(ctx context.Context, fileID, newName string) error {
	var out commonResp
	err := f.postAPI(ctx, "/nd.bizuserres.s/v1/file/rename", map[string]any{
		"fileId":  fileID,
		"newName": newName,
	}, &out)
	if err != nil {
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(out.Msg), "success") {
		return fmt.Errorf("rename failed: %s", strings.TrimSpace(out.Msg))
	}
	return nil
}

func (f *Fs) apiDelete(ctx context.Context, fileID string) error {
	var out deleteResp
	err := f.postAPI(ctx, "/nd.bizuserres.s/v1/file/delete_file", map[string]any{
		"fileIds": []string{fileID},
	}, &out)
	if err != nil {
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(out.Msg), "success") {
		return fmt.Errorf("delete failed: %s", strings.TrimSpace(out.Msg))
	}
	if strings.TrimSpace(out.Data.TaskID) == "" {
		return nil
	}
	return f.waitTaskDone(ctx, out.Data.TaskID)
}

func (f *Fs) apiMove(ctx context.Context, fileID, parentID string) error {
	var out deleteResp
	err := f.postAPI(ctx, "/nd.bizuserres.s/v1/file/move_file", map[string]any{
		"fileIds":  []string{fileID},
		"parentId": parentID,
	}, &out)
	if err != nil {
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(out.Msg), "success") {
		return fmt.Errorf("move failed: %s", strings.TrimSpace(out.Msg))
	}
	if strings.TrimSpace(out.Data.TaskID) == "" {
		return nil
	}
	return f.waitTaskDone(ctx, out.Data.TaskID)
}

func (f *Fs) apiCopy(ctx context.Context, fileID, parentID string) error {
	var out deleteResp
	err := f.postAPI(ctx, "/nd.bizuserres.s/v1/file/copy_file", map[string]any{
		"fileIds":  []string{fileID},
		"parentId": parentID,
	}, &out)
	if err != nil {
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(out.Msg), "success") {
		return fmt.Errorf("copy failed: %s", strings.TrimSpace(out.Msg))
	}
	if strings.TrimSpace(out.Data.TaskID) == "" {
		return nil
	}
	return f.waitTaskDone(ctx, out.Data.TaskID)
}

func (f *Fs) apiDownloadURL(ctx context.Context, fileID string) (string, error) {
	var out downloadResp
	err := f.postAPI(ctx, "/nd.bizuserres.s/v1/get_res_download_url", map[string]any{
		"fileId": fileID,
	}, &out)
	if err != nil {
		return "", err
	}
	link := strings.TrimSpace(out.Data.SignedURL)
	if link == "" {
		link = strings.TrimSpace(out.Data.DownloadURL)
	}
	if link == "" {
		return "", errors.New("empty download url")
	}
	return link, nil
}

func (f *Fs) postAPI(ctx context.Context, apiPath string, body any, out any) error {
	if err := f.ensureAccessToken(ctx); err != nil {
		return err
	}
	headers := f.apiHeaders()
	headers["Authorization"] = "Bearer " + f.opt.AccessToken
	respBody, status, err := f.doJSON(ctx, http.MethodPost, apiBaseURL+apiPath, headers, body, out)
	if err == nil {
		return nil
	}
	if status != http.StatusUnauthorized && status != http.StatusForbidden {
		return err
	}
	if strings.TrimSpace(f.opt.RefreshToken) == "" {
		return err
	}
	if refreshErr := f.refreshToken(ctx); refreshErr != nil {
		return refreshErr
	}
	headers["Authorization"] = "Bearer " + f.opt.AccessToken
	_, _, retryErr := f.doJSON(ctx, http.MethodPost, apiBaseURL+apiPath, headers, body, out)
	if retryErr != nil {
		return retryErr
	}
	_ = respBody
	return nil
}

func (f *Fs) waitTaskDone(ctx context.Context, taskID string) error {
	const (
		maxTry   = 30
		interval = 300 * time.Millisecond
	)
	for i := 0; i < maxTry; i++ {
		var out taskStatusResp
		if err := f.postAPI(ctx, "/nd.bizuserres.s/v1/get_task_status", map[string]any{"taskId": taskID}, &out); err != nil {
			return err
		}
		if !strings.EqualFold(strings.TrimSpace(out.Msg), "success") {
			return fmt.Errorf("get task status failed: %s", strings.TrimSpace(out.Msg))
		}
		switch out.Data.Status {
		case 2:
			return nil
		case -1, 3:
			return fmt.Errorf("task %s failed with status=%d", taskID, out.Data.Status)
		}
		if i == maxTry-1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
	return fmt.Errorf("task %s timeout", taskID)
}

func (o *Object) Fs() fs.Info                           { return o.fs }
func (o *Object) String() string                        { return o.remote }
func (o *Object) Remote() string                        { return o.remote }
func (o *Object) ModTime(ctx context.Context) time.Time { return o.modTime }
func (o *Object) Size() int64                           { return o.size }
func (o *Object) Storable() bool                        { return true }
func (o *Object) ID() string                            { return o.item.FileID }
func (o *Object) SetModTime(ctx context.Context, _ time.Time) error {
	return fs.ErrorCantSetModTime
}
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	link, err := o.fs.apiDownloadURL(ctx, o.item.FileID)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link, nil)
	if err != nil {
		return nil, err
	}
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	resp, err := o.fs.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("open file failed: status=%d body=%s", resp.StatusCode, string(body))
	}
	return resp.Body, nil
}

func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	if src.Size() < 0 {
		return errors.New("1guangyapan requires known size")
	}
	if err := o.fs.upload(ctx, in, src, options...); err != nil {
		return err
	}
	newObj, err := o.fs.NewObject(ctx, src.Remote())
	if err != nil {
		return err
	}
	updated := newObj.(*Object)
	*o = *updated
	return nil
}

func (o *Object) Remove(ctx context.Context) error {
	return o.fs.apiDelete(ctx, o.item.FileID)
}

type headerMap map[string]string

func (f *Fs) doJSON(ctx context.Context, method, endpoint string, headers headerMap, body any, out any) ([]byte, int, error) {
	var reqBody io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
		reqBody = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reqBody)
	if err != nil {
		return nil, 0, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return respBody, resp.StatusCode, fmt.Errorf("request failed: status=%d body=%s", resp.StatusCode, string(respBody))
	}
	if out != nil {
		if err = json.Unmarshal(respBody, out); err != nil {
			return respBody, resp.StatusCode, err
		}
	}
	return respBody, resp.StatusCode, nil
}

func (f *Fs) accountHeaders() headerMap {
	headers := headerMap{
		"Accept":             "application/json, text/plain, */*",
		"Content-Type":       "application/json",
		"X-Device-Model":     "chrome%2F147.0.0.0",
		"X-Device-Name":      "PC-Chrome",
		"X-Device-Sign":      "wdi10." + f.opt.DeviceID + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		"X-Net-Work-Type":    "NONE",
		"X-OS-Version":       "MacIntel",
		"X-Platform-Version": "1",
		"X-Protocol-Version": "301",
		"X-Provider-Name":    "NONE",
		"X-SDK-Version":      "9.0.2",
		"X-Client-Id":        f.opt.ClientID,
		"X-Client-Version":   "0.0.1",
		"X-Device-Id":        f.opt.DeviceID,
	}
	if f.opt.CaptchaToken != "" {
		headers["X-Captcha-Token"] = f.opt.CaptchaToken
	}
	return headers
}

func (f *Fs) apiHeaders() headerMap {
	return headerMap{
		"Accept":       "application/json, text/plain, */*",
		"Content-Type": "application/json",
		"Did":          f.opt.DeviceID,
		"Dt":           "4",
	}
}

func (f *Fs) saveTokens() error {
	if err := config.SetValueAndSave(f.originalName, "access_token", f.opt.AccessToken); err != nil {
		return err
	}
	if err := config.SetValueAndSave(f.originalName, "refresh_token", f.opt.RefreshToken); err != nil {
		return err
	}
	if err := config.SetValueAndSave(f.originalName, "verify_code", f.opt.VerifyCode); err != nil {
		return err
	}
	return config.SetValueAndSave(f.originalName, "verification_id", f.opt.VerificationID)
}

func (f *Fs) saveSMSState() error {
	if err := config.SetValueAndSave(f.originalName, "captcha_token", f.opt.CaptchaToken); err != nil {
		return err
	}
	if err := config.SetValueAndSave(f.originalName, "verification_id", f.opt.VerificationID); err != nil {
		return err
	}
	if err := config.SetValueAndSave(f.originalName, "send_code", fmt.Sprintf("%v", f.opt.SendCode)); err != nil {
		return err
	}
	return nil
}

func normalizePhoneE164(phone string) string {
	p := strings.TrimSpace(phone)
	if p == "" {
		return ""
	}
	p = strings.ReplaceAll(p, " ", "")
	if strings.HasPrefix(p, "+") {
		if strings.HasPrefix(p, "+86") && len(p) > 3 {
			return "+86 " + strings.TrimPrefix(p, "+86")
		}
		return p
	}
	digits := normalizeCaptchaUsername(p)
	if len(digits) == 11 {
		return "+86 " + digits
	}
	return p
}

func normalizeCaptchaUsername(phone string) string {
	p := strings.TrimSpace(phone)
	p = strings.ReplaceAll(p, " ", "")
	p = strings.TrimPrefix(p, "+")
	buf := make([]byte, 0, len(p))
	for i := 0; i < len(p); i++ {
		ch := p[i]
		if ch >= '0' && ch <= '9' {
			buf = append(buf, ch)
		}
	}
	digits := string(buf)
	if strings.HasPrefix(digits, "86") && len(digits) > 11 {
		digits = digits[2:]
	}
	return digits
}

func normalizeDeviceID(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	v = strings.ReplaceAll(v, "-", "")
	if len(v) != 32 {
		return ""
	}
	for i := 0; i < len(v); i++ {
		ch := v[i]
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return ""
		}
	}
	return v
}

func randomDeviceID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		now := time.Now().UnixNano()
		return fmt.Sprintf("%032x", now)
	}
	return hex.EncodeToString(buf[:])
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func normalizeOSSEndpoint(endpoint, bucket string) string {
	ep := strings.TrimSpace(endpoint)
	if ep == "" {
		return ep
	}
	if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
		ep = "https://" + ep
	}
	u, err := url.Parse(ep)
	if err != nil || u.Host == "" {
		return ep
	}
	host := u.Host
	if bucket != "" && strings.HasPrefix(host, bucket+".") {
		host = strings.TrimPrefix(host, bucket+".")
	}
	u.Host = host
	return u.String()
}
