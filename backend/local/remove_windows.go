//go:build windows

package local

import (
	"os"
	"time"

	"github.com/rclone/rclone/fs"
	"golang.org/x/sys/windows"
)

// Removes name, retrying on a sharing violation
func remove(name string) (err error) {
	const maxTries = 10
	var sleepTime = 1 * time.Millisecond
	for i := 0; i < maxTries; i++ {
		err = os.Remove(name)
		if err == nil {
			break
		}
		pathErr, ok := err.(*os.PathError)
		if !ok {
			break
		}
		if pathErr.Err == windows.ERROR_ACCESS_DENIED {
			// Read-only files on Windows return access denied on delete.
			// Clear the attribute once, then let the loop retry removal.
			_ = os.Chmod(name, 0o666)
		}
		if pathErr.Err != windows.ERROR_SHARING_VIOLATION {
			if pathErr.Err != windows.ERROR_ACCESS_DENIED {
				break
			}
		}
		fs.Logf(name, "Remove retry %d/%d after %v due to %v", i+1, maxTries, sleepTime, pathErr.Err)
		time.Sleep(sleepTime)
		sleepTime <<= 1
	}
	return err
}
