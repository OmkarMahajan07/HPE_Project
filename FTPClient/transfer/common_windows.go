//go:build windows
// +build windows

package transfer

import (
	"os"

	"golang.org/x/sys/windows"
)

func TryExclusiveLock(file *os.File) bool {
	if file == nil {
		return false
	}
	h := windows.Handle(file.Fd())
	ol := new(windows.Overlapped)
	const LOCKFILE_EXCLUSIVE_LOCK = 0x00000002
	const LOCKFILE_FAIL_IMMEDIATELY = 0x00000001
	err := windows.LockFileEx(h, LOCKFILE_EXCLUSIVE_LOCK|LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, ol)
	return err == nil
}

func UnlockFile(file *os.File) {
	if file == nil {
		return
	}
	h := windows.Handle(file.Fd())
	ol := new(windows.Overlapped)
	const maxUint32 = ^uint32(0)
	_ = windows.UnlockFileEx(h, 0, maxUint32, maxUint32, ol)
}
