//go:build !windows
// +build !windows

package transfer

import (
	"os"
	"syscall"
)

func TryExclusiveLock(file *os.File) bool {
	if file == nil {
		return false
	}
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil
}

func UnlockFile(file *os.File) {
	if file == nil {
		return
	}
	_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}
