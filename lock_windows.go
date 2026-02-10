//build: windows

package main

import (
	"golang.org/x/sys/windows"
	"os"
)

func Lock(f *os.File) error {
	fd := windows.Handle(f.Fd())
	var ov windows.Overlapped

	err := windows.LockFileEx(fd, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, 1, 0, &ov)

	if err != nil {
		if err == windows.ERROR_LOCK_VIOLATION {
			return ErrStoreLocked
		}
		return err
	}

	return nil
}

func LockShared(f *os.File) error {
	fd := windows.Handle(f.Fd())
	var ov windows.Overlapped

	err := windows.LockFileEx(fd, windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, 1, 0, &ov)

	if err != nil {
		if err == windows.ERROR_LOCK_VIOLATION {
			return ErrStoreLocked
		}
		return err
	}

	return nil
}

func Unlock(f *os.File) error {
	fd := windows.Handle(f.Fd())
	var ov windows.Overlapped
	return windows.UnlockFileEx(fd, 0, 1, 0, &ov)
}
