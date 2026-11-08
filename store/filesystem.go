package store

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

type FileSystem interface {
	Open(name string) (*os.File, error)
	CreateTemp(dir string, pattern string) (*os.File, error)
	Create(path string) (*os.File, error)
	Remove(name string) error
	ReadDir(name string) ([]fs.DirEntry, error)
	acquireExclusiveLock(directory string) (*os.File, error)
	acquireSharedLock(directory string) (*os.File, error)
	unlockAcquiredLock(lockFile *os.File) error
	OpenFile(name string, flag int, perm os.FileMode) (*os.File, error)
	Rename(oldpath string, newpath string) error
	Stat(name string) (os.FileInfo, error)
}

type OSFileSystem struct{}

func (fs OSFileSystem) unlockAcquiredLock(lockFile *os.File) error {
	return Unlock(lockFile)
}

func (fs OSFileSystem) acquireExclusiveLock(directory string) (*os.File, error) {
	filePath := filepath.Join(directory, ".lock")

	lockFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		return nil, fmt.Errorf("cannot create lock file: %w", err)
	}

	if err := Lock(lockFile); err != nil {
		lockFile.Close()
		return nil, err
	}

	lockFile.Truncate(0)
	lockFile.Seek(0, 0)
	fmt.Fprintf(lockFile, "%d\n", os.Getpid())
	lockFile.Sync()

	return lockFile, nil
}
func (fs OSFileSystem) acquireSharedLock(directory string) (*os.File, error) {
	filePath := filepath.Join(directory, ".lock")

	lockFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		return nil, fmt.Errorf("cannot create lock file: %w", err)
	}

	if err := LockShared(lockFile); err != nil {
		lockFile.Close()
		return nil, err
	}
	return lockFile, nil
}

func (OSFileSystem) CreateTemp(dir string, pattern string) (*os.File, error) {
	return os.CreateTemp(dir, pattern)
}
func (OSFileSystem) Create(path string) (*os.File, error) {
	return os.Create(path)
}
func (OSFileSystem) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

func (OSFileSystem) Open(name string) (*os.File, error) {
	return os.Open(name)
}
func (OSFileSystem) Rename(oldpath string, newpath string) error {
	return os.Rename(oldpath, newpath)
}
func (OSFileSystem) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (OSFileSystem) ReadDir(name string) ([]fs.DirEntry, error) {
	return os.ReadDir(name)
}

func (OSFileSystem) Remove(name string) error {
	return os.Remove(name)
}
