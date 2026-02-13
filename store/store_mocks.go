package store

import (
	"io/fs"
	"os"
)

const (
	MockLockExc = iota
	MockLockShr
)

type MockFile struct {
	Real *os.File
}

func (f MockFile) Write(b []byte) (n int, err error) {
	// partial write
	n, err = f.Real.Write(b[:len(b)-2])
	n -= 2
	return
}
func (f MockFile) Close() error {
	return f.Real.Close()
}
func (m MockFile) Sync() error {
	return m.Real.Sync()
}
func (m MockFile) Read(p []byte) (int, error) {
	return m.Real.Read(p)
}
func (m MockFile) Seek(offset int64, whence int) (int64, error) {
	return m.Real.Seek(offset, whence)
}
func (m MockFile) Truncate(size int64) error {
	return m.Real.Truncate(size)
}

type MockFileSystem struct {
	lockedPathes             map[string]int
	StatFunc                 func(name string) (fs.FileInfo, error)
	CreateTempFunc           func(dir, pattern string) (*os.File, error)
	CreateFunc               func(path string) (*os.File, error)
	RemoveFunc               func(name string) error
	ReadDirFunc              func(name string) ([]fs.DirEntry, error)
	acquireExclusiveLockFunc func(directory string) (*os.File, error)
	acquireSharedLockFunc    func(directory string) (*os.File, error)
	getNewFileIdFunc         func(directory string) (int, error)
}

func (m MockFileSystem) getNewFileId(directory string) (int, error) {
	if m.getNewFileIdFunc != nil {
		return m.getNewFileIdFunc(directory)
	}
	return 0, nil

}

func (m MockFileSystem) acquireExclusiveLock(directory string) (*os.File, error) {
	if m.acquireExclusiveLockFunc != nil {
		return m.acquireExclusiveLockFunc(directory)
	}
	return nil, nil
}

func (m MockFileSystem) acquireSharedLock(directory string) (*os.File, error) {
	if m.acquireSharedLockFunc != nil {
		return m.acquireSharedLockFunc(directory)
	}
	return nil, nil
}

func (m MockFileSystem) isEcxlusiveLocked(path string) bool {
	l, ok := m.lockedPathes[path]
	return ok && l == MockLockExc
}

func (m MockFileSystem) isSharedLocked(path string) bool {
	l, ok := m.lockedPathes[path]
	return ok && l == MockLockShr
}

func (m MockFileSystem) Open(name string) (*os.File, error) {
	return nil, fs.ErrNotExist
}

func (m MockFileSystem) Stat(name string) (fs.FileInfo, error) {
	if m.StatFunc != nil {
		return m.StatFunc(name)
	}
	return nil, fs.ErrNotExist
}

func (m MockFileSystem) CreateTemp(dir string, pattern string) (*os.File, error) {
	if m.CreateTempFunc != nil {
		return m.CreateTempFunc(dir, pattern)
	}
	return nil, nil
}
func (m MockFileSystem) Create(path string) (*os.File, error) {
	// if m.CreateTempFunc != nil {
	// 	return m.CreateFunc(path)
	// }
	return nil, nil
}

func (m MockFileSystem) ReadDir(name string) ([]fs.DirEntry, error) {
	if m.ReadDirFunc != nil {
		return m.ReadDirFunc(name)
	}
	return nil, nil
}

func (m MockFileSystem) Remove(name string) error {
	if m.RemoveFunc != nil {
		return m.RemoveFunc(name)
	}
	return nil
}
