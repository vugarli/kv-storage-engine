package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type FileSystem interface {
	Open(name string) (*os.File, error)
	CreateTemp(dir string, pattern string) (*os.File, error)
	Create(path string) (*os.File, error)
	Remove(name string) error
	ReadDir(name string) ([]fs.DirEntry, error)
	acquireExclusiveLock(directory string) (*os.File, error)
	acquireSharedLock(directory string) (*os.File, error)
	getNewFileId(directory string) (uint32, error)
}

type OSFileSystem struct{}

func (OSFileSystem) CreateTemp(dir string, pattern string) (*os.File, error) {
	return os.CreateTemp(dir, pattern)
}
func (OSFileSystem) Create(path string) (*os.File, error) {
	return os.Create(path)
}

func (OSFileSystem) Open(name string) (*os.File, error) {
	return os.Open(name)
}

func (OSFileSystem) ReadDir(name string) ([]fs.DirEntry, error) {
	return os.ReadDir(name)
}

func (OSFileSystem) Remove(name string) error {
	return os.Remove(name)
}

const (
	KEY_SIZE_SIZE   = 4
	VALUE_SIZE_SIZE = 4
	CRC_SIZE        = 4
	TSTAMP_SIZE     = 8
	HEADER_SIZE     = 20

	CRC_OFFSET        = 0
	TIMESTAMP_OFFSET  = 4
	KEY_SIZE_OFFSET   = 12
	VALUE_SIZE_OFFSET = 16
	KEY_OFFSET        = 20
)

type LatestEntryRecord struct {
	FileId    uint32
	ValueSize uint32
	ValuePos  uint64
	Timestamp uint64
}

// 	Get(key string) ([]byte, error)
// 	ListKeys() ([]string, error)

// 	Put(key string, value []byte) error
// 	Delete(key string) error
// 	Merge(directoryName string) error
// 	Sync() error
// 	Close() error

type store struct {
	DirectoryName string
	KeyDir        map[string]LatestEntryRecord
	lockFile      *os.File
	currentFileId uint32
	mu            sync.RWMutex
	fileSystem    FileSystem
	currentFile   *os.File
}

type Store struct {
	*store
	syncOnPut bool
}

type ReadOnlyStore struct {
	*store
}

func (s *Store) writeEntry(entry []byte, key string, value []byte, timestamp uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.currentFile == nil {
		return fmt.Errorf("Store file is not initialized")
	}

	keyByte := []byte(key)

	position, err := s.currentFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("getting file position: %w", err)
	}
	n, err := s.currentFile.Write(entry)
	if err != nil || n != len(entry) {
		// rollback?
		return fmt.Errorf("Entry write op failed: %w", err)
	}
	valuePosition := position + int64(HEADER_SIZE) + int64(len(keyByte))

	s.KeyDir[string(keyByte)] = LatestEntryRecord{
		FileId:    s.currentFileId,
		ValueSize: uint32(len(value)),
		ValuePos:  uint64(valuePosition),
		Timestamp: timestamp}

	if s.syncOnPut {
		if err := s.currentFile.Sync(); err != nil {
			return fmt.Errorf("syncing file: %w", err)
		}
	}

	// file rotation? if bigger than max size

	return nil

}

func (s *Store) Put(key string, value []byte) error {
	keyByte := []byte(key)
	timeStamp := uint64(time.Now().Unix())
	entry := InitEntry(keyByte, []byte(value), timeStamp)
	return s.writeEntry(entry, key, value, timeStamp)
}

func (*store) Get(key string) ([]byte, error) {
	return nil, nil
}
func (*store) ListKeys() ([]string, error) {
	return nil, nil
}

func (*Store) Delete(key string) error {
	return nil
}

func (*Store) Merge(directoryName string) error {
	return nil
}
func (*Store) Sync() error {
	return nil
}

var (
	ErrInvalidStoreOperation          = errors.New("Invalid store operation error")
	ErrStoreDirectoryNotFound         = errors.New("Specified store directory doesn't exist error")
	ErrStoreDirectoryPermissionDenied = errors.New("Permission denied error")
	ErrStoreLocked                    = errors.New("Store is locked")
)

func (of OSFileSystem) acquireExclusiveLock(directory string) (*os.File, error) {
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

func (of OSFileSystem) acquireSharedLock(directory string) (*os.File, error) {
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

func (s *store) Close() error {
	if s.lockFile != nil {
		Unlock(s.lockFile)
		s.lockFile.Close()
	}

	if s.currentFile != nil {
		s.currentFile.Close()
	}

	return nil
}

func (of OSFileSystem) getNewFileId(directory string) (uint32, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return 0, fmt.Errorf("reading the directory: %w", err)
	}
	var maxId uint32 = 0
	foundAny := false
	for _, entry := range entries {
		fi, err := entry.Info()
		if err != nil {
			return 0, fmt.Errorf("reading the file: %w", err)
		}
		if s, f := strings.CutSuffix(fi.Name(), ".data"); f {
			if id, err := strconv.ParseUint(s, 10, 32); err == nil {
				foundAny = true
				if uint32(id) > maxId {
					maxId = uint32(id)
				}
			}
		}
	}
	if foundAny {
		return maxId + 1, nil
	}
	return maxId, nil
}

func createNewDataFile(newFileId uint32, directory string, fileSystem FileSystem) (*os.File, error) {
	newFilePath := filepath.Join(directory, fmt.Sprintf("%d.data", newFileId))
	f, err := fileSystem.Create(newFilePath)
	if err != nil {
		return nil, err
	}
	// if f != nil {
	// 	f.Close()
	// }
	return f, nil
}

func Open(directory string, fileSystem FileSystem, syncOnPut bool) (*Store, error) {
	if err := validateReadPermission(directory, fileSystem); err != nil {
		return nil, err
	}
	if err := validateWritePermission(directory, fileSystem); err != nil {
		return nil, err
	}

	lockFile, err := fileSystem.acquireExclusiveLock(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire exclusive lock: %w", err)
	}

	newFileId, err := fileSystem.getNewFileId(directory)
	if err != nil {
		return nil, fmt.Errorf("getting a new file Id for store: %w", err)
	}

	newFile, err := createNewDataFile(newFileId, directory, fileSystem)
	if err != nil {
		return nil, fmt.Errorf("Failed creating initial data file: %w", err)
	}

	return &Store{
		store: &store{
			DirectoryName: directory,
			KeyDir:        make(map[string]LatestEntryRecord),
			lockFile:      lockFile,
			currentFileId: newFileId,
			fileSystem:    fileSystem,
			currentFile:   newFile,
		},
		syncOnPut: syncOnPut}, nil
}

func OpenReadOnly(directory string, fileSystem FileSystem) (*ReadOnlyStore, error) {
	if err := validateReadPermission(directory, fileSystem); err != nil {
		return nil, err
	}
	lockFile, err := fileSystem.acquireSharedLock(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire shared lock: %w", err)
	}

	newFileId, err := fileSystem.getNewFileId(directory)
	if err != nil {
		return nil, fmt.Errorf("getting a new file Id for store: %w", err)
	}

	//TODO: populate keydir

	return &ReadOnlyStore{
		store: &store{
			DirectoryName: directory,
			KeyDir:        make(map[string]LatestEntryRecord),
			lockFile:      lockFile,
			currentFileId: newFileId,
			fileSystem:    fileSystem,
		}}, nil
}

func validateReadPermission(directory string, fileSystem FileSystem) error {
	_, err := fileSystem.ReadDir(directory)
	if err != nil {
		switch {
		case errors.Is(err, os.ErrNotExist):
			return fmt.Errorf("%s: %w", directory, ErrStoreDirectoryNotFound)
		case errors.Is(err, syscall.ENOTDIR):
			return fmt.Errorf("%s: %w", directory, ErrStoreDirectoryNotFound)
		case errors.Is(err, os.ErrPermission):
			return fmt.Errorf("%s: %w", directory, ErrStoreDirectoryPermissionDenied)
		default:
			return fmt.Errorf("cannot read directory %s: %w", directory, err)
		}
	}
	return nil
}

func validateWritePermission(directory string, fileSystem FileSystem) error {
	tmpFile, err := fileSystem.CreateTemp(directory, ".storecheck-*")
	if err != nil {
		switch {
		case errors.Is(err, os.ErrNotExist):
			return fmt.Errorf("%s: %w", directory, ErrStoreDirectoryNotFound)
		case errors.Is(err, syscall.ENOTDIR):
			return fmt.Errorf("%s: %w", directory, ErrStoreDirectoryNotFound)
		case errors.Is(err, os.ErrPermission):
			return fmt.Errorf("%s: %w", directory, ErrStoreDirectoryPermissionDenied)
		default:
			return fmt.Errorf("cannot write to directory %s: %w", directory, err)
		}
	}
	if tmpFile != nil {
		tmpFile.Close()
		fileSystem.Remove(tmpFile.Name())
	}
	return nil
}

func InitEntry(key, value []byte, timeStamp uint64) []byte {
	// CRC TSTAMP KSZ VSZ K V
	totalSize := CRC_SIZE + TSTAMP_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + len(key) + len(value)
	buf := make([]byte, totalSize)

	valueOffset := KEY_OFFSET + len(key)
	binary.LittleEndian.PutUint64(buf[TIMESTAMP_OFFSET:], timeStamp)
	binary.LittleEndian.PutUint32(buf[KEY_SIZE_OFFSET:], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[VALUE_SIZE_OFFSET:], uint32(len(value)))
	copy(buf[KEY_OFFSET:], key)
	copy(buf[valueOffset:], value)

	crc := crc32.ChecksumIEEE(buf[CRC_SIZE:])
	binary.LittleEndian.PutUint32(buf[CRC_OFFSET:], crc)

	return buf
}
