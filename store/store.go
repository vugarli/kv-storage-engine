package store

import (
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	KEY_SIZE_SIZE   = 4
	VALUE_SIZE_SIZE = 4
	CRC_SIZE        = 4
	TSTAMP_SIZE     = 8
	VALUE_POS_SIZE  = 8
	HEADER_SIZE     = 20

	CRC_OFFSET        = 0
	TIMESTAMP_OFFSET  = 4
	KEY_SIZE_OFFSET   = 12
	VALUE_SIZE_OFFSET = 16
	KEY_OFFSET        = 20
)

type FileLike interface {
	Write([]byte) (int, error)
	Close() error
	Sync() error
	io.ReadWriteCloser
	io.Seeker
	Truncate(size int64) error
}

// Its assumed that single entry will never be bigger than 4GB

const MAXIMUM_FILE_SIZE = 2 * 1024 * 1024 * 1024        // 2GB
const MAXIMUM_MERGED_FILE_SIZE = 4 * 1024 * 1024 * 1024 // 4GB
const MAX_KEY_SIZE = 1 * 1024                           // 1Kb
const MAX_VALUE_SIZE = 1 * 1024 * 1024 * 1024           // 1GB

var (
	ErrEmptyKey                       = errors.New("key cannot be empty")
	ErrKeyTooLarge                    = errors.New("key exceeds maximum size")
	ErrValueTooLarge                  = errors.New("value exceeds maximum size")
	ErrEntryTooLarge                  = errors.New("entry exceeds maximum file size")
	ErrKeyNotFound                    = errors.New("key not found")
	ErrStoreDirectoryNotFound         = errors.New("Specified store directory doesn't exist error")
	ErrStoreDirectoryPermissionDenied = errors.New("Permission denied error")
	ErrStoreLocked                    = errors.New("Store is locked")
	ErrCorruptedState                 = errors.New("Store operation failed. Needs rollback")
)

type KeyDir map[string]EntryRecord

type store struct {
	DirectoryName string
	entryIndex    KeyDir
	lockFile      *os.File
	currentFileId int
	mu            sync.RWMutex
	currentFile   FileLike
	currentSize   uint32
	syncOnPut     bool
	fileSystem    FileSystem
	readFileCache fileHandlerCache
	nextId        func() (int, bool)
}

type ROStore struct{ *store }
type RWStore struct{ *store }

type option func(*store)

func Open(directory string, syncOnPut bool, options ...option) (*RWStore, error) {
	ok := false // success flag for cleanup in case of panic

	s := &store{
		DirectoryName: directory,
		syncOnPut:     syncOnPut,
		fileSystem:    OSFileSystem{},
		readFileCache: fileHandlerCache{cache: make(map[int]*os.File, 0)}}

	if len(options) != 0 {
		for _, o := range options {
			o(s)
		}
	}

	if err := validateReadPermission(directory, s.fileSystem); err != nil {
		return nil, err
	}
	if err := validateWritePermission(directory, s.fileSystem); err != nil {
		return nil, err
	}

	lockFile, err := s.fileSystem.acquireExclusiveLock(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire exclusive lock: %w", err)
	}
	s.lockFile = lockFile

	// panic cleanup
	defer func() {
		if !ok {
			if lockFile != nil {
				s.fileSystem.unlockAcquiredLock(lockFile)
				lockFile.Close()
			}
			if s.currentFile != nil {
				s.currentFile.Close()
			}
		}
	}()

	dataFileIds, err := fileIds(directory, s.fileSystem)
	if err != nil {
		return nil, fmt.Errorf("Error while getting dataFileIds in dir:%s :%w", directory, err)
	}

	keyDir, err := generateKeyDirFromFileIds(directory, dataFileIds, s.fileSystem)
	if err != nil {
		return nil, fmt.Errorf("Error while getting keyDir in dir:%s :%w", directory, err)
	}
	s.entryIndex = keyDir

	newFileId := getMaxFileId(dataFileIds)
	seq := idSequence(newFileId)
	next, _ := iter.Pull(seq)
	s.nextId = next

	s.currentFileId = s.acquireNextId()

	newFile, err := createNewDataFile(newFileId, directory, s.fileSystem)
	if err != nil {
		return nil, fmt.Errorf("Failed creating initial data file: %w", err)
	}
	s.currentFile = newFile

	store := &RWStore{store: s}
	ok = true
	return store, nil
}

// Gets next unique incremental Id from iterator.
func (s *store) acquireNextId() int {
	id, ok := s.nextId()
	if !ok {
		panic("file id sequence exhausted")
	}
	return id
}

func getMaxFileId(dataFileIds []int) int {
	var newFileId int
	if len(dataFileIds) != 0 {
		newFileId = slices.Max(dataFileIds) + 1
	}
	return newFileId
}

func OpenReadOnly(directory string, options ...option) (*ROStore, error) {
	ok := false // success flag for cleanup in case of panic
	s := &store{DirectoryName: directory, fileSystem: OSFileSystem{}, readFileCache: fileHandlerCache{cache: make(map[int]*os.File, 0)}}

	if len(options) != 0 {
		for _, o := range options {
			o(s)
		}
	}

	if err := validateReadPermission(directory, s.fileSystem); err != nil {
		return nil, err
	}
	lockFile, err := s.fileSystem.acquireSharedLock(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire shared lock: %w", err)
	}
	s.lockFile = lockFile

	// panic cleanup
	defer func() {
		if !ok {
			if lockFile != nil {
				s.fileSystem.unlockAcquiredLock(lockFile)
				lockFile.Close()
			}
			if s.currentFile != nil {
				s.currentFile.Close()
			}
		}
	}()

	dataFileIds, err := fileIds(directory, s.fileSystem)
	if err != nil {
		return nil, fmt.Errorf("Error while getting dataFileIds in dir:%s :%w", directory, err)
	}

	keyDir, err := generateKeyDirFromFileIds(directory, dataFileIds, s.fileSystem)
	if err != nil {
		return nil, fmt.Errorf("Error while getting keyDir in dir:%s :%w", directory, err)
	}
	s.entryIndex = keyDir

	newFileId := getMaxFileId(dataFileIds)
	s.currentFileId = newFileId

	store := &ROStore{store: s}

	ok = true
	return store, nil
}

// Unlocks directory. Closes currentFile. Clears read file handler cache. Clears junk in current dir.
func (s *store) Close() error {
	if s.lockFile != nil {
		s.fileSystem.unlockAcquiredLock(s.lockFile)
		s.lockFile.Close()
	}
	if s.currentFile != nil {
		s.currentFile.Close()
	}
	s.readFileCache.evictall()
	return s.cleanJunk()
}

// ACQUIRES/RELEASES LOCK
// Initializes an entry. Writes the entry. Updates the KeyDir. Checks file size constraint and
// rotates file if necessary.
func (s *RWStore) Put(key string, value []byte) error {

	if err := validatePut(key, value); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	keyByte := []byte(key)
	timeStamp := uint64(time.Now().Unix())
	entry := initEntry(keyByte, []byte(value), timeStamp)
	record, err := s.writeEntry(entry, key, timeStamp)
	if err != nil {
		return err
	}
	s.updateKeyDir(keyByte, record)
	s.currentSize += uint32(len(entry))
	if s.currentSize >= MAXIMUM_FILE_SIZE {
		if err := s.rotateFile(); err != nil {
			return fmt.Errorf("Warning: File Rotation failed: %w", err)
		}
	}

	return nil
}

// REQUIRES KEYDIR LOCK
func (s *RWStore) updateKeyDir(keyByte []byte, record *EntryRecord) {
	s.entryIndex[string(keyByte)] = *record
}

func (s *store) Get(key string) ([]byte, error) {
	if key == "" {
		return nil, fmt.Errorf("Key can't be empty string")
	}

	s.mu.RLock()
	entryRecord, exists := s.entryIndex[key]
	directoryName := s.DirectoryName
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("key: %s not found", key)
	}

	readFile, err := s.readFileCache.get(directoryName, entryRecord.FileId)
	if err != nil {
		return nil, fmt.Errorf("opening data file: %w", err)
	}

	var buf = make([]byte, entryRecord.ValueSize)

	n, err := readFile.ReadAt(buf, int64(entryRecord.ValuePos))

	if err != nil && err != io.EOF {
		return nil, err
	}

	if n != len(buf) {
		return nil, io.ErrUnexpectedEOF
	}

	return buf, nil
}

// ACQUIRES/RELEASES LOCK
func (s *store) ListKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Collect(maps.Keys(s.entryIndex))
}

// ACQUIRES/RELEASES LOCK
func (s *RWStore) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.currentFile == nil {
		return fmt.Errorf("Current file is nil")
	}
	if err := s.currentFile.Sync(); err != nil {
		return fmt.Errorf("syncing data file: %w", err)
	}
	return nil
}

// ACQUIRES/RELEASES LOCK
func (s *RWStore) Delete(key string) error {
	if key == "" {
		return ErrKeyNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.entryIndex[key]
	if !exists {
		return ErrKeyNotFound
	}

	timeStamp := uint64(time.Now().Unix())
	tombStoneEntry := initTombstoneEntry(key, timeStamp)

	if _, err := s.writeEntry(tombStoneEntry, key, timeStamp); err != nil {
		return err
	}
	delete(s.entryIndex, key)

	return nil
}

func extractFileId(a string) (int, error) {
	start := -1
	end := -1

	for i := 0; i < len(a); i++ {
		if a[i] >= '0' && a[i] <= '9' {
			if start == -1 {
				start = i
			}
			end = i + 1
		} else if start != -1 {
			break
		}
	}

	if start != -1 {
		digit := a[start:end]
		num, err := strconv.Atoi(digit)
		if err != nil {
			return 0, fmt.Errorf("data file format is wrong: %w", err)
		}
		return num, nil
	}
	return 0, fmt.Errorf("data file format is wrong")
}

// Given inactive files, and dir, updates KeyDir
func generateKeyDirFromFileIds(directory string, inactiveDataFileIds []int, fileSystem FileSystem) (KeyDir, error) {
	keyDir := make(KeyDir)

	for _, dataFileId := range inactiveDataFileIds {
		dataFileName := fmt.Sprintf("%d.data", dataFileId)
		dataFilepath := filepath.Join(directory, dataFileName)

		hintFileName := fmt.Sprintf("%d.hint", dataFileId)
		hintFilepath := filepath.Join(directory, hintFileName)

		if _, err := fileSystem.Stat(hintFilepath); err == nil {
			loadKeyDirFromHintFile(hintFilepath, keyDir, dataFileId, fileSystem)
			// log
		} else {
			if err := loadKeyDirFromDataFile(dataFilepath, keyDir, fileSystem); err != nil {
				return nil, fmt.Errorf("Warning: error loading %s: %v", dataFileName, err)
			}
		}
	}
	return keyDir, nil
}

func loadKeyDirFromHintFile(filePath string, index KeyDir, fileId int, fileSystem FileSystem) error {
	hintFile, err := fileSystem.Open(filePath)
	if err != nil {
		return fmt.Errorf("Error opening hint file to load keydir: %w", err)
	}
	defer hintFile.Close()

	offset := int64(0)

	for {
		hintEntryHeader, key, err := readHintEntry(hintFile, offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		existing, exists := index[key]
		if !exists || (exists && hintEntryHeader.Timestamp > existing.Timestamp) {
			index[key] = EntryRecord{
				FileId:    fileId,
				ValueSize: hintEntryHeader.ValueSize,
				ValuePos:  hintEntryHeader.ValuePos,
				Timestamp: hintEntryHeader.Timestamp,
				KeySize:   uint32(len(key)),
			}
		}
		offset += int64(len(key) + HINT_HEADER_SIZE)
	}
	return nil
}

func loadKeyDirFromDataFile(filePath string, keyDir KeyDir, fileSystem FileSystem) error {
	filename := filepath.Base(filePath)
	fileId, err := extractFileId(filename)
	if err != nil {
		return fmt.Errorf("extracting file ID from %s: %w", filename, err)
	}

	file, err := fileSystem.Open(filePath)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}
	defer file.Close()

	offset := uint64(0)

	for {

		entry, entryHeader, err := readEntry(file, offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		var keyByte []byte
		if keyByte, err = ExtractKeyGivenHeader(entry, *entryHeader); err != nil {
			return err
		}

		key := string(keyByte)

		isTomb, err := isTombStoneEntry(entry)
		if err != nil {
			fmt.Printf("Error while checking if entry is tombstone entry: %v", err)
		}
		if isTomb {
			delete(keyDir, key)
			offset += uint64(len(entry))
			continue
		}

		valuePos := offset + uint64(HEADER_SIZE) + uint64(entryHeader.KeySize)

		existing, exists := keyDir[key]
		if !exists || (exists && entryHeader.Timestamp > existing.Timestamp) {
			keyDir[key] = EntryRecord{
				FileId:    fileId,
				ValueSize: entryHeader.ValueSize,
				ValuePos:  uint64(valuePos),
				Timestamp: entryHeader.Timestamp,
				KeySize:   uint32(len(key)),
			}
		}
		offset += uint64(len(entry))
	}

	return nil
}

// Cleans files that are not present in current KeyDir
func (s *store) cleanJunk() error {
	dataIds, err := inactiveFileIds(s.DirectoryName, s.currentFileId, s.fileSystem)
	if err != nil {
		return err
	}

	inUseDataIds := make(map[int]struct{})
	inUseDataIds[s.currentFileId] = struct{}{}
	for entry := range maps.Values(s.entryIndex) {
		inUseDataIds[entry.FileId] = struct{}{}
	}

	for _, dataId := range dataIds {
		_, exists := inUseDataIds[dataId]
		if !exists {
			fileName := filepath.Join(s.DirectoryName, fmt.Sprintf("%d.data", dataId))
			s.readFileCache.evict(dataId)
			s.fileSystem.Remove(fileName)
		}
	}
	return nil
}

// Next invocation returns startId, and furhter invocations returns incrementally
func idSequence(startId int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for id := startId; ; id++ {
			if !yield(id) {
				return
			}
		}
	}
}

func fileIds(directory string, fileSystem FileSystem) ([]int, error) {
	dirEntries, err := fileSystem.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	ids := make([]int, 0)
	for _, entry := range dirEntries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".data") {
			continue
		}
		id, err := extractFileId(entry.Name())
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}
func inactiveFileIds(directory string, currentFileId int, fileSystem FileSystem) ([]int, error) {
	ids, err := fileIds(directory, fileSystem)
	if err != nil {
		return nil, err
	}
	inactive := make([]int, 0, len(ids))
	for _, id := range ids {
		if id != currentFileId {
			inactive = append(inactive, id)
		}
	}
	return inactive, nil
}

// Needs caller to hold Lock as it mutates store state
func (s *RWStore) rotateFile() error {

	if s.currentFile != nil {
		if err := s.currentFile.Sync(); err != nil {
			return fmt.Errorf("failed to rotate current file. Sync error:%w", err)
		}
		s.currentFile.Close()
	}
	//TODO refactor. new file gen + currentId and size needs to be isolated

	// Acquire new Id, and reset store size to 0
	s.currentFileId = s.acquireNextId()
	s.currentSize = 0

	newFile, err := createNewDataFile(s.currentFileId, s.DirectoryName, s.fileSystem)
	if err != nil {
		return err
	}

	s.currentFile = newFile
	return nil
}
func createNewDataFile(newFileId int, directory string, fileSystem FileSystem) (*os.File, error) {
	newFilePath := filepath.Join(directory, fmt.Sprintf("%d.data", newFileId))
	f, err := fileSystem.Create(newFilePath)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func validatePut(key string, value []byte) error {
	if key == "" {
		return ErrEmptyKey
	}

	if len(key) > MAX_KEY_SIZE {
		return fmt.Errorf("key size %d exceeds maximum %d: %w",
			len(key), MAX_KEY_SIZE, ErrKeyTooLarge)
	}

	if len(value) > MAX_VALUE_SIZE {
		return fmt.Errorf("value size %d exceeds maximum %d: %w",
			len(value), MAX_VALUE_SIZE, ErrValueTooLarge)
	}

	if uint64(len(value)+len(key)+HEADER_SIZE) > uint64(math.MaxUint32) {
		return fmt.Errorf("entry size %d exceeds uint32 max: %w",
			len(value), ErrValueTooLarge)
	}

	entrySize := HEADER_SIZE + len(key) + len(value)
	if int64(entrySize) > MAXIMUM_FILE_SIZE {
		return fmt.Errorf("entry size %d exceeds max file size %d: %w",
			entrySize, MAXIMUM_FILE_SIZE, ErrEntryTooLarge)
	}

	return nil
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
