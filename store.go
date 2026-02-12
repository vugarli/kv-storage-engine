package main

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"maps"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
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

type FileLike interface {
	Write([]byte) (int, error)
	Close() error
	Sync() error
	io.ReadWriteCloser
	io.Seeker
	Truncate(size int64) error
}

type EntryRecord struct {
	FileId    int
	ValueSize uint32
	ValuePos  uint64
	Timestamp uint64
	KeySize   uint32
}

type Entry struct {
	Key         string
	IsTombEntry bool
	ValueSize   uint32
	ValuePos    uint64
	Timestamp   uint64
}

// Its assumed that single entry will never be bigger than 4GB

const MAXIMUM_FILE_SIZE = 2 * 1024 * 1024 * 1024        // 2GB
const MAXIMUM_MERGED_FILE_SIZE = 4 * 1024 * 1024 * 1024 // 4GB
const MAX_KEY_SIZE = 1 * 1024                           // 1Kb
const MAX_VALUE_SIZE = 1 * 1024 * 1024 * 1024           // 1GB

var (
	ErrEmptyKey      = errors.New("key cannot be empty")
	ErrKeyTooLarge   = errors.New("key exceeds maximum size")
	ErrValueTooLarge = errors.New("value exceeds maximum size")
	ErrEntryTooLarge = errors.New("entry exceeds maximum file size")
)

type store struct {
	DirectoryName string
	KeyDir        map[string]EntryRecord
	lockFile      *os.File
	currentFileId int
	mu            sync.RWMutex
	fileSystem    FileSystem
	currentFile   FileLike
	currentSize   uint32
}

type Store struct {
	*store
	syncOnPut bool
}

type ReadOnlyStore struct {
	*store
}

func (s *Store) writeEntry(entry []byte, key string, value []byte, timestamp uint64) (*EntryRecord, error) {
	if key == "" {
		return nil, fmt.Errorf("Key can't be empty string")
	}
	if s.currentFile == nil {
		return nil, fmt.Errorf("Store file is not initialized")
	}

	keyByte := []byte(key)

	position, err := s.currentFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("getting file position: %w", err)
	}

	n, err := s.currentFile.Write(entry)

	if err != nil || n != len(entry) {
		truncErr := s.currentFile.Truncate(position)
		_, seekErr := s.currentFile.Seek(position, io.SeekStart)

		if truncErr != nil || seekErr != nil {
			return nil, fmt.Errorf(
				"Write failed and rollback failed: writeErr=%v truncErr=%v seekErr=%v",
				err, truncErr, seekErr,
			)
		}

		return nil, fmt.Errorf("Entry write op failed: %w", err)
	}

	valuePosition := position + int64(HEADER_SIZE) + int64(len(keyByte))

	entryRecord := EntryRecord{
		FileId:    s.currentFileId,
		ValueSize: uint32(len(value)),
		ValuePos:  uint64(valuePosition),
		Timestamp: timestamp,
		KeySize:   uint32(len(key)),
	}

	if s.syncOnPut {
		if err := s.currentFile.Sync(); err != nil {
			return nil, fmt.Errorf("syncing file: %w", err)
		}
	}

	return &entryRecord, nil
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
		num, _ := strconv.Atoi(digit)
		return num, nil
	}
	return 0, fmt.Errorf("data file format is wrong")
}

// Given inactive files, and updates KeyDir
func generateKeyDirFromFileIds(directory string, inactiveDataFileIds []int) (map[string]EntryRecord, error) {
	keyDir := make(map[string]EntryRecord)

	for _, dataFileId := range inactiveDataFileIds {
		dataFileName := fmt.Sprintf("%d.data", dataFileId)
		filepath := filepath.Join(directory, dataFileName)

		if err := populateKeyDirWEntriesFromFile(filepath, keyDir); err != nil {
			fmt.Printf("Warning: error loading %s: %v", dataFileName, err)
		}
	}
	return keyDir, nil
}

func isTombStoneEntry(header []byte) (bool, error) {
	parsedHeader, err := ParseEntryHeader(header)
	if err != nil {
		return false, err
	}
	timeStamp := parsedHeader.Timestamp

	return timeStamp>>63 == 1 && parsedHeader.ValueSize == 0, nil
}

func populateKeyDirWEntriesFromFile(filePath string, keyDir map[string]EntryRecord) error {
	filename := filepath.Base(filePath)
	fileId, err := extractFileId(filename)
	if err != nil {
		return fmt.Errorf("extracting file ID from %s: %w", filename, err)
	}

	file, err := os.Open(filePath)
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

func (s *Store) Put(key string, value []byte) error {

	if err := validatePut(key, value); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	keyByte := []byte(key)
	timeStamp := uint64(time.Now().Unix())
	entry := InitEntry(keyByte, []byte(value), timeStamp)
	record, err := s.writeEntry(entry, key, value, timeStamp)
	if err != nil {
		return err
	}
	s.KeyDir[string(keyByte)] = *record
	s.currentSize += uint32(len(entry))
	if s.currentSize >= MAXIMUM_FILE_SIZE {
		if err := s.rotateFile(); err != nil {
			return fmt.Errorf("Warning: File Rotation failed: %w", err)
		}
	}

	return nil
}

func (s *store) Get(key string) ([]byte, error) {
	if key == "" {
		return nil, fmt.Errorf("Key can't be empty string")
	}

	s.mu.RLock()
	entryRecord, exists := s.KeyDir[key]
	directoryName := s.DirectoryName
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("key: %s not found", key)
	}

	fileName := filepath.Join(directoryName, fmt.Sprintf("%d.data", entryRecord.FileId))

	readFile, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("opening data file: %w", err)
	}
	defer readFile.Close()

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

func (s *store) ListKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Collect(maps.Keys(s.KeyDir))
}

func (s *Store) Sync() error {
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

var (
	ErrKeyNotFound = errors.New("key not found")
)

func (s *Store) Delete(key string) error {
	if key == "" {
		return ErrKeyNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.KeyDir[key]
	if !exists {
		return ErrKeyNotFound
	}

	timeStamp := uint64(time.Now().Unix())
	tombStoneEntry := InitTombstoneEntry(key, timeStamp)

	if _, err := s.writeEntry(tombStoneEntry, key, []byte{}, timeStamp); err != nil {
		return err
	}
	delete(s.KeyDir, key)

	return nil
}

func (s *Store) Merge() error {
	var entries []MergeEntryRecord

	s.mu.Lock()

	for key, record := range maps.All(s.KeyDir) {
		if record.FileId != s.currentFileId {
			entries = append(entries, MergeEntryRecord{
				Record: record,
				Key:    key,
			})
		}
	}

	groups := groupEntriesFFD(entries, MAXIMUM_MERGED_FILE_SIZE)

	//TODO need rollback here
	results, err := s.saveGroups(groups)
	if err != nil {
		return fmt.Errorf("Error during writing groups to files:%w", err)
	}
	s.updateKeydirFromMergeResults(results)
	s.mu.Unlock()

	if err := s.cleanJunk(); err != nil {
		return fmt.Errorf("Error while cleaning junk data file names:%w", err)
	}
	return nil
}

// Cleans files that are not present in current KeyDir
func (s *store) cleanJunk() error {
	dataIds, err := inactiveFileIds(s.DirectoryName, OSFileSystem{}, s.currentFileId)
	if err != nil {
		return err
	}

	inUseDataIds := []int{s.currentFileId}
	for entry := range maps.Values(s.KeyDir) {
		if slices.Index(inUseDataIds, entry.FileId) == -1 {
			inUseDataIds = append(inUseDataIds, entry.FileId)
		}
	}
	for _, dataId := range dataIds {
		if slices.Index(inUseDataIds, dataId) == -1 {
			fileName := path.Join(s.DirectoryName, fmt.Sprintf("%d.data", dataId))
			os.Remove(fileName)
		}
	}
	return nil
}

func (s *store) updateKeydirFromMergeResults(results []MergeResult) {
	for _, result := range results {
		entry, e := s.KeyDir[result.Key]
		if e {
			entry.FileId = result.FileId
			entry.ValuePos = result.ValuePos
		}
		s.KeyDir[result.Key] = entry
	}
}

type MergeEntryRecord struct {
	Record EntryRecord
	Key    string
}
type MergeResult struct {
	Key      string
	ValuePos uint64
	FileId   int
}

// Writes groups to .data files. New fileIds gets incremented from currentFileId
func (s *store) saveGroups(groups [][]MergeEntryRecord) ([]MergeResult, error) {
	var length int
	for _, v := range groups {
		length += len(v)
	}
	result := make([]MergeResult, 0, length)
	nextFileId := s.currentFileId + 1
	for _, group := range groups {
		mergeResults, err := s.saveGroupToFile(group, nextFileId)
		if err != nil {
			return result, err
		}
		result = append(result, mergeResults...)
		nextFileId++
	}
	return result, nil
}

func (s *store) saveGroupToFile(group []MergeEntryRecord, fileId int) ([]MergeResult, error) {
	destinationFileName := path.Join(s.DirectoryName, fmt.Sprintf("%d.data", fileId))
	destinationFile, err := os.OpenFile(destinationFileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening destination file: %w", err)
	}
	defer destinationFile.Close()

	result := make([]MergeResult, 0, len(group))
	var currentOffset uint64
	for _, staleEntry := range group {
		originFile, err := os.Open(path.Join(s.DirectoryName, fmt.Sprintf("%d.data", staleEntry.Record.FileId)))
		if err != nil {
			return nil, fmt.Errorf("opening origin file: %w", err)
		}
		entry, entryHeader, err := readEntry(originFile, uint64(staleEntry.Record.ValuePos-HEADER_SIZE-uint64(len(staleEntry.Key))))
		originFile.Close()
		if err != nil {
			return nil, fmt.Errorf("reading entry: %w", err)
		}
		n, err := destinationFile.WriteAt(entry, int64(currentOffset))
		if err != nil {
			return nil, fmt.Errorf("writing entry: %w", err)
		}
		if n != len(entry) {
			return nil, fmt.Errorf("incomplete write: wrote %d of %d bytes", n, len(entry))
		}
		result = append(result, MergeResult{
			Key:      staleEntry.Key,
			ValuePos: uint64(entryHeader.ValueOffset),
			FileId:   fileId,
		})
		currentOffset += uint64(len(entry))
	}
	if err := destinationFile.Sync(); err != nil {
		return nil, fmt.Errorf("syncing destination file: %w", err)
	}
	return result, nil
}

func (e EntryRecord) EntrySize() int64 {
	return int64(HEADER_SIZE) + int64(e.KeySize) + int64(e.ValueSize)
}

// Groups entries in FFD order so that no group exceeds maxSize
func groupEntriesFFD(entries []MergeEntryRecord, maxSize int64) [][]MergeEntryRecord {
	sorted := make([]MergeEntryRecord, len(entries))
	copy(sorted, entries)
	slices.SortFunc(sorted, func(a, b MergeEntryRecord) int {
		return cmp.Compare(b.Record.EntrySize(), a.Record.EntrySize())
	})

	var groups [][]MergeEntryRecord
	var currentGroup []MergeEntryRecord
	currentSize := int64(0)

	for _, entry := range sorted {
		entrySize := entry.Record.EntrySize()

		if entrySize >= maxSize {
			if len(currentGroup) > 0 {
				groups = append(groups, currentGroup)
				currentGroup = nil
				currentSize = 0
			}
			groups = append(groups, []MergeEntryRecord{entry})
			continue
		}

		if currentSize+entrySize <= maxSize {
			currentGroup = append(currentGroup, entry)
			currentSize += entrySize
			continue
		}

		groups = append(groups, currentGroup)
		currentGroup = []MergeEntryRecord{entry}
		currentSize = entrySize
	}

	if len(currentGroup) > 0 {
		groups = append(groups, currentGroup)
	}

	return groups
}

func readEntry(reader io.ReaderAt, offset uint64) ([]byte, *EntryHeader, error) {
	headerBuf := make([]byte, HEADER_SIZE)
	n, err := reader.ReadAt(headerBuf, int64(offset))
	if err == io.EOF {
		return nil, nil, err
	}
	if err != nil {
		return nil, nil, fmt.Errorf("Read: %d/%d bytes. Reading header at offset %d: %w", n, HEADER_SIZE, offset, err)
	}
	entryHeader, err := ParseEntryHeader(headerBuf)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing header at offset %d: %w", offset, err)
	}
	dataSize := uint64(entryHeader.KeySize + entryHeader.ValueSize)
	dataBuf := make([]byte, dataSize)
	n, err = reader.ReadAt(dataBuf, int64(offset)+HEADER_SIZE)
	if err == io.EOF {
		return nil, nil, err
	}
	if err != nil {
		return nil, nil, fmt.Errorf("Read: %d/%d bytes. Reading entry data at offset %d: %w", n, HEADER_SIZE, offset, err)
	}
	fullEntry := make([]byte, HEADER_SIZE+dataSize)
	copy(fullEntry, headerBuf)
	copy(fullEntry[HEADER_SIZE:], dataBuf)

	if err := VerifyEntryCRC(fullEntry); err != nil {
		return nil, nil, fmt.Errorf("Warning: CRC mismatch at offset %d", offset)
	}
	//isTomb, err := isTombStoneEntry(fullEntry)
	// if err != nil {
	// 	fmt.Printf("Error while checking if entry is tombstone entry: %v", err)
	// }
	// if isTomb {
	// 	return nil, nil, fmt.Errorf("Entry is tomb entry!")
	// }
	return fullEntry, entryHeader, nil
}

func fileIds(directory string, fs FileSystem) ([]int, error) {
	dirEntries, err := fs.ReadDir(directory)
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

func inactiveFileIds(directory string, fs FileSystem, currentFileId int) ([]int, error) {
	ids, err := fileIds(directory, fs)
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

func (s *Store) rotateFile() error {

	if s.currentFile != nil {
		s.currentFile.Sync()
		s.currentFile.Close()
	}

	s.currentFileId++
	s.currentSize = 0

	newFile, err := createNewDataFile(s.currentFileId, s.DirectoryName, s.fileSystem)
	if err != nil {
		return err
	}

	s.currentFile = newFile
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

func createNewDataFile(newFileId int, directory string, fileSystem FileSystem) (*os.File, error) {
	newFilePath := filepath.Join(directory, fmt.Sprintf("%d.data", newFileId))
	f, err := fileSystem.Create(newFilePath)
	if err != nil {
		return nil, err
	}
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

	dataFileIds, err := fileIds(directory, fileSystem)
	if err != nil {
		return nil, fmt.Errorf("Error while getting dataFileIds in dir:%s :%w", directory, err)
	}

	keyDir, err := generateKeyDirFromFileIds(directory, dataFileIds)
	if err != nil {
		return nil, fmt.Errorf("Error while getting keyDir in dir:%s :%w", directory, err)
	}

	var newFileId int
	if len(dataFileIds) != 0 {
		newFileId = slices.Max(dataFileIds) + 1
	}

	newFile, err := createNewDataFile(newFileId, directory, fileSystem)
	if err != nil {
		return nil, fmt.Errorf("Failed creating initial data file: %w", err)
	}

	store := &Store{
		store: &store{
			DirectoryName: directory,
			KeyDir:        keyDir,
			lockFile:      lockFile,
			currentFileId: newFileId,
			fileSystem:    fileSystem,
			currentFile:   newFile,
		},
		syncOnPut: syncOnPut}

	return store, nil
}

func OpenReadOnly(directory string, fileSystem FileSystem) (*ReadOnlyStore, error) {
	if err := validateReadPermission(directory, fileSystem); err != nil {
		return nil, err
	}
	lockFile, err := fileSystem.acquireSharedLock(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire shared lock: %w", err)
	}

	dataFileIds, err := fileIds(directory, fileSystem)
	if err != nil {
		return nil, fmt.Errorf("Error while getting dataFileIds in dir:%s :%w", directory, err)
	}

	keyDir, err := generateKeyDirFromFileIds(directory, dataFileIds)
	if err != nil {
		return nil, fmt.Errorf("Error while getting keyDir in dir:%s :%w", directory, err)
	}
	var newFileId int
	if len(dataFileIds) != 0 {
		newFileId = slices.Max(dataFileIds) + 1
	}

	store := &ReadOnlyStore{
		store: &store{
			DirectoryName: directory,
			KeyDir:        keyDir,
			lockFile:      lockFile,
			currentFileId: newFileId,
			fileSystem:    fileSystem,
		}}

	return store, nil
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

// tombstone entry is regular entry with timestamp's least significant bit set to 1
func InitTombstoneEntry(key string, timeStamp uint64) []byte {
	// CRC ModifiedTimeStamp KSZ VSZ(0) K V(nil)
	totalSize := CRC_SIZE + TSTAMP_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + len(key) + 0
	buf := make([]byte, totalSize)
	tombStoneTimeStamp := timeStamp | 1<<63
	binary.LittleEndian.PutUint64(buf[TIMESTAMP_OFFSET:], tombStoneTimeStamp)
	binary.LittleEndian.PutUint32(buf[KEY_SIZE_OFFSET:], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[VALUE_SIZE_OFFSET:], uint32(0))
	copy(buf[KEY_OFFSET:], key)

	crc := crc32.ChecksumIEEE(buf[CRC_SIZE:])
	binary.LittleEndian.PutUint32(buf[CRC_OFFSET:], crc)
	return buf
}
