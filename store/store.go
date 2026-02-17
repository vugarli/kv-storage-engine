package store

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
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

func (e EntryRecord) EntrySize() int64 {
	return int64(HEADER_SIZE) + int64(e.KeySize) + int64(e.ValueSize)
}

type Entry struct {
	Key         string
	IsTombEntry bool
	ValueSize   uint32
	ValuePos    uint64
	Timestamp   uint64
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

	dataFileIds, err := fileIds(directory, s.fileSystem)
	if err != nil {
		return nil, fmt.Errorf("Error while getting dataFileIds in dir:%s :%w", directory, err)
	}

	keyDir, err := generateKeyDirFromFileIds(directory, dataFileIds)
	if err != nil {
		return nil, fmt.Errorf("Error while getting keyDir in dir:%s :%w", directory, err)
	}
	s.entryIndex = keyDir

	newFileId := getMaxFileId(dataFileIds)
	seq := idSequence(newFileId)
	next, _ := iter.Pull(seq)
	s.nextId = next
	// handle error gracefully
	s.currentFileId, _ = s.acquireId()

	newFile, err := createNewDataFile(newFileId, directory, s.fileSystem)
	if err != nil {
		return nil, fmt.Errorf("Failed creating initial data file: %w", err)
	}
	s.currentFile = newFile

	store := &RWStore{store: s}

	//store.ScheduleMerge(PeriodicMerge(10 * time.Second))

	return store, nil
}

func (s *store) acquireId() (int, error) {
	id, ok := s.nextId()
	if !ok {
		return 0, fmt.Errorf("file id sequence exhausted")
	}
	return id, nil
}

func getMaxFileId(dataFileIds []int) int {
	var newFileId int
	if len(dataFileIds) != 0 {
		newFileId = slices.Max(dataFileIds) + 1
	}
	return newFileId
}

func OpenReadOnly(directory string, options ...option) (*ROStore, error) {
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

	dataFileIds, err := fileIds(directory, s.fileSystem)
	if err != nil {
		return nil, fmt.Errorf("Error while getting dataFileIds in dir:%s :%w", directory, err)
	}

	keyDir, err := generateKeyDirFromFileIds(directory, dataFileIds)
	if err != nil {
		return nil, fmt.Errorf("Error while getting keyDir in dir:%s :%w", directory, err)
	}
	s.entryIndex = keyDir

	newFileId := getMaxFileId(dataFileIds)
	s.currentFileId = newFileId

	store := &ROStore{store: s}
	return store, nil
}

func (s *store) Close() error {
	if s.lockFile != nil {
		Unlock(s.lockFile)
		s.lockFile.Close()
	}
	if s.currentFile != nil {
		s.currentFile.Close()
	}
	s.readFileCache.evictall()
	return s.cleanJunk()
}

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
	s.entryIndex[string(keyByte)] = *record
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
func (s *store) ListKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Collect(maps.Keys(s.entryIndex))
}
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

func (s *RWStore) Merge(retStrat MergeCandidateRetrievalStrat) error {
	// Holding RLock as candidate retrieval may access KeyDir
	s.mu.RLock()
	candidateEntries := retStrat(s.entryIndex)
	s.mu.RUnlock()
	if len(candidateEntries) == 0 {
		return nil
	}
	// Grouping entries to files efficiently in FFD order
	groups := groupEntriesFFD(candidateEntries, MAXIMUM_MERGED_FILE_SIZE)

	// Holding WLock as next ops modify state
	s.mu.Lock()
	defer s.mu.Unlock()

	// id sequence starts after currentFileId
	results, err := s.saveGroups(groups, SaveToDiskStrat(s.DirectoryName, s.nextId))
	if err != nil {
		return fmt.Errorf("Error during writing groups to files:%w", err)
	}

	s.updateKeydirFromMergeResults(results)

	if err := s.cleanJunk(); err != nil {
		return fmt.Errorf("Error while cleaning junk data file names:%w", err)
	}
	return nil
}

type binState struct {
	groups  [][]MergeEntryRecord
	current []MergeEntryRecord
	size    int64
}

func foldEntry(state binState, entry MergeEntryRecord, maxSize int64) binState {
	entrySize := entry.Record.EntrySize()

	switch {
	case entrySize >= maxSize:
		// doesn't fit seal current bin if any, give entry its own bin
		groups := state.groups
		if len(state.current) > 0 {
			groups = append(groups, state.current)
		}
		return binState{
			groups:  append(groups, []MergeEntryRecord{entry}),
			current: nil,
			size:    0,
		}

	case state.size+entrySize <= maxSize:
		// fits in current bin
		return binState{
			groups:  state.groups,
			current: append(state.current, entry),
			size:    state.size + entrySize,
		}

	default:
		// doesn't fit seal current bin, start new one
		return binState{
			groups:  append(state.groups, state.current),
			current: []MergeEntryRecord{entry},
			size:    entrySize,
		}
	}
}

// Groups entries in FFD order so that no group exceeds maxSize
func groupEntriesFFD(entries map[int][]MergeEntryRecord, maxSize int64) [][]MergeEntryRecord {
	// flatten map values into a slice for sorting
	flat := make([]MergeEntryRecord, 0, totalEntries(entries))
	for _, recs := range entries {
		flat = append(flat, recs...)
	}

	slices.SortFunc(flat, func(a, b MergeEntryRecord) int {
		return cmp.Compare(b.Record.EntrySize(), a.Record.EntrySize())
	})

	final := fold(flat, binState{}, func(state binState, entry MergeEntryRecord) binState {
		return foldEntry(state, entry, maxSize)
	})

	if len(final.current) > 0 {
		return append(final.groups, final.current)
	}
	return final.groups
}

func totalEntries(entries map[int][]MergeEntryRecord) int {
	return fold(
		slices.Collect(maps.Values(entries)),
		0,
		func(acc int, recs []MergeEntryRecord) int { return acc + len(recs) },
	)
}

func initEntry(key, value []byte, timeStamp uint64) []byte {
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

// tombstone entry is regular entry with timestamp's most significant bit set to 1
func initTombstoneEntry(key string, timeStamp uint64) []byte {
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
	return fullEntry, entryHeader, nil
}
func (s *RWStore) writeEntry(entry []byte, key string, timestamp uint64) (*EntryRecord, error) {
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

	// write failed restore seek to old position
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
		ValueSize: uint32(len(entry[HEADER_SIZE+len(key):])),
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
		num, err := strconv.Atoi(digit)
		if err != nil {
			return 0, fmt.Errorf("data file format is wrong: %w", err)
		}
		return num, nil
	}
	return 0, fmt.Errorf("data file format is wrong")
}

// Given inactive files, and dir, updates KeyDir
func generateKeyDirFromFileIds(directory string, inactiveDataFileIds []int) (KeyDir, error) {
	keyDir := make(KeyDir)

	for _, dataFileId := range inactiveDataFileIds {
		dataFileName := fmt.Sprintf("%d.data", dataFileId)
		filepath := filepath.Join(directory, dataFileName)

		if err := loadKeyDirFromFile(filepath, keyDir); err != nil {
			return nil, fmt.Errorf("Warning: error loading %s: %v", dataFileName, err)
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

func loadKeyDirFromFile(filePath string, keyDir KeyDir) error {
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
func (s *RWStore) updateKeydirFromMergeResults(results []MergeResult) {
	for _, result := range results {
		entry, e := s.entryIndex[result.Key]
		if e {
			entry.FileId = result.FileId
			entry.ValuePos = result.ValuePos
			s.entryIndex[result.Key] = entry
		}
	}
}

func (s *store) cleanCorruptedMergeFiles(corruptedResults []MergeResult) {
	for _, result := range corruptedResults {
		filePath := filepath.Join(s.DirectoryName, fmt.Sprintf("%d.data", result.FileId))
		if err := s.fileSystem.Remove(filePath); err != nil {
			//log
		}
	}
}

func idSequence(startId int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for id := startId; ; id++ {
			if !yield(id) {
				return
			}
		}
	}
}

type GroupSavingStrat func(group []MergeEntryRecord) ([]MergeResult, error)

func SaveToDiskStrat(directory string, idFetcher func() (int, bool)) GroupSavingStrat {

	return func(group []MergeEntryRecord) ([]MergeResult, error) {
		fileId, ok := idFetcher()
		if !ok {
			return nil, fmt.Errorf("id sequence exhausted")
		}

		destinationFileName := filepath.Join(directory, fmt.Sprintf("%d.data", fileId))
		destinationFile, err := os.OpenFile(destinationFileName, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("opening destination file: %w", err)
		}
		defer destinationFile.Close()

		result := make([]MergeResult, 0, len(group))
		var currentOffset uint64
		for _, staleEntry := range group {
			originFile, err := os.Open(filepath.Join(directory, fmt.Sprintf("%d.data", staleEntry.Record.FileId)))
			if err != nil {
				return nil, fmt.Errorf("%w Opening origin file: %w", ErrCorruptedState, err)
			}
			entry, entryHeader, err := readEntry(originFile, uint64(staleEntry.Record.ValuePos-HEADER_SIZE-uint64(len(staleEntry.Key))))
			originFile.Close()
			if err != nil {
				return result, fmt.Errorf("%w Reading entry: %w", ErrCorruptedState, err)
			}
			n, err := destinationFile.WriteAt(entry, int64(currentOffset))
			if err != nil {
				return result, fmt.Errorf("%w Writing entry: %w", ErrCorruptedState, err)
			}
			if n != len(entry) {
				return result, fmt.Errorf("%w Incomplete write: wrote %d of %d bytes", ErrCorruptedState, n, len(entry))
			}
			newValuePos := currentOffset + HEADER_SIZE + uint64(entryHeader.KeySize)
			result = append(result, MergeResult{
				Key:      staleEntry.Key,
				ValuePos: newValuePos,
				FileId:   fileId,
			})
			currentOffset += uint64(len(entry))
		}
		if err := destinationFile.Sync(); err != nil {
			return nil, fmt.Errorf("syncing destination file: %w", err)
		}
		return result, nil
	}
}

// BUG: group file Id wrongly calculated
// Writes groups to .data files. New fileIds gets incremented from currentFileId
func (s *RWStore) saveGroups(groups [][]MergeEntryRecord, savingStrat GroupSavingStrat) ([]MergeResult, error) {
	var length int
	for _, v := range groups {
		length += len(v)
	}
	result := make([]MergeResult, 0, length)
	nextFileId := s.currentFileId + 1
	for _, group := range groups {
		mergeResults, err := savingStrat(group)
		// Need to clean corrupted files
		if err == ErrCorruptedState {
			s.cleanCorruptedMergeFiles(mergeResults)
		}
		if err != nil {
			return result, err
		}
		//TODO save group to hint

		result = append(result, mergeResults...)
		nextFileId++
	}
	return result, nil
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

func (s *RWStore) rotateFile() error {

	if s.currentFile != nil {
		if err := s.currentFile.Sync(); err != nil {
			return fmt.Errorf("failed to rotate current file. Sync error:%w", err)
		}
		s.currentFile.Close()
	}

	//TODO handle gracefully
	s.currentFileId, _ = s.acquireId()
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
