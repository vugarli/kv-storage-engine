package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	tests := []struct {
		name          string
		path          string
		syncOnPut     bool
		expectedError error
		setupMock     func(*MockFileSystem)
	}{
		{
			name:          "Directory doesn't exist",
			path:          "/nonexistent",
			syncOnPut:     false,
			expectedError: ErrStoreDirectoryNotFound,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return nil, os.ErrNotExist
				}
			},
		},
		{
			name:          "Path is file not directory",
			path:          "/file.txt",
			syncOnPut:     false,
			expectedError: ErrStoreDirectoryNotFound,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return nil, syscall.ENOTDIR
				}
			},
		},
		{
			name:          "No read permission",
			path:          "/restricted",
			syncOnPut:     false,
			expectedError: ErrStoreDirectoryPermissionDenied,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return nil, os.ErrPermission
				}
			},
		},
		{
			name:          "No write permission",
			path:          "/readonly",
			syncOnPut:     false,
			expectedError: ErrStoreDirectoryPermissionDenied,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return []fs.DirEntry{}, nil
				}
				m.CreateTempFunc = func(dir, pattern string) (*os.File, error) {
					return nil, os.ErrPermission
				}
			},
		},
		{
			name:          "Successful open without sync",
			path:          "/valid/store",
			syncOnPut:     false,
			expectedError: nil,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return []fs.DirEntry{}, nil
				}
				m.CreateTempFunc = func(dir, pattern string) (*os.File, error) {
					return nil, nil
				}
				m.RemoveFunc = func(name string) error {
					return nil
				}
				m.acquireExclusiveLockFunc = func(directory string) (*os.File, error) {
					return nil, nil
				}
			},
		},
		{
			name:          "Successful open with sync",
			path:          "/valid/store",
			syncOnPut:     true,
			expectedError: nil,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return []fs.DirEntry{}, nil
				}
				m.CreateTempFunc = func(dir, pattern string) (*os.File, error) {
					return nil, nil
				}
				m.RemoveFunc = func(name string) error {
					return nil
				}
				m.acquireExclusiveLockFunc = func(directory string) (*os.File, error) {
					return nil, nil
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockFS := &MockFileSystem{}

			tempFs := fileSystem
			fileSystem = mockFS
			defer func() {
				fileSystem = tempFs
			}()

			tt.setupMock(mockFS)

			store, err := OpenTest(tt.path, tt.syncOnPut)

			if tt.expectedError != nil {
				if err == nil {
					t.Fatalf("Expected error %v, got nil", tt.expectedError)
				}
				if !errors.Is(err, tt.expectedError) {
					t.Errorf("Expected error %v, got %v", tt.expectedError, err)
				}
				if store != nil {
					t.Errorf("Expected nil store on error, got %+v", store)
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				if store == nil {
					t.Fatal("Expected non-nil store, got nil")
				}
				if store.DirectoryName != tt.path {
					t.Errorf("Expected directory %s, got %s", tt.path, store.DirectoryName)
				}
				if store.KeyDir == nil {
					t.Errorf("Expected initialized KeyDir, got nil")
				}
				if store.syncOnPut != tt.syncOnPut {
					t.Errorf("Expected syncOnPut %v, got %v", tt.syncOnPut, store.syncOnPut)
				}
			}
		})
	}
}
func TestOpenReadOnly(t *testing.T) {
	tests := []struct {
		name          string
		path          string
		expectedError error
		setupMock     func(*MockFileSystem)
	}{
		{
			name:          "Directory doesn't exist",
			path:          "/nonexistent",
			expectedError: ErrStoreDirectoryNotFound,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return nil, os.ErrNotExist
				}
			},
		},
		{
			name:          "Path is file not directory",
			path:          "/file.txt",
			expectedError: ErrStoreDirectoryNotFound,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return nil, syscall.ENOTDIR
				}
			},
		},
		{
			name:          "No read permission",
			path:          "/restricted",
			expectedError: ErrStoreDirectoryPermissionDenied,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return nil, os.ErrPermission
				}
			},
		},
		{
			name:          "Successful read-only open",
			path:          "/valid/store",
			expectedError: nil,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return []fs.DirEntry{}, nil
				}
				m.acquireSharedLockFunc = func(directory string) (*os.File, error) {
					return nil, nil
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockFS := &MockFileSystem{}

			tempFs := fileSystem
			fileSystem = mockFS
			defer func() {
				fileSystem = tempFs
			}()

			tt.setupMock(mockFS)

			store, err := OpenReadOnlyTest(tt.path)

			if tt.expectedError != nil {
				if err == nil {
					t.Fatalf("Expected error %v, got nil", tt.expectedError)
				}
				if !errors.Is(err, tt.expectedError) {
					t.Errorf("Expected error %v, got %v", tt.expectedError, err)
				}
				if store != nil {
					t.Errorf("Expected nil store on error, got %+v", store)
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				if store == nil {
					t.Fatal("Expected non-nil store, got nil")
				}
				if store.DirectoryName != tt.path {
					t.Errorf("Expected directory %s, got %s", tt.path, store.DirectoryName)
				}
				if store.KeyDir == nil {
					t.Errorf("Expected initialized KeyDir, got nil")
				}
			}
		})
	}
}

func TestInitEntry(t *testing.T) {
	cases := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{
			name:  "Correct key value",
			key:   []byte("key"),
			value: []byte("value"),
		},
		{
			name:  "Empty key value",
			key:   make([]byte, 0),
			value: make([]byte, 0),
		},
		{
			name:  "large key",
			key:   []byte(strings.Repeat("k", 1024)),
			value: []byte("value"),
		},
		{
			name:  "large value",
			key:   []byte("key"),
			value: make([]byte, 1024*1024),
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			entry := initEntry(tt.key, tt.value, uint64(time.Now().Unix()))

			expectedSize := HEADER_SIZE + len(tt.key) + len(tt.value)
			if len(entry) != expectedSize {
				t.Errorf("entry size = %d, want %d", len(entry), expectedSize)
			}

			keySize := binary.LittleEndian.Uint32(entry[KEY_SIZE_OFFSET:])
			if keySize != uint32(len(tt.key)) {
				t.Errorf("keySize = %d, want %d", keySize, len(tt.key))
			}

			valueSize := binary.LittleEndian.Uint32(entry[VALUE_SIZE_OFFSET:])
			if valueSize != uint32(len(tt.value)) {
				t.Errorf("valueSize = %d, want %d", valueSize, len(tt.value))
			}

			keyStart := KEY_OFFSET
			keyEnd := keyStart + len(tt.key)
			if !bytes.Equal(entry[keyStart:keyEnd], tt.key) {
				t.Error("key content mismatch")
			}

			valueStart := HEADER_SIZE + len(tt.key)
			valueEnd := valueStart + len(tt.value)
			if !bytes.Equal(entry[valueStart:valueEnd], tt.value) {
				t.Error("value content mismatch")
			}
		})
	}
}

func TestInitEntry_MaxSize(t *testing.T) {

	largeKey := make([]byte, 1024*1024)
	largeValue := make([]byte, 10*1024*1024)

	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	entry := initEntry(largeKey, largeValue, uint64(time.Now().Unix()))

	expectedSize := HEADER_SIZE + len(largeKey) + len(largeValue)
	if len(entry) != expectedSize {
		t.Errorf("entry size = %d, want %d", len(entry), expectedSize)
	}

	storedCRC := binary.LittleEndian.Uint32(entry[CRC_OFFSET:])
	calculatedCRC := crc32.ChecksumIEEE(entry[4:])
	if storedCRC != calculatedCRC {
		t.Error("CRC failed for large entry")
	}

}

func TestEntryParsing(t *testing.T) {
	tests := []struct {
		key      string
		value    string
		wantSize int
	}{
		{
			key:      "",
			value:    "",
			wantSize: 20,
		},
		{
			key:      "a",
			value:    "b",
			wantSize: 22,
		},
		{
			key:      "user:123",
			value:    "John Doe",
			wantSize: 36,
		},
		{
			key:      "key",
			value:    string(make([]byte, 1024)),
			wantSize: 1047,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s:%s", tt.key, tt.value[:min(len(tt.value), 10)]), func(t *testing.T) {
			timeStamp := uint64(time.Now().Unix())
			entry := initEntry([]byte(tt.key), []byte(tt.value), uint64(timeStamp))

			if len(entry) != tt.wantSize {
				t.Errorf("entry size = %d, want %d", len(entry), tt.wantSize)
			}

			extractedKey, err := ExtractKeyGivenData(entry)
			if err != nil {
				t.Fatal(err)
			}
			if string(extractedKey) != tt.key {
				t.Errorf("extracted key = %s, want %s", extractedKey, tt.key)
			}

			extractedValue, err := ExtractValue(entry)
			if err != nil {
				t.Fatal(err)
			}
			if string(extractedValue) != tt.value {
				t.Errorf("extracted value mismatch")
			}

			if err := VerifyEntryCRC(entry); err != nil {
				t.Errorf("CRC verification failed: %v", err)
			}
		})
	}
}

func TestWriteEntry_EdgeCases(t *testing.T) {
	t.Run("nil currentFile", func(t *testing.T) {
		store := &store{
			currentFile: nil,
			KeyDir:      make(map[string]EntryRecord),
		}

		err := store.Put("key", []byte("value"))

		if err == nil {
			t.Error("expected error with nil currentFile")
		}
	})

	t.Run("nil key", func(t *testing.T) {
		store, _ := setupTestStore(t, false)
		err := store.Put("", []byte("value"))
		if err == nil {
			t.Error("Expected put op to fail for nil key, but didn't fail")
		}
	})

	t.Run("Partial write gives error, and recovers file", func(t *testing.T) {
		store, _ := setupTestStore(t, true)

		store.currentFile = MockFile{Real: store.currentFile.(*os.File)}

		err := store.Put("key", []byte("value"))

		if err == nil {
			t.Error("Expected error from partial write but go no error")
		}
		pos, err := store.currentFile.Seek(0, io.SeekCurrent)
		if pos != 0 {
			t.Errorf("Expected position to be set to 0, but got %d", pos)
		}

	})
}

func TestGet(t *testing.T) {
	t.Run("Fails get op for nil key", func(t *testing.T) {
		store, _ := setupTestStore(t, true)
		key, value := "key", "value"

		writeTestEntry(t, store, key, []byte(value))

		_, err := store.Get("")

		if err == nil {
			t.Error("Expected get op fail for nil key, but didn't fail")
		}
	})
	t.Run("Successful get op after write op", func(t *testing.T) {
		store, _ := setupTestStore(t, true)
		key, value := "key", "value"

		writeTestEntry(t, store, key, []byte(value))

		gotValue, err := store.Get(key)

		if err != nil {
			t.Errorf("Got unexpected error for Get op: %v", err)
		}

		if string(gotValue) != value {
			t.Errorf("Got %s, but expected %s", string(gotValue), value)
		}
	})
	t.Run("Fails to get non-existent key", func(t *testing.T) {
		store, _ := setupTestStore(t, true)
		key := "key"
		_, err := store.Get(key)

		if err == nil {
			t.Errorf("Expected error, but got none")
		}
	})
	t.Run("Successful get op after overwriting write op", func(t *testing.T) {
		store, _ := setupTestStore(t, true)
		key, value := "key", "value"

		updatedValue := "NewValue"

		writeTestEntry(t, store, key, []byte(value))
		writeTestEntry(t, store, key, []byte(updatedValue))

		gotValue, err := store.Get(key)

		if err != nil {
			t.Errorf("Got unexpected error for Get op: %v", err)
		}

		if string(gotValue) != updatedValue {
			t.Errorf("Got %s, but expected %s", string(gotValue), value)
		}
	})
	t.Run("Fails read op because file corrupted", func(t *testing.T) {
		store, tempDir := setupTestStore(t, false)

		store.Put("key", []byte("value"))
		store.Close()
		dataFile := filepath.Join(tempDir, "0.data")
		os.Truncate(dataFile, 10)

		store2, err := Open(tempDir, false)
		if err != nil {
			t.Error("Store open failed: %w", err)
		}

		_, err = store2.Get("key")
		store2.Close()
		if err == nil {
			t.Error("expected error when reading truncated file")
		}
	})

	t.Run("get correct values for multiple keys", func(t *testing.T) {
		store, _ := setupTestStore(t, true)

		kvs := map[string]string{
			"user1":          "user1",
			"user2":          "user2",
			"user3":          strings.Repeat("user", 333),
			"testingunicode": "🚩",
			"config:retries": "3",
		}

		for k, v := range kvs {
			store.Put(k, []byte(v))
		}

		for k, want := range kvs {
			got, err := store.Get(k)
			if err != nil {
				t.Errorf("Get(%s) failed: %v", k, err)
				continue
			}

			if string(got) != want {
				t.Errorf("Get(%s) = %s, want %s", k, got, want)
			}
		}
	})

}

func TestExtractFileId(t *testing.T) {
	cases := []struct {
		testTitle string
		name      string
		expected  int
		fails     bool
	}{
		{testTitle: "Success", name: "0.data", expected: 0},
		{testTitle: "Success", name: "9.data", expected: 9},
		{testTitle: "Success", name: "100.data", expected: 100},
		{testTitle: "Non num Id fails", name: "x.data", fails: true},
		{testTitle: "Empty Id fails", name: ".data", fails: true},
	}

	for _, c := range cases {
		t.Run(c.testTitle, func(t *testing.T) {
			got, err := extractFileId(c.name)
			if c.fails {
				if err == nil {
					t.Error("Expected to fail")
				}
			}
			if got != c.expected {
				t.Errorf("Expected %d but got %d", c.expected, got)
			}
		})
	}
}

func TestInitTombstoneEntry(t *testing.T) {
	key := "key"
	timeStamp := uint64(time.Now().Unix())

	tombStoneEntry := initTombstoneEntry(key, timeStamp)

	tombStoneEntryHeader, err := ParseEntryHeader(tombStoneEntry)
	if err != nil {
		t.Error("Failed to parse tombstone entry header")
	}

	gotTimeStamp := tombStoneEntryHeader.Timestamp
	recoveredTimeStamp := gotTimeStamp ^ 1<<63
	if recoveredTimeStamp != timeStamp {
		t.Errorf("Failed to recover timestamp. Expected: %d got %d ", timeStamp, recoveredTimeStamp)
	}

	recoveredKey := string(tombStoneEntry[tombStoneEntryHeader.KeyOffset : tombStoneEntryHeader.KeyOffset+int(tombStoneEntryHeader.KeySize)])
	if recoveredKey != key {
		t.Errorf("Expected key:%s, but got %s", key, recoveredKey)
	}

	if tombStoneEntryHeader.ValueSize != 0 {
		t.Error("Tombstone entry value size should be 0")
	}

}

func TestIsTombStoneEntry(t *testing.T) {
	key := "key"
	timeStamp := uint64(time.Now().Unix())

	tombStoneEntry := initTombstoneEntry(key, timeStamp)
	b, err := isTombStoneEntry(tombStoneEntry)
	if err != nil {
		t.Error("Error while checking entry: %w", err)
	}
	if !b {
		t.Error("Expected entry to be tombstone entry")
	}
}

func TestListKeys(t *testing.T) {

	mockFS := MockFileSystem{}
	tempFs := fileSystem
	fileSystem = mockFS
	defer func() {
		fileSystem = tempFs
	}()

	store, _ := OpenTest(".", false)
	entries := []struct {
		key string
	}{
		{key: "key"},
		{key: "XX"},
		{key: "key"},
		{key: "XXX"},
		{key: "key"},
	}
	for _, e := range entries {
		store.KeyDir[e.key] = EntryRecord{}
	}
	expected := []string{"key", "XX", "XXX"}
	got := store.ListKeys()
	slices.Sort(got)
	slices.Sort(expected)

	if slices.Compare(got, expected) != 0 {
		t.Errorf("Expected %v, but got %v", expected, got)
	}
}

func TestGroupEntriesFFD(t *testing.T) {

	tests := []struct {
		name          string
		valueSizes    []int
		maxSize       int
		expectedSizes []int
	}{
		{
			name:          "correct grouping when single entry size equals maxSize",
			valueSizes:    []int{376, 26, 76, 76, 26, 26},
			maxSize:       400,
			expectedSizes: []int{400, 350},
		},
		{
			name:       "gap filling (FFD logic)",
			valueSizes: []int{300, 150, 70},
			maxSize:    400,
			// Sorted total sizes: 324, 174, 94
			expectedSizes: []int{324, 268},
		},
		{
			name:       "perfect multiples of maxSize",
			valueSizes: []int{176, 176, 176, 176},
			maxSize:    400,
			// each is 200.
			expectedSizes: []int{400, 400},
		},
		{
			name:          "many small items in one bin",
			valueSizes:    []int{26, 26, 26, 26, 26},
			maxSize:       250,
			expectedSizes: []int{250},
		},
		{
			name:          "oversized item behavior",
			valueSizes:    []int{500},
			maxSize:       400,
			expectedSizes: []int{524},
		},
		{
			name:          "empty input",
			valueSizes:    []int{},
			maxSize:       400,
			expectedSizes: []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entries := createMockEntriesGivenValueSizes(t, tt.valueSizes, 4)

			groups := groupEntriesFFD(entries, int64(tt.maxSize))

			if len(groups) != len(tt.expectedSizes) {
				t.Errorf("expected %d groups, got %d", len(tt.expectedSizes), len(groups))
				return
			}

			for i, group := range groups {
				var actualGroupSize int
				for _, entry := range group {
					actualGroupSize += int(entry.Record.ValueSize) + int(entry.Record.KeySize) + HEADER_SIZE
				}

				if tt.expectedSizes[i] != actualGroupSize {
					t.Errorf("group %d: expected total size %d, got %d", i, tt.expectedSizes[i], actualGroupSize)
				}
			}
		})
	}
}
