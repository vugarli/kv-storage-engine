package main

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
	"strings"
	"sync"
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
			tt.setupMock(mockFS)

			store, err := Open(tt.path, mockFS, tt.syncOnPut)

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
			tt.setupMock(mockFS)

			store, err := OpenReadOnly(tt.path, mockFS)

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
			entry := InitEntry(tt.key, tt.value, uint64(time.Now().Unix()))

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

	entry := InitEntry(largeKey, largeValue, uint64(time.Now().Unix()))

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
			entry := InitEntry([]byte(tt.key), []byte(tt.value), uint64(timeStamp))

			if len(entry) != tt.wantSize {
				t.Errorf("entry size = %d, want %d", len(entry), tt.wantSize)
			}

			extractedKey, err := ExtractKey(entry)
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

func TestPut(t *testing.T) {

	t.Run("Successful write", func(t *testing.T) {
		mf := OSFileSystem{}
		key, value := "key", "value"
		directory := t.TempDir()
		store, _ := Open(directory, mf, true)

		timeStamp := uint64(time.Now().Unix())

		entry := InitEntry([]byte(key), []byte(value), timeStamp)

		if err := store.writeEntry(entry, key, []byte(value), timeStamp); err != nil {
			t.Errorf("Write failed %v", err)
		}

		store.Close()

		entryFile, _ := os.ReadFile(filepath.Join(directory, "0.data"))

		extractedKey, _ := ExtractKey(entryFile)
		extractedValue, _ := ExtractValue(entryFile)

		if string(extractedKey) != key {
			t.Errorf("Expected %s, but got %s as a key", key, extractedKey)
		}
		if string(extractedValue) != value {
			t.Errorf("Expected %s, but got %s as a key", value, &extractedValue)
		}

		if VerifyEntryCRC(entryFile) != nil {
			t.Error("CRC check failed!")
		}

	})

	t.Run("Successful write with correct position", func(t *testing.T) {
		store, directory := setupTestStore(t, false)

		writeTestEntry(t, store, "key1", []byte("value1"))
		writeTestEntry(t, store, "key2", []byte("value2"))

		record := assertKeyInKeyDir(t, store, "key1")

		if record.ValueSize != 6 {
			t.Errorf("wrong ValueSize: got %d, want 6", record.ValueSize)
		}
		store.Close()

		entryFile, _ := os.ReadFile(filepath.Join(directory, "0.data"))

		firstEntry := entryFile[:HEADER_SIZE+10]
		secondEntry := entryFile[HEADER_SIZE+10:]

		if err := VerifyEntryCRC(firstEntry); err != nil {
			t.Errorf("FirstEntry: CRC verification failed: %v", err)
		}

		if err := VerifyEntryCRC(secondEntry); err != nil {
			t.Errorf("SecondEntry: CRC verification failed: %v", err)
		}

		fV, _ := ExtractValue(firstEntry)
		sV, _ := ExtractValue(secondEntry)

		if string(fV) != "value1" {
			t.Error("First entry value is wrong")
		}

		if string(sV) != "value2" {
			t.Error("Second entry value is wrong")
		}
	})

}

func TestWriteEntry_ValuePosition(t *testing.T) {
	tempDir := t.TempDir()
	store, _ := Open(tempDir, OSFileSystem{}, false)
	defer store.Close()

	tests := []struct {
		key   string
		value []byte
	}{
		{"a", []byte("x")},
		{"short", []byte("value")},
		{"medium-length-key", []byte("some value here")},
		{strings.Repeat("k", 100), []byte("value")},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			timestamp := uint64(time.Now().Unix())
			entry := InitEntry([]byte(tt.key), tt.value, timestamp)

			positionBefore, _ := store.currentFile.Seek(0, io.SeekCurrent)

			err := store.writeEntry(entry, tt.key, tt.value, timestamp)
			if err != nil {
				t.Fatal(err)
			}

			record := store.KeyDir[tt.key]

			expectedValuePos := positionBefore + int64(HEADER_SIZE) + int64(len(tt.key))

			if record.ValuePos != uint64(expectedValuePos) {
				t.Errorf("wrong ValuePos: got %d, want %d", record.ValuePos, expectedValuePos)
			}

			store.currentFile.Seek(int64(record.ValuePos), io.SeekStart)
			valueRead := make([]byte, record.ValueSize)
			store.currentFile.Read(valueRead)

			if !bytes.Equal(valueRead, tt.value) {
				t.Error("value at ValuePos doesn't match")
			}
		})
	}
}
func TestWriteEntry_EdgeCases(t *testing.T) {
	t.Run("nil currentFile", func(t *testing.T) {
		store := &Store{
			store: &store{
				currentFile: nil, // Not initialized
				KeyDir:      make(map[string]LatestEntryRecord),
			},
		}

		entry := InitEntry([]byte("key"), []byte("value"), 123)
		err := store.writeEntry(entry, "key", []byte("value"), 123)

		if err == nil {
			t.Error("expected error with nil currentFile")
		}
	})

	t.Run("multiple writes to same key", func(t *testing.T) {
		tempDir := t.TempDir()
		store, _ := Open(tempDir, OSFileSystem{}, false)
		defer store.Close()

		key := "key"

		entry1 := InitEntry([]byte(key), []byte("value1"), 100)
		store.writeEntry(entry1, key, []byte("value1"), 100)

		record1 := store.KeyDir[key]

		entry2 := InitEntry([]byte(key), []byte("value2"), 200)
		store.writeEntry(entry2, key, []byte("value2"), 200)

		record2 := store.KeyDir[key]

		if record1.ValuePos == record2.ValuePos {
			t.Error("expected different positions for overwrites")
		}

		if record2.Timestamp != 200 {
			t.Errorf("timestamp not updated: got %d, want 200", record2.Timestamp)
		}
	})

	t.Run("concurrent writes are serialized", func(t *testing.T) {
		tempDir := t.TempDir()
		store, _ := Open(tempDir, OSFileSystem{}, false)
		defer store.Close()

		var wg sync.WaitGroup
		numWrites := 100

		for i := 0; i < numWrites; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", n)
				value := []byte(fmt.Sprintf("value%d", n))
				timestamp := uint64(n)
				entry := InitEntry([]byte(key), value, timestamp)

				store.writeEntry(entry, key, value, timestamp)
			}(i)
		}

		wg.Wait()

		if len(store.KeyDir) != numWrites {
			t.Errorf("expected %d keys, got %d", numWrites, len(store.KeyDir))
		}
	})
}
