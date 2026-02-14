package store

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestPut(t *testing.T) {

	t.Run("Successful write", func(t *testing.T) {
		key, value := "key", "value"
		directory := t.TempDir()
		store, _ := Open(directory, true)

		if err := store.Put(key, []byte(value)); err != nil {
			t.Errorf("Write failed %v", err)
		}

		store.Close()

		entryFile, _ := os.ReadFile(filepath.Join(directory, "0.data"))

		extractedKey, _ := ExtractKeyGivenData(entryFile)
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
	t.Run("multiple writes to same key", func(t *testing.T) {
		tempDir := t.TempDir()
		stores, _ := Open(tempDir, false)
		store := Unwrap(stores)

		defer store.Close()

		key := "key"

		store.Put(key, []byte("value1"))

		record1 := store.KeyDir[key]

		store.Put(key, []byte("value2"))

		record2 := store.KeyDir[key]

		if record1.ValuePos == record2.ValuePos {
			t.Error("expected different positions for overwrites")
		}

	})

	t.Run("concurrent writes are serialized", func(t *testing.T) {
		tempDir := t.TempDir()
		stores, _ := Open(tempDir, false)
		store := Unwrap(stores)
		defer store.Close()

		var wg sync.WaitGroup
		numWrites := 100

		for i := 0; i < numWrites; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", n)
				value := []byte(fmt.Sprintf("value%d", n))
				store.Put(key, value)
			}(i)
		}

		wg.Wait()

		if len(store.KeyDir) != numWrites {
			t.Errorf("expected %d keys, got %d", numWrites, len(store.KeyDir))
		}
	})

	t.Run("Successful write with correct currentSize", func(t *testing.T) {
		store, _ := setupTestStore(t, false)

		writeTestEntry(t, store, "key1", []byte("value1"))
		writeTestEntry(t, store, "key2", []byte("value2"))
		writeTestEntry(t, store, "key3", []byte("value3"))

		record := assertKeyInKeyDir(t, store, "key1")

		if record.ValueSize != 6 {
			t.Errorf("wrong ValueSize: got %d, want 6", record.ValueSize)
		}
		gotSize := store.currentSize
		store.Close()

		expectedSize := 3 * (HEADER_SIZE + 4 + 6)

		if uint32(expectedSize) != gotSize {
			t.Errorf("Store.currentSize is wrong. Expected size: %d, but got %d", expectedSize, gotSize)
		}
	})
}

func TestFileRotation(t *testing.T) {
	t.Run("successful rotation after size exceeds", func(t *testing.T) {
		store, directory := setupTestStore(t, false)

		writeTestEntry(t, store, "1gb", make([]byte, 1<<30))
		writeTestEntry(t, store, "1gb", make([]byte, 1<<30))
		writeTestEntry(t, store, "1gb", make([]byte, 1<<30))

		if store.currentFileId != 1 {
			t.Error("Expected file rotation didn't happen.")
			return
		}

		store.Close()

		oldFile, _ := os.Stat(path.Join(directory, "0.data"))
		newFile, _ := os.Stat(path.Join(directory, "1.data"))

		if oldFile.Size() != 1<<31+2*(HEADER_SIZE+3) {
			t.Errorf("Old data file size is wrong, expected: %d, got: %d", 1<<31, oldFile.Size())
		}
		if newFile.Size() != 1<<30+HEADER_SIZE+3 {
			t.Errorf("New data file size is wrong, expected: %d, got: %d", 1<<30, newFile.Size())
		}
	})
}

func TestWriteEntry_ValuePosition(t *testing.T) {
	tempDir := t.TempDir()
	stores, _ := Open(tempDir, false)
	store := Unwrap(stores)
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
			positionBefore, _ := store.currentFile.Seek(0, io.SeekCurrent)

			err := store.Put(tt.key, tt.value)
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

func TestUpdateKeydir(t *testing.T) {

	entries := []struct {
		key      string
		value    string
		expected string
		pos      uint64
	}{
		{key: "key", value: "value1", expected: "value3", pos: 136},
		{key: "XX", value: "value", expected: "value", pos: 51},
		{key: "key", value: "value2", expected: "value3", pos: 136},
		{key: "XXX", value: "value", expected: "value", pos: 108},
		{key: "key", value: "value3", expected: "value3", pos: 136},

		{key: "XXX", value: "value3", expected: "value3", pos: 23},
		{key: "key", value: "value4", expected: "value4", pos: 51},
	}

	store, directory := setupTestStore(t, true)

	for i, entry := range entries[:5] {
		writeTestEntryWithTimeStamp(t, store, entry.key, []byte(entry.value), uint64(i))
	}

	store.Close()

	store = openTestStore(t, true, directory)

	for _, entry := range entries[:5] {
		record := assertKeyInKeyDir(t, store, entry.key)
		if record.ValuePos != entry.pos {
			t.Errorf("Value position is wrong for %s. Expected %d but found %d", entry.key, record.ValuePos, uint64(HEADER_SIZE+len(entry.key)))
		}
		value, err := store.Get(entry.key)
		if err != nil {
			t.Errorf("Error %v", err)
		}
		if string(value) != entry.expected {
			t.Errorf("Expected %s: %s, but got %s", entry.key, entry.value, string(value))
		}

	}

	for i, entry := range entries[5:] {
		writeTestEntryWithTimeStamp(t, store, entry.key, []byte(entry.value), uint64(i))
	}

	for _, entry := range entries[5:] {
		assertKeyInKeyDir(t, store, entry.key)
		value, err := store.Get(entry.key)
		if err != nil {
			t.Errorf("Error %v", err)
		}
		if string(value) != entry.expected {
			t.Errorf("Expected %s: %s, but got %s", entry.key, entry.value, string(value))
		}

	}

}

func TestDelete(t *testing.T) {
	t.Run("Remove entry successfully", func(t *testing.T) {

		entries := []struct {
			key   string
			value string
		}{
			{key: "key", value: "value1"},
			{key: "XX", value: "value"},
			{key: "key", value: "value2"},
			{key: "XXX", value: "value"},
			{key: "key", value: "value3"},
		}

		store, _ := setupTestStore(t, true)

		for i, entry := range entries[:5] {
			writeTestEntryWithTimeStamp(t, store, entry.key, []byte(entry.value), uint64(i))
		}

		deletedKey := "key"
		err := store.Delete(deletedKey)
		if err != nil {
			t.Errorf("Failed to delete entry with key:%s", deletedKey)
		}

		for _, entry := range entries {
			if entry.key == deletedKey {
				assertKeyNotInKeyDir(t, store, entry.key)
			}
		}
	})
	t.Run("Removed entry shouldn't be present when store opens again", func(t *testing.T) {
		entries := []struct {
			key   string
			value string
		}{
			{key: "key", value: "value1"},
			{key: "XX", value: "value"},
			{key: "key", value: "value2"},
			{key: "XXX", value: "value"},
			{key: "key", value: "value3"},
		}

		store, directory := setupTestStore(t, true)

		for i, entry := range entries[:5] {
			writeTestEntryWithTimeStamp(t, store, entry.key, []byte(entry.value), uint64(i))
		}

		deletedKey := "key"
		err := store.Delete(deletedKey)
		if err != nil {
			t.Errorf("Failed to delete entry with key:%s", deletedKey)
		}

		store.Close()
		store = openTestStore(t, true, directory)

		for _, entry := range entries {
			if entry.key == deletedKey {
				assertKeyNotInKeyDir(t, store, entry.key)
			}
		}
	})
}

func TestMerge(t *testing.T) {
	t.Run("Succsessfully compacts SINGLE FILE by removing stale entries", func(t *testing.T) {
		entries := []struct {
			key   string
			value string
		}{
			{key: "key", value: "value1"},
			{key: "XX", value: "value"},
			{key: "key", value: "value2"},
			{key: "XXX", value: "value"},
			{key: "key", value: "value3"},
		}

		store, directory := setupTestStore(t, false)

		for i, entry := range entries {
			writeTestEntryWithTimeStamp(t, store, entry.key, []byte(entry.value), uint64(i))
		}

		_, size := ensureFileRotationHappens(t, store, true, "key3")

		if err := store.Merge(); err != nil {
			t.Error("Failed to Merge")
		}
		store.Close()

		// check size to make sure single file compacted
		fileStat, _ := os.Stat(path.Join(directory, "2.data"))
		expectedSize := (HEADER_SIZE * 4) + 12 + 16 + size
		if expectedSize != int(fileStat.Size()) {
			t.Errorf("Expected compaction didn't happen. Expected file size:%d, but got:%d", expectedSize, fileStat.Size())
		}
		fileStat1, _ := os.Stat(path.Join(directory, "1.data"))
		expectedSize1 := 0

		if expectedSize1 != int(fileStat1.Size()) {
			t.Errorf("Latest active file size is wrong: expected:%d got:%d", expectedSize1, fileStat1.Size())
		}

		// test only updated entries remain
		store = openTestStore(t, true, directory)

		assertEntryExistsKeyValue(t, store, "key", "value3")
		assertEntryExistsKeyValue(t, store, "XX", "value")
		assertEntryExistsKeyValue(t, store, "XXX", "value")
		assertKeyInKeyDir(t, store, "key3")
	})

	t.Run("Succsessfully compacts MULTIPLE FILES TO SINGLE FILE by removing stale entries", func(t *testing.T) {
		entries := []struct {
			key   string
			value string
		}{
			{key: "key", value: "value1"},
			{key: "XX", value: "value"},
			{key: "key", value: "value2"},
			{key: "XXX", value: "value"},
			{key: "key", value: "value3"},
		}

		store, directory := setupTestStore(t, false)

		for i, entry := range entries {
			writeTestEntryWithTimeStamp(t, store, entry.key, []byte(entry.value), uint64(i))
		}
		_, size := ensureFileRotationHappens(t, store, true, "key3")
		// active: 1.data.
		// inactive: 0.data. 5 entries + N key3 entries

		_, size2 := ensureFileRotationHappens(t, store, true, "key4")
		// active: 2.data.
		// inactive: 0.data. 5 entries + N key3 entries
		// inactive: 1.data. 2 key4 entries

		if err := store.Merge(); err != nil {
			t.Error("Failed to Merge %w", err)
		}
		// remaining files: 2.data, 1.data
		// 2.data
		// 3.data 1 key3 + 1 key4 + 3 entries
		store.Close()

		dirEntries, _ := filepath.Glob(path.Join(directory, "*.data"))
		assertInList(t, filepath.Join(directory, "2.data"), dirEntries, "Expected %s file in directory, but file were not present")
		assertInList(t, filepath.Join(directory, "3.data"), dirEntries, "Expected %s file in directory, but file were not present")
		assertNotInList(t, filepath.Join(directory, "0.data"), dirEntries, "Didn't expect %s file in directory, but file were present")
		assertNotInList(t, filepath.Join(directory, "1.data"), dirEntries, "Didn't expect %s file in directory, but file were present")

		data1ExpectedSize := (HEADER_SIZE * 5) + 3 + 2 + 3 + 4 + 4 + (6 + 5 + 5 + size + size2)
		assertFileSize(t, path.Join(directory, "3.data"), data1ExpectedSize, "Expected compaction didn't happen. Expected file size:%d, but got:%d")

		data2ExpectedSize := 0
		assertFileSize(t, path.Join(directory, "2.data"), data2ExpectedSize, "Expected compaction didn't happen. Expected file size:%d, but got:%d")

		// test only updated entries remain
		store = openTestStore(t, true, directory)

		assertEntryExistsKeyValue(t, store, "key", "value3")
		assertEntryExistsKeyValue(t, store, "XX", "value")
		assertEntryExistsKeyValue(t, store, "XXX", "value")
		//assertEntryExistsKeyValue(t, store, "key3", strings.Repeat("\x00", size))
		assertKeyInKeyDir(t, store, "key3")
		assertKeyInKeyDir(t, store, "key4")
	})

}

func TestReadEntryFunc(t *testing.T) {
	store, directory := setupTestStore(t, true)
	defer store.Close()

	entries := []struct {
		key   string
		value string
	}{
		{"k1", "val1"}, // 20 + 2 + 4 = 26
		{"k2", "val2"}, // 20 + 2 + 4 = 26
		{"k3", "val3"}, // 20 + 2 + 4 = 26
	}

	for _, e := range entries {
		writeTestEntry(t, store, e.key, []byte(e.value))
	}
	store.Close()

	file, err := os.Open(path.Join(directory, "0.data"))
	if err != nil {
		t.Fatalf("Failed to open data file: %v", err)
	}
	defer file.Close()

	t.Run("read middle entry with correct offset", func(t *testing.T) {
		offset := uint64(26)
		entry, _, err := readEntry(file, offset)

		key, _ := ExtractKeyGivenData(entry)

		if err != nil {
			t.Fatalf("Failed to read entry at offset %d: %v", offset, err)
		}
		if string(key) != "k2" {
			t.Errorf("Expected key k2, got %s", string(key))
		}
	})

	t.Run("read last entry", func(t *testing.T) {
		offset := uint64(52)
		entry, _, err := readEntry(file, offset)
		key, _ := ExtractKeyGivenData(entry)

		if err != nil {
			t.Fatalf("Failed to read last entry: %v", err)
		}
		if string(key) != "k3" {
			t.Errorf("Expected key k3, got %s", string(key))
		}
	})

	t.Run("read beyond file limits", func(t *testing.T) {
		_, _, err := readEntry(file, 100)
		if err == nil {
			t.Error("Expected error when reading past EOF, got nil")
		}
	})

	t.Run("read with corrupted/invalid offset", func(t *testing.T) {
		_, _, err := readEntry(file, 5)
		if err == nil {
			t.Error("Expected error when reading from an invalid offset, got nil")
		}
	})
}
