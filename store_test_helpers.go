package main

import (
	"fmt"
	"math"
	"os"
	"slices"
	"testing"
)

func openTestStore(t *testing.T, syncOnPut bool, tempDir string) *Store {
	t.Helper()

	store, err := Open(tempDir, OSFileSystem{}, syncOnPut)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		store.Close()
	})

	return store

}

func setupTestStore(t *testing.T, syncOnPut bool) (*Store, string) {
	t.Helper()

	tempDir := t.TempDir()
	store, err := Open(tempDir, OSFileSystem{}, syncOnPut)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		store.Close()
	})

	return store, tempDir
}

func assertFileSize(t *testing.T, path string, size int, message string) {
	fileStat, _ := os.Stat(path)
	if size != int(fileStat.Size()) {
		t.Errorf(message, size, fileStat.Size())
	}
}

func assertInList[T comparable](t *testing.T, entry T, list []T, message string) {
	if slices.Index(list, entry) == -1 {
		t.Errorf(message, entry)
	}
}
func assertNotInList[T comparable](t *testing.T, entry T, list []T, message string) {
	if slices.Index(list, entry) != -1 {
		t.Errorf(message, entry)
	}
}

// Warning! During testing, generated timestamps will be same. Because of this, stale, and updated entries will be treated as same.
// Use writeTestEntryWithTimeStamp tp have sequential entry timestamps
func writeTestEntry(t *testing.T, store *Store, key string, value []byte) {
	t.Helper()

	if err := store.Put(key, value); err != nil {
		t.Fatalf("writeEntry failed: %v", err)
	}
}
func writeTestEntryWithTimeStamp(t *testing.T, store *Store, key string, value []byte, timestamp uint64) {
	t.Helper()

	entry := InitEntry([]byte(key), value, timestamp)

	store.mu.Lock()
	defer store.mu.Unlock()
	record, err := store.writeEntry(entry, key, value, timestamp)
	store.currentSize += uint32(len(entry))

	if err != nil {
		t.Fatalf("writeEntry failed: %v", err)
	}
	store.KeyDir[key] = *record
}

func assertKeyInKeyDir(t *testing.T, store *Store, key string) EntryRecord {
	t.Helper()

	record, exists := store.KeyDir[key]
	if !exists {
		t.Fatalf("key %s not found in KeyDir", key)
	}

	return record
}

// Ensures that current active file becomes inactive (forces file rotation), by Puting "blank" entries.
// Setting isTemp to false will make sure that all inserted "blank" entries get persisted after merge operation
// Setting isTemp to true will guarantee that at least one entry will be preserved after merge operation
// keySuffix is keyvalue that will be used for entries. In the case of isTemp false entry keys are formatted: %dkeySuffix
//
// Returns: number of entries inserted, and latest written entry key
func ensureFileRotationHappens(t *testing.T, store *Store, isTemp bool, keySuffix string) (uint32, int) {
	sizeOfEntryValue := 1 << 30 // 1GB
	temp := float64(MAXIMUM_FILE_SIZE-store.currentSize) / float64(sizeOfEntryValue)
	numberOfEntriesNeeded := math.Ceil(temp)
	var entryKey string
	for i := 0; i < int(numberOfEntriesNeeded); i++ {

		entryKey = fmt.Sprintf("%d%s", i, keySuffix)
		if isTemp {
			entryKey = keySuffix
		}

		writeTestEntry(t, store, entryKey, make([]byte, sizeOfEntryValue))
	}
	return uint32(numberOfEntriesNeeded), sizeOfEntryValue
}

func createMockEntriesGivenValueSizes(t *testing.T, valueSizes []int, keySize int) []MergeEntryRecord {
	t.Helper()
	var records = make([]MergeEntryRecord, 0, len(valueSizes))

	for _, valueSize := range valueSizes {
		records = append(records, MergeEntryRecord{
			Record: EntryRecord{
				KeySize:   uint32(keySize),
				ValueSize: uint32(valueSize)},
			Key: "1key"})
	}
	return records
}

func assertEntryExistsKeyValue(t *testing.T, store *Store, key, value string) {
	t.Helper()
	v, err := store.Get(key)
	if err != nil {
		t.Errorf("Unexpected Get error key:%s err:%v", key, err)
	}
	if string(v) != value {
		t.Errorf("For key:%s expected value: %s, but got %s", key, value, string(v))
	}
}

func assertKeyNotInKeyDir(t *testing.T, store *Store, key string) {
	t.Helper()
	_, exists := store.KeyDir[key]
	if exists {
		t.Fatalf("unexpected key: %s found in KeyDir", key)
	}
}
