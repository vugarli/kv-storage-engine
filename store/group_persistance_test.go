package store

import "testing"

func TestParseHintEntry(t *testing.T) {
	valueSize := 32
	valuePos := 64
	timestamp := 128
	key := "key"
	keySize := len(key)
	fileId := 23

	entryRecord := EntryRecord{
		FileId:    0,
		ValueSize: uint32(valueSize),
		ValuePos:  uint64(valuePos),
		Timestamp: uint64(timestamp),
		KeySize:   uint32(keySize),
	}

	mresult := MergeResult{StaleEntry: entryRecord,
		Key:      key,
		ValuePos: uint64(valuePos),
		FileId:   fileId}

	hintEntryByte := InitHintEntry(mresult)
	hintEntryHeader := parseHintEntryHeader(hintEntryByte)

	if hintEntryHeader.ValuePos != uint64(valuePos) {
		t.Errorf("Expected %v, but got %v", valuePos, hintEntryHeader.ValuePos)
	}
	if uint64(hintEntryHeader.ValueSize) != uint64(valueSize) {
		t.Errorf("Expected %v, but got %v", valueSize, hintEntryHeader.ValueSize)
	}
	if uint64(hintEntryHeader.Timestamp) != uint64(timestamp) {
		t.Errorf("Expected %v, but got %v", timestamp, hintEntryHeader.Timestamp)
	}
	if uint64(hintEntryHeader.KeySize) != uint64(keySize) {
		t.Errorf("Expected %v, but got %v", keySize, hintEntryHeader.KeySize)
	}
}
