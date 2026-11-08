package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

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

// CRC TSTAMP KSZ VSZ K V
func initEntry(key, value []byte, timeStamp uint64) []byte {
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
// CRC ModifiedTimeStamp KSZ VSZ(0) K V(nil)
func initTombstoneEntry(key string, timeStamp uint64) []byte {
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

func isTombStoneEntry(header []byte) (bool, error) {
	parsedHeader, err := ParseEntryHeader(header)
	if err != nil {
		return false, err
	}
	timeStamp := parsedHeader.Timestamp
	return timeStamp>>63 == 1 && parsedHeader.ValueSize == 0, nil
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
