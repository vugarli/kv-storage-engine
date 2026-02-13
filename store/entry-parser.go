package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type EntryHeader struct {
	CRC         uint32
	Timestamp   uint64
	KeySize     uint32
	ValueSize   uint32
	KeyOffset   int
	ValueOffset int
	TotalSize   int
}

func ParseEntryHeader(data []byte) (*EntryHeader, error) {
	crc := binary.LittleEndian.Uint32(data[CRC_OFFSET:])
	timestamp := binary.LittleEndian.Uint64(data[TIMESTAMP_OFFSET:])
	keySize := binary.LittleEndian.Uint32(data[KEY_SIZE_OFFSET:])
	valueSize := binary.LittleEndian.Uint32(data[VALUE_SIZE_OFFSET:])

	totalSize := 20 + int(keySize) + int(valueSize)

	return &EntryHeader{
		CRC:         crc,
		Timestamp:   timestamp,
		KeySize:     keySize,
		ValueSize:   valueSize,
		KeyOffset:   KEY_OFFSET,
		ValueOffset: 20 + int(keySize),
		TotalSize:   totalSize,
	}, nil
}
func ExtractKeyGivenData(data []byte) ([]byte, error) {
	header, err := ParseEntryHeader(data)
	if err != nil {
		return nil, err
	}

	if len(data) < header.KeyOffset+int(header.KeySize) {
		return nil, fmt.Errorf("entry truncated at key")
	}

	return data[header.KeyOffset : header.KeyOffset+int(header.KeySize)], nil
}
func ExtractKeyGivenHeader(data []byte, header EntryHeader) ([]byte, error) {
	if len(data) < header.KeyOffset+int(header.KeySize) {
		return nil, fmt.Errorf("entry truncated at key")
	}
	return data[header.KeyOffset : header.KeyOffset+int(header.KeySize)], nil
}
func ExtractValue(data []byte) ([]byte, error) {
	header, err := ParseEntryHeader(data)
	if err != nil {
		return nil, err
	}

	if len(data) < header.ValueOffset+int(header.ValueSize) {
		return nil, fmt.Errorf("entry truncated at value")
	}

	return data[header.ValueOffset : header.ValueOffset+int(header.ValueSize)], nil
}
func VerifyEntryCRC(data []byte) error {
	storedCRC := binary.LittleEndian.Uint32(data[CRC_OFFSET:])
	calculatedCRC := crc32.ChecksumIEEE(data[4:])

	if storedCRC != calculatedCRC {
		return fmt.Errorf("CRC mismatch: stored %x, calculated %x", storedCRC, calculatedCRC)
	}

	return nil
}
