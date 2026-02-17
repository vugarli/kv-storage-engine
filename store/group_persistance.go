package store

import (
	"fmt"
	"os"
	"path/filepath"
)

type GroupSavingStrat func(group []MergeEntryRecord) ([]MergeResult, error)
type GroupCorruptionStrat func(results []MergeResult)
type GroupCommitStrat func(results []MergeResult)

// TODO filesystem rename
func CommitToDisk(directory string, fileSystem FileSystem) GroupCommitStrat {
	return func(results []MergeResult) {
		resultedFileIds := make(map[int]struct{})

		for _, result := range results {
			if _, exists := resultedFileIds[result.FileId]; !exists {
				resultedFileIds[result.FileId] = struct{}{}

				oldPath := filepath.Join(directory, fmt.Sprintf("%d.datatemp", result.FileId))
				newPath := filepath.Join(directory, fmt.Sprintf("%d.data", result.FileId))
				os.Rename(oldPath, newPath)
				//log error
			}
		}

	}
}

func CleanCorruptedFromDisk(directory string, fileSystem FileSystem) GroupCorruptionStrat {
	return func(results []MergeResult) {
		for _, result := range results {
			filePath := filepath.Join(directory, fmt.Sprintf("%d.data", result.FileId))
			if err := fileSystem.Remove(filePath); err != nil {
				//log
			}
		}
	}
}

// TODO add fileSystem contract
func SaveToDisk(directory string, idFetcher func() (int, bool)) GroupSavingStrat {

	return func(group []MergeEntryRecord) ([]MergeResult, error) {
		fileId, ok := idFetcher()
		if !ok {
			return nil, fmt.Errorf("id sequence exhausted")
		}

		destinationFileName := filepath.Join(directory, fmt.Sprintf("%d.datatemp", fileId))
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
