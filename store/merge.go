package store

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
)

type MergeEntryRecord struct {
	Record EntryRecord
	Key    string
}
type MergeResult struct {
	StaleEntry EntryRecord
	Key        string
	ValuePos   uint64
	FileId     int
}

// TODO
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
	results, err := s.saveGroups(groups, SaveToDisk(s.DirectoryName, s.nextId, s.fileSystem),
		CleanCorruptedFromDisk(s.DirectoryName, s.fileSystem),
		GenerateHintDecorator(s.fileSystem, s.DirectoryName, writeHintEntriesToDisk, CommitToDisk(s.DirectoryName, s.fileSystem)))
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

// Persists, and handles corrupted groups according to strats
func (s *RWStore) saveGroups(groups [][]MergeEntryRecord,
	savingStrat GroupSavingStrat, onCorruption GroupCorruptionStrat, onSuccess GroupOnSuccessStrat) ([]MergeResult, error) {
	var length int
	for _, v := range groups {
		length += len(v)
	}
	results := make([]MergeResult, 0, length)
	nextFileId := s.currentFileId + 1
	for _, group := range groups {
		mergeResults, err := savingStrat(group)
		if err == ErrCorruptedState {
			onCorruption(mergeResults)
		}
		if err != nil {
			return results, err
		}
		//TODO save group to hint
		results = append(results, mergeResults...)
		nextFileId++
	}
	// commit results
	onSuccess(results)

	return results, nil
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
