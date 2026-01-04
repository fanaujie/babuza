// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package walbase

import (
	"fmt"
	"runtime"
	"testing"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type MemoryStats struct {
	HeapAlloc  uint64
	TotalAlloc uint64
	HeapInuse  uint64
	NumGC      uint32
}

func measureMemory() MemoryStats {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return MemoryStats{
		HeapAlloc:  m.HeapAlloc,
		TotalAlloc: m.TotalAlloc,
		HeapInuse:  m.HeapInuse,
		NumGC:      m.NumGC,
	}
}

func formatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func generateEntries(count int, dataSize int) []raftpb.Entry {
	entries := make([]raftpb.Entry, count)
	for i := 0; i < count; i++ {
		data := make([]byte, dataSize)
		for j := 0; j < dataSize; j++ {
			data[j] = byte(j % 256)
		}
		entries[i] = raftpb.Entry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  raftpb.EntryNormal,
			Data:  data,
		}
	}
	return entries
}

func generateEntryIndices[T any](count int, metadata T) []EntryIndex[T] {
	indices := make([]EntryIndex[T], count)
	for i := 0; i < count; i++ {
		indices[i] = EntryIndex[T]{
			Term:     1,
			Index:    uint64(i + 1),
			Type:     raftpb.EntryNormal,
			Metadata: metadata,
		}
	}
	return indices
}

func TestMemoryUsageComparison(t *testing.T) {
	testCases := []struct {
		numEntries int
		dataSize   int
	}{
		{1000, 100},
		{1000, 1024},
		{1000, 10240},
		{10000, 100},
		{10000, 1024},
		{10000, 10240},
		{100000, 100},
		{100000, 1024},
		{100000, 10240},
	}

	fmt.Println("\n" + "=" + "==========================================================================")
	fmt.Println("Memory Usage Comparison: etcd MemoryStorage vs Babuza EntryStorage")
	fmt.Println("==========================================================================")
	fmt.Printf("%-12s | %-10s | %-15s | %-15s | %-10s\n", "Entries", "Data Size", "etcd Memory", "Babuza Memory", "Saved")
	fmt.Println("---------------------------------------------------------------------------")

	for _, tc := range testCases {
		// Measure etcd MemoryStorage
		etcdMem := measureEtcdMemoryStorage(tc.numEntries, tc.dataSize)

		// Measure Babuza EntryStorage
		babuzaMem := measureBabuzaEntryStorage(tc.numEntries, tc.dataSize)

		// Calculate savings
		var savedPercent float64
		if etcdMem > 0 {
			savedPercent = (1 - float64(babuzaMem)/float64(etcdMem)) * 100
		}

		fmt.Printf("%-12d | %-10s | %-15s | %-15s | %.1f%%\n",
			tc.numEntries,
			formatBytes(uint64(tc.dataSize)),
			formatBytes(etcdMem),
			formatBytes(babuzaMem),
			savedPercent)
	}
	fmt.Println("==========================================================================")
}

func measureEtcdMemoryStorage(numEntries int, dataSize int) uint64 {
	// Force GC and get baseline
	before := measureMemory()

	// Create etcd MemoryStorage and append entries
	storage := raft.NewMemoryStorage()
	entries := generateEntries(numEntries, dataSize)
	_ = storage.Append(entries)

	// Measure after
	after := measureMemory()

	// Keep storage alive until measurement
	runtime.KeepAlive(storage)

	return after.HeapAlloc - before.HeapAlloc
}

func measureBabuzaEntryStorage(numEntries int, dataSize int) uint64 {
	// Force GC and get baseline
	before := measureMemory()

	// Create Babuza EntryStorage with mock reader
	reader := &noopReader{}
	storage := NewEntryStorage[EntryMetadata](reader)

	// Generate entry indices (no data, only metadata)
	indices := generateEntryIndices(numEntries, EntryMetadata{
		FileId:       1,
		Offset:       0,
		DataLen:      int64(dataSize),
		DataCapacity: int64(dataSize),
	})
	_ = storage.AppendEntryIndex(indices)

	// Measure after
	after := measureMemory()

	// Keep storage alive until measurement
	runtime.KeepAlive(storage)

	return after.HeapAlloc - before.HeapAlloc
}

type noopReader struct{}

func (r *noopReader) ReadEntriesData(readMetadata []EntryIndex[EntryMetadata], ents []raftpb.Entry) error {
	return nil
}

func BenchmarkEtcdMemoryStorageAppend(b *testing.B) {
	testCases := []struct {
		numEntries int
		dataSize   int
	}{
		{1000, 1024},
		{10000, 1024},
		{100000, 1024},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("entries=%d/dataSize=%d", tc.numEntries, tc.dataSize)
		b.Run(name, func(b *testing.B) {
			entries := generateEntries(tc.numEntries, tc.dataSize)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				storage := raft.NewMemoryStorage()
				_ = storage.Append(entries)
			}
		})
	}
}

func BenchmarkBabuzaEntryStorageAppend(b *testing.B) {
	testCases := []struct {
		numEntries int
		dataSize   int
	}{
		{1000, 1024},
		{10000, 1024},
		{100000, 1024},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("entries=%d/dataSize=%d", tc.numEntries, tc.dataSize)
		b.Run(name, func(b *testing.B) {
			indices := generateEntryIndices(tc.numEntries, EntryMetadata{
				FileId:       1,
				Offset:       0,
				DataLen:      int64(tc.dataSize),
				DataCapacity: int64(tc.dataSize),
			})

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				reader := &noopReader{}
				storage := NewEntryStorage[EntryMetadata](reader)
				_ = storage.AppendEntryIndex(indices)
			}
		})
	}
}

func TestBabuzaMemoryIndependentOfDataSize(t *testing.T) {
	numEntries := 10000
	dataSizes := []int{100, 1024, 10240, 102400}

	fmt.Println("\n" + "==========================================================================")
	fmt.Println("Verifying Babuza EntryStorage memory is independent of data size")
	fmt.Println("==========================================================================")

	var memories []uint64
	for _, dataSize := range dataSizes {
		mem := measureBabuzaEntryStorage(numEntries, dataSize)
		memories = append(memories, mem)
		fmt.Printf("Data Size: %-10s -> Babuza Memory: %s\n", formatBytes(uint64(dataSize)), formatBytes(mem))
	}

	// Verify all memory usages are within 10% of each other
	if len(memories) > 1 {
		base := memories[0]
		allSimilar := true
		for i, mem := range memories[1:] {
			diff := float64(mem) / float64(base)
			if diff < 0.9 || diff > 1.1 {
				allSimilar = false
				t.Errorf("Memory usage differs significantly: base=%s, dataSizes[%d]=%s (ratio=%.2f)",
					formatBytes(base), i+1, formatBytes(mem), diff)
			}
		}
		if allSimilar {
			fmt.Println("\n[PASS] Babuza EntryStorage memory usage is independent of data size")
		}
	}
	fmt.Println("==========================================================================")
}

func TestEtcdMemoryLinearWithDataSize(t *testing.T) {
	numEntries := 10000
	dataSizes := []int{100, 1024, 10240}

	fmt.Println("\n" + "==========================================================================")
	fmt.Println("Verifying etcd MemoryStorage memory grows linearly with data size")
	fmt.Println("==========================================================================")

	var memories []uint64
	for _, dataSize := range dataSizes {
		mem := measureEtcdMemoryStorage(numEntries, dataSize)
		memories = append(memories, mem)
		fmt.Printf("Data Size: %-10s -> etcd Memory: %s\n", formatBytes(uint64(dataSize)), formatBytes(mem))
	}

	// Verify memory grows (roughly) linearly with data size
	if len(memories) >= 2 {
		// Check that larger data sizes result in more memory usage
		for i := 1; i < len(memories); i++ {
			if memories[i] <= memories[i-1] {
				t.Errorf("Expected memory to increase: memories[%d]=%s <= memories[%d]=%s",
					i, formatBytes(memories[i]), i-1, formatBytes(memories[i-1]))
			}
		}
		fmt.Println("\n[PASS] etcd MemoryStorage memory usage grows with data size")
	}
	fmt.Println("==========================================================================")
}
