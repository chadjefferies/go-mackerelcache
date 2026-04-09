package mackerelcache

import (
	"fmt"
	"math"
	"sync"

	pb "github.com/chadjefferies/go-mackerelcache/api/mackerelcachepb"
)

type NodeStats struct {
	Stats map[string]*pb.CacheStats
	mu    sync.Mutex
}

func NewStats() *NodeStats {
	return &NodeStats{
		Stats: make(map[string]*pb.CacheStats),
	}
}

func (s *NodeStats) AddNodeStats(node string, stats *pb.CacheStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Stats[node] = stats
}

func (s *NodeStats) Aggregate() *pb.CacheStats {
	s.mu.Lock()
	defer s.mu.Unlock()

	totalStats := &pb.CacheStats{}
	for _, stats := range s.Stats {
		totalStats.CurrentItems += stats.CurrentItems
		totalStats.TotalItems += stats.TotalItems
		totalStats.Hits += stats.Hits
		totalStats.Misses += stats.Misses
		totalStats.TotalEvictions += stats.TotalEvictions
		totalStats.TotalExpirations += stats.TotalExpirations
		totalStats.MemoryUsed += stats.MemoryUsed
		totalStats.AllocatedMemory += stats.AllocatedMemory
		totalStats.ServerTotalMemory += stats.ServerTotalMemory
		totalStats.PagedSystemMemorySize += stats.PagedSystemMemorySize
		totalStats.PagedMemorySize += stats.PagedMemorySize
		totalStats.VirtualMemorySize += stats.VirtualMemorySize
		totalStats.WorkingSet += stats.WorkingSet
		totalStats.PeakPagedMemorySize += stats.PeakPagedMemorySize
		totalStats.PeakVirtualMemorySize += stats.PeakVirtualMemorySize
		totalStats.PeakWorkingSet += stats.PeakWorkingSet
		totalStats.GarbageCollections += stats.GarbageCollections
		totalStats.ServerProcessors += stats.ServerProcessors
		totalStats.TotalCacheSize += stats.TotalCacheSize
		totalStats.TotalReservedCacheSize += stats.TotalReservedCacheSize
		totalStats.TotalWatchEvents += stats.TotalWatchEvents
		totalStats.HeapFragmentation += stats.HeapFragmentation
		totalStats.TotalHeapSize += stats.TotalHeapSize
		totalStats.HeapCommitted += stats.HeapCommitted

		if totalStats.ServerName == "" {
			totalStats.ServerName = stats.ServerName
		} else {
			totalStats.ServerName += "," + stats.ServerName
		}

		if totalStats.TotalProcessorTime == nil {
			totalStats.TotalProcessorTime = stats.TotalProcessorTime
		} else {
			totalStats.TotalProcessorTime.Seconds += stats.TotalProcessorTime.Seconds
			totalStats.TotalProcessorTime.Nanos += stats.TotalProcessorTime.Nanos
		}

		for i := range stats.HeapSizes {
			if len(totalStats.HeapSizes) <= i {
				totalStats.HeapSizes = append(totalStats.HeapSizes, stats.HeapSizes[i])
			} else {
				totalStats.HeapSizes[i] += stats.HeapSizes[i]
			}
		}

		if totalStats.ModifiedDate == nil || (stats.ModifiedDate != nil && stats.ModifiedDate.Seconds > totalStats.ModifiedDate.Seconds) {
			totalStats.ModifiedDate = stats.ModifiedDate
		}
		if stats.Partitions > totalStats.Partitions {
			totalStats.Partitions = stats.Partitions
		}
		if stats.CurrentWatchStreams > totalStats.CurrentWatchStreams {
			totalStats.CurrentWatchStreams = stats.CurrentWatchStreams
		}
		if stats.CurrentWatches > totalStats.CurrentWatches {
			totalStats.CurrentWatches = stats.CurrentWatches
		}
		if totalStats.EvictedTime == nil || stats.EvictedTime.Seconds >= totalStats.EvictedTime.Seconds {
			totalStats.EvictedTime = stats.EvictedTime
		}
		if totalStats.Uptime == nil || stats.Uptime.Seconds <= totalStats.Uptime.Seconds {
			totalStats.Uptime = stats.Uptime
		}
		if stats.GcPauseTimePercentage > totalStats.GcPauseTimePercentage {
			totalStats.GcPauseTimePercentage = stats.GcPauseTimePercentage
		}
	}

	totalStats.HitRate = calculateHitRate(totalStats.Hits, totalStats.Misses)
	totalStats.AllocatedMemoryHuman = toHumanMBString(totalStats.AllocatedMemory)
	totalStats.MemoryUsedHuman = toHumanMBString(totalStats.MemoryUsed)
	totalStats.PagedMemorySizeHuman = toHumanMBString(totalStats.PagedMemorySize)
	totalStats.PeakPagedMemorySizeHuman = toHumanMBString(totalStats.PeakPagedMemorySize)
	totalStats.WorkingSetHuman = toHumanMBString(totalStats.WorkingSet)
	totalStats.PeakWorkingSetHuman = toHumanMBString(totalStats.PeakWorkingSet)
	totalStats.PagedSystemMemorySizeHuman = toHumanMBString(totalStats.PagedSystemMemorySize)
	totalStats.VirtualMemorySizeHuman = toHumanMBString(totalStats.VirtualMemorySize)
	totalStats.ServerTotalMemoryHuman = toHumanMBString(totalStats.ServerTotalMemory)
	totalStats.PeakVirtualMemorySizeHuman = toHumanMBString(totalStats.PeakVirtualMemorySize)
	totalStats.TotalCacheSizeHuman = toHumanMBString(totalStats.TotalCacheSize)
	totalStats.TotalReservedCacheSizeHuman = toHumanMBString(totalStats.TotalReservedCacheSize)
	totalStats.HeapFragmentationHuman = toHumanMBString(totalStats.HeapFragmentation)
	totalStats.TotalHeapSizeHuman = toHumanMBString(totalStats.TotalHeapSize)
	totalStats.HeapCommittedHuman = toHumanMBString(totalStats.HeapCommitted)

	return totalStats
}

func calculateHitRate(hits, misses int64) float64 {
	if hits == 0 && misses == 0 {
		return 0.0
	}
	return math.Round(float64(hits)/float64(hits+misses)*1e4) / 1e4
}

func toHumanMBString(value int64) string {
	return fmt.Sprintf("%.0f MB", float64(value)/math.Pow(1024, 2))
}
