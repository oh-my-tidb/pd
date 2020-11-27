// Copyright 2019 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/movingaverage"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

// StoresStats is a cache hold hot regions.
type StoresStats struct {
	sync.RWMutex
	rollingStoresStats map[uint64]*RollingStoreStats
	totalReadLoads     []float64
	totalWriteLoads    []float64
}

// NewStoresStats creates a new hot spot cache.
func NewStoresStats() *StoresStats {
	return &StoresStats{
		rollingStoresStats: make(map[uint64]*RollingStoreStats),
		totalReadLoads:     make([]float64, DimLen),
		totalWriteLoads:    make([]float64, DimLen),
	}
}

// RemoveRollingStoreStats removes RollingStoreStats with a given store ID.
func (s *StoresStats) RemoveRollingStoreStats(storeID uint64) {
	s.Lock()
	defer s.Unlock()
	delete(s.rollingStoresStats, storeID)
}

// GetRollingStoreStats gets RollingStoreStats with a given store ID.
func (s *StoresStats) GetRollingStoreStats(storeID uint64) *RollingStoreStats {
	s.RLock()
	defer s.RUnlock()
	return s.rollingStoresStats[storeID]
}

// GetOrCreateRollingStoreStats gets or creates RollingStoreStats with a given store ID.
func (s *StoresStats) GetOrCreateRollingStoreStats(storeID uint64) *RollingStoreStats {
	s.Lock()
	defer s.Unlock()
	ret, ok := s.rollingStoresStats[storeID]
	if !ok {
		ret = newRollingStoreStats()
		s.rollingStoresStats[storeID] = ret
	}
	return ret
}

// Observe records the current store status with a given store.
func (s *StoresStats) Observe(storeID uint64, stats *pdpb.StoreStats) {
	store := s.GetOrCreateRollingStoreStats(storeID)
	store.Observe(stats)
}

// Set sets the store statistics (for test).
func (s *StoresStats) Set(storeID uint64, stats *pdpb.StoreStats) {
	store := s.GetOrCreateRollingStoreStats(storeID)
	store.Set(stats)
}

// UpdateTotalLoad updates the total loads of all stores.
func (s *StoresStats) UpdateTotalLoad(stores []*core.StoreInfo) {
	s.Lock()
	defer s.Unlock()
	totalRead, totalWrite := make([]float64, DimLen), make([]float64, DimLen)
	for _, store := range stores {
		stats, ok := s.rollingStoresStats[store.GetID()]
		if !store.IsUp() || !ok {
			continue
		}
		for i := 0; i < DimLen; i++ {
			totalRead[i] += stats.GetReadLoad(i)
			totalWrite[i] += stats.GetWriteLoad(i)
		}
	}
	s.totalReadLoads, s.totalWriteLoads = totalRead, totalWrite
}

// GetTotalReadLoad returns the total read load of all StoreInfo by dim.
func (s *StoresStats) GetTotalReadLoad(i int) float64 {
	s.RLock()
	defer s.RUnlock()
	return s.totalReadLoads[i]
}

// GetTotalWriteLoad returns the total write load of all StoreInfo by dim.
func (s *StoresStats) GetTotalWriteLoad(i int) float64 {
	s.RLock()
	defer s.RUnlock()
	return s.totalWriteLoads[i]
}

// GetStoreCPUUsage returns the total cpu usages of threads of the specified store.
func (s *StoresStats) GetStoreCPUUsage(storeID uint64) float64 {
	s.RLock()
	defer s.RUnlock()
	if storeStat, ok := s.rollingStoresStats[storeID]; ok {
		return storeStat.GetCPUUsage()
	}
	return 0
}

// GetStoreDiskReadRate returns the total read disk io rate of threads of the specified store.
func (s *StoresStats) GetStoreDiskReadRate(storeID uint64) float64 {
	s.RLock()
	defer s.RUnlock()
	if storeStat, ok := s.rollingStoresStats[storeID]; ok {
		return storeStat.GetDiskReadRate()
	}
	return 0
}

// GetStoreDiskWriteRate returns the total write disk io rate of threads of the specified store.
func (s *StoresStats) GetStoreDiskWriteRate(storeID uint64) float64 {
	s.RLock()
	defer s.RUnlock()
	if storeStat, ok := s.rollingStoresStats[storeID]; ok {
		return storeStat.GetDiskWriteRate()
	}
	return 0
}

// GetStoresReadLoads returns all stores read loads.
func (s *StoresStats) GetStoresReadLoads() map[uint64][]float64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64][]float64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		for i := 0; i < DimLen; i++ {
			res[storeID] = append(res[storeID], stats.GetReadLoad(i))
		}
	}
	return res
}

// GetStoresWriteLoads returns all stores read loads.
func (s *StoresStats) GetStoresWriteLoads() map[uint64][]float64 {
	s.RLock()
	defer s.RUnlock()
	res := make(map[uint64][]float64, len(s.rollingStoresStats))
	for storeID, stats := range s.rollingStoresStats {
		for i := 0; i < DimLen; i++ {
			res[storeID] = append(res[storeID], stats.GetWriteLoad(i))
		}
	}
	return res
}

func (s *StoresStats) storeIsUnhealthy(cluster core.StoreSetInformer, storeID uint64) bool {
	store := cluster.GetStore(storeID)
	return store.IsTombstone() || store.IsUnhealthy()
}

// FilterUnhealthyStore filter unhealthy store
func (s *StoresStats) FilterUnhealthyStore(cluster core.StoreSetInformer) {
	s.Lock()
	defer s.Unlock()
	for storeID := range s.rollingStoresStats {
		if s.storeIsUnhealthy(cluster, storeID) {
			delete(s.rollingStoresStats, storeID)
		}
	}
}

// UpdateStoreHeartbeatMetrics is used to update store heartbeat interval metrics
func (s *StoresStats) UpdateStoreHeartbeatMetrics(store *core.StoreInfo) {
	storeHeartbeatIntervalHist.Observe(time.Since(store.GetLastHeartbeatTS()).Seconds())
}

// RollingStoreStats are multiple sets of recent historical records with specified windows size.
type RollingStoreStats struct {
	sync.RWMutex
	readLoads     []*movingaverage.TimeMedian
	writeLoads    []*movingaverage.TimeMedian
	cpuUsage      movingaverage.MovingAvg
	diskReadRate  movingaverage.MovingAvg
	diskWriteRate movingaverage.MovingAvg
}

const (
	storeStatsRollingWindows = 3
	// DefaultAotSize is default size of average over time.
	DefaultAotSize = 2
	// DefaultWriteMfSize is default size of write median filter
	DefaultWriteMfSize = 5
	// DefaultReadMfSize is default size of read median filter
	DefaultReadMfSize = 3
)

// NewRollingStoreStats creates a RollingStoreStats.
func newRollingStoreStats() *RollingStoreStats {
	readLoads := make([]*movingaverage.TimeMedian, DimLen)
	writeLoads := make([]*movingaverage.TimeMedian, DimLen)
	for i := 0; i < DimLen; i++ {
		readLoads[i] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultReadMfSize, StoreHeartBeatReportInterval)
		writeLoads[i] = movingaverage.NewTimeMedian(DefaultAotSize, DefaultWriteMfSize, StoreHeartBeatReportInterval)
	}
	return &RollingStoreStats{
		readLoads:     readLoads,
		writeLoads:    writeLoads,
		cpuUsage:      movingaverage.NewMedianFilter(storeStatsRollingWindows),
		diskReadRate:  movingaverage.NewMedianFilter(storeStatsRollingWindows),
		diskWriteRate: movingaverage.NewMedianFilter(storeStatsRollingWindows),
	}
}

func collect(records []*pdpb.RecordPair) float64 {
	var total uint64
	for _, record := range records {
		total += record.GetValue()
	}
	return float64(total)
}

// Observe records current statistics.
func (r *RollingStoreStats) Observe(stats *pdpb.StoreStats) {
	statInterval := stats.GetInterval()
	interval := statInterval.GetEndTimestamp() - statInterval.GetStartTimestamp()
	log.Debug("update store stats", zap.Uint64("key-write", stats.KeysWritten), zap.Uint64("bytes-write", stats.BytesWritten), zap.Duration("interval", time.Duration(interval)*time.Second), zap.Uint64("store-id", stats.GetStoreId()))
	r.Lock()
	defer r.Unlock()
	r.writeLoads[ByteDim].Add(float64(stats.BytesWritten), time.Duration(interval)*time.Second)
	r.readLoads[ByteDim].Add(float64(stats.BytesRead), time.Duration(interval)*time.Second)
	r.writeLoads[KeyDim].Add(float64(stats.KeysWritten), time.Duration(interval)*time.Second)
	r.readLoads[KeyDim].Add(float64(stats.KeysRead), time.Duration(interval)*time.Second)

	// Updates the cpu usages and disk rw rates of store.
	r.cpuUsage.Add(collect(stats.GetCpuUsages()))
	r.diskReadRate.Add(collect(stats.GetReadIoRates()))
	r.diskWriteRate.Add(collect(stats.GetWriteIoRates()))
}

// Set sets the statistics (for test).
func (r *RollingStoreStats) Set(stats *pdpb.StoreStats) {
	statInterval := stats.GetInterval()
	interval := statInterval.GetEndTimestamp() - statInterval.GetStartTimestamp()
	if interval == 0 {
		return
	}
	r.Lock()
	defer r.Unlock()
	r.writeLoads[ByteDim].Set(float64(stats.BytesWritten) / float64(interval))
	r.readLoads[ByteDim].Set(float64(stats.BytesRead) / float64(interval))
	r.writeLoads[KeyDim].Set(float64(stats.KeysWritten) / float64(interval))
	r.readLoads[KeyDim].Set(float64(stats.KeysRead) / float64(interval))
}

// GetWriteLoad returns the write load by dim.
func (r *RollingStoreStats) GetWriteLoad(i int) float64 {
	r.RLock()
	defer r.RUnlock()
	return r.writeLoads[i].Get()
}

// GetReadLoad returns the read load by dim.
func (r *RollingStoreStats) GetReadLoad(i int) float64 {
	r.RLock()
	defer r.RUnlock()
	return r.readLoads[i].Get()
}

// GetCPUUsage returns the total cpu usages of threads in the store.
func (r *RollingStoreStats) GetCPUUsage() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.cpuUsage.Get()
}

// GetDiskReadRate returns the total read disk io rate of threads in the store.
func (r *RollingStoreStats) GetDiskReadRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.diskReadRate.Get()
}

// GetDiskWriteRate returns the total write disk io rate of threads in the store.
func (r *RollingStoreStats) GetDiskWriteRate() float64 {
	r.RLock()
	defer r.RUnlock()
	return r.diskWriteRate.Get()
}
