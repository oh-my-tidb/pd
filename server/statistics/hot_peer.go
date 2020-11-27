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
	"time"

	"github.com/tikv/pd/pkg/movingaverage"
)

// Indicator dims.
const (
	ByteDim int = iota
	KeyDim
	DimLen
)

// HotPeerStat records each hot peer's statistics
type HotPeerStat struct {
	StoreID  uint64 `json:"store_id"`
	RegionID uint64 `json:"region_id"`

	// HotDegree records the times for the region considered as hot spot during each HandleRegionHeartbeat
	HotDegree int `json:"hot_degree"`
	// AntiCount used to eliminate some noise when remove region in cache
	AntiCount int `json:"anti_count"`

	Kind FlowKind `json:"kind"`

	Loads []float64 `json:"loads"`
	// rolling statistics, recording some recently added records.
	RollingLoads []*movingaverage.TimeMedian

	// LastUpdateTime used to calculate average write
	LastUpdateTime time.Time `json:"last_update_time"`
	// Version used to check the region split times
	Version uint64 `json:"version"`

	needDelete bool
	isLeader   bool
	isNew      bool
}

// ID returns region ID. Implementing TopNItem.
func (stat *HotPeerStat) ID() uint64 {
	return stat.RegionID
}

// Less compares two HotPeerStat.Implementing TopNItem.
func (stat *HotPeerStat) Less(k int, than TopNItem) bool {
	return stat.GetLoad(k) < than.(*HotPeerStat).GetLoad(k)
}

// IsNeedDelete to delete the item in cache.
func (stat *HotPeerStat) IsNeedDelete() bool {
	return stat.needDelete
}

// IsLeader indicates the item belong to the leader.
func (stat *HotPeerStat) IsLeader() bool {
	return stat.isLeader
}

// IsNew indicates the item is first update in the cache of the region.
func (stat *HotPeerStat) IsNew() bool {
	return stat.isNew
}

// GetLoad returns denoised load if possible.
func (stat *HotPeerStat) GetLoad(i int) float64 {
	if len(stat.RollingLoads) > i {
		return stat.RollingLoads[i].Get()
	}
	return stat.Loads[i]
}

// Clone clones the HotPeerStat for hot scheduler.
func (stat *HotPeerStat) Clone() *HotPeerStat {
	ret := *stat
	ret.Loads = make([]float64, DimLen)
	for i := 0; i < DimLen; i++ {
		ret.Loads[i] = stat.GetLoad(i) // replace with denoised loads
	}
	ret.RollingLoads = nil
	return &ret
}
