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
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"time"

	"github.com/tikv/pd/pkg/movingaverage"
)

const (
	byteDim int = iota
	keyDim
	dimLen
)

type dimStat struct {
	typ         int
	Rolling     *movingaverage.TimeMedian
	LastAverage *movingaverage.AvgOverTime
}

func newDimStat(typ int) *dimStat {
	reportInterval := time.Duration(RegionHeartBeatReportInterval) * time.Second
	return &dimStat{
		typ:         typ,
		Rolling:     movingaverage.NewTimeMedian(DefaultAotSize, rollingWindowsSize, reportInterval),
		LastAverage: movingaverage.NewAvgOverTime(reportInterval),
	}
}

func (d *dimStat) Add(delta float64, interval time.Duration) {
	d.LastAverage.Add(delta, interval)
	d.Rolling.Add(delta, interval)
}

func (d *dimStat) isHot(thresholds [dimLen]float64) bool {
	return d.LastAverage.IsFull() && d.LastAverage.Get() >= thresholds[d.typ]
}

func (d *dimStat) isFull() bool {
	return d.LastAverage.IsFull()
}

func (d *dimStat) clearLastAverage() {
	d.LastAverage.Clear()
}

func (d *dimStat) Get() float64 {
	return d.Rolling.Get()
}

// HotPeerStat records each hot peer's statistics
type HotPeerStat struct {
	StoreID  uint64 `json:"store_id"`
	RegionID uint64 `json:"region_id"`

	// HotDegree records the times for the region considered as hot spot during each HandleRegionHeartbeat
	HotDegree int `json:"hot_degree"`
	// AntiCount used to eliminate some noise when remove region in cache
	AntiCount int `json:"anti_count"`

	Kind     FlowKind `json:"kind"`
	ByteRate float64  `json:"flow_bytes"`
	KeyRate  float64  `json:"flow_keys"`

	// rolling statistics, recording some recently added records.
	RollingByteRate *dimStat
	RollingKeyRate  *dimStat

	// LastUpdateTime used to calculate average write
	LastUpdateTime time.Time `json:"last_update_time"`
	// Version used to check the region split times
	Version uint64 `json:"version"`

	needDelete    bool
	isLeader      bool
	isNew         bool
	isNewLeader   bool
	interval      uint64
	thresholdsLog [2]float64
	peers         []uint64
}

// ID returns region ID. Implementing TopNItem.
func (stat *HotPeerStat) ID() uint64 {
	return stat.RegionID
}

// Less compares two HotPeerStat.Implementing TopNItem.
func (stat *HotPeerStat) Less(k int, than TopNItem) bool {
	rhs := than.(*HotPeerStat)
	switch k {
	case keyDim:
		return stat.GetKeyRate() < rhs.GetKeyRate()
	case byteDim:
		fallthrough
	default:
		return stat.GetByteRate() < rhs.GetByteRate()
	}
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

// GetByteRate returns denoised BytesRate if possible.
func (stat *HotPeerStat) GetByteRate() float64 {
	if stat.RollingByteRate == nil {
		return stat.ByteRate
	}
	return stat.RollingByteRate.Get()
}

// GetKeyRate returns denoised KeysRate if possible.
func (stat *HotPeerStat) GetKeyRate() float64 {
	if stat.RollingKeyRate == nil {
		return stat.KeyRate
	}
	return stat.RollingKeyRate.Get()
}

// Clone clones the HotPeerStat
func (stat *HotPeerStat) Clone() *HotPeerStat {
	ret := *stat
	ret.ByteRate = stat.GetByteRate()
	ret.RollingByteRate = nil
	ret.KeyRate = stat.GetKeyRate()
	ret.RollingKeyRate = nil
	return &ret
}

func (stat *HotPeerStat) isHot(thresholds [dimLen]float64) bool {
	return stat.RollingByteRate.isHot(thresholds) || stat.RollingKeyRate.isHot(thresholds)
}

func (stat *HotPeerStat) clearLastAverage() {
	stat.RollingByteRate.clearLastAverage()
	stat.RollingKeyRate.clearLastAverage()
}

func (stat *HotPeerStat) Log(str string) {
	log.Info(str,
		zap.Uint64("interval", stat.interval),
		zap.Uint64("region", stat.RegionID),
		zap.Uint64("store", stat.StoreID),
		zap.Uint64("epoch", stat.Version),
		zap.Float64("byteRate", stat.ByteRate),
		zap.Float64("byteRateTh", stat.thresholdsLog[byteDim]),
		zap.Float64("keyRate", stat.KeyRate),
		zap.Float64("keyRateTh", stat.thresholdsLog[keyDim]),
		zap.Int("hotDegree", stat.HotDegree),
		zap.Int("hotRegionAntiCount", stat.AntiCount),
		zap.Bool("isNewLeader", stat.isNewLeader),
		zap.Bool("isLeader", stat.isLeader),
		zap.Bool("needDelete", stat.needDelete),
		zap.String("type", stat.Kind.String()))
}
