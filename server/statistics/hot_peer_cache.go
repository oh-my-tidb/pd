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
	"math"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/movingaverage"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/core"
)

const (
	topNN             = 60
	topNTTL           = 3 * RegionHeartBeatReportInterval * time.Second
	hotThresholdRatio = 0.8

	rollingWindowsSize = 5

	hotRegionReportMinInterval = 3

	hotRegionAntiCount = 2
)

var minHotThresholds = [RegionStatCount]float64{
	RegionWriteBytes: 1 * 1024,
	RegionWriteKeys:  32,
	RegionReadBytes:  8 * 1024,
	RegionReadKeys:   128,
}

// hotPeerCache saves the hot peer's statistics.
type hotPeerCache struct {
	kind           HotCacheKind
	peersOfStore   map[uint64]*TopN               // storeID -> hot peers
	storesOfRegion map[uint64]map[uint64]struct{} // regionID -> storeIDs
}

// NewHotStoresStats creates a HotStoresStats
func NewHotStoresStats(kind HotCacheKind) *hotPeerCache {
	return &hotPeerCache{
		kind:           kind,
		peersOfStore:   make(map[uint64]*TopN),
		storesOfRegion: make(map[uint64]map[uint64]struct{}),
	}
}

// RegionStats returns hot items
func (f *hotPeerCache) RegionStats() map[uint64][]*HotPeerStat {
	res := make(map[uint64][]*HotPeerStat)
	for storeID, peers := range f.peersOfStore {
		values := peers.GetAll()
		stat := make([]*HotPeerStat, len(values))
		res[storeID] = stat
		for i := range values {
			stat[i] = values[i].(*HotPeerStat)
		}
	}
	return res
}

// Update updates the items in statistics.
func (f *hotPeerCache) Update(item *HotPeerStat) {
	if item.IsNeedDelete() {
		if peers, ok := f.peersOfStore[item.StoreID]; ok {
			peers.Remove(item.RegionID)
		}

		if stores, ok := f.storesOfRegion[item.RegionID]; ok {
			delete(stores, item.StoreID)
		}
	} else {
		peers, ok := f.peersOfStore[item.StoreID]
		if !ok {
			peers = NewTopN(len(f.kind.RegionStats()), topNN, topNTTL)
			f.peersOfStore[item.StoreID] = peers
		}
		peers.Put(item)

		stores, ok := f.storesOfRegion[item.RegionID]
		if !ok {
			stores = make(map[uint64]struct{})
			f.storesOfRegion[item.RegionID] = stores
		}
		stores[item.StoreID] = struct{}{}
	}
}

func (f *hotPeerCache) collectRegionMetrics(loads []float64, interval uint64) {
	regionHeartbeatIntervalHist.Observe(float64(interval))
	if interval == 0 {
		return
	}
	if len(loads) != len(f.kind.RegionStats()) {
		return
	}

	statKinds := f.kind.RegionStats()
	for i, l := range loads {
		switch statKinds[i] {
		case RegionReadBytes:
			readByteHist.Observe(l)
		case RegionReadKeys:
			readKeyHist.Observe(l)
		case RegionWriteBytes:
			writeByteHist.Observe(l)
		case RegionWriteKeys:
			writeKeyHist.Observe(l)
		}
	}
}

// CheckRegionFlow checks the flow information of region.
func (f *hotPeerCache) CheckRegionFlow(region *core.RegionInfo) (ret []*HotPeerStat) {
	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()

	deltaLoads := f.getRegionDeltaLoads(region)
	loads := make([]float64, len(deltaLoads))
	for i := range deltaLoads {
		loads[i] = deltaLoads[i] / float64(interval)
	}

	f.collectRegionMetrics(loads, interval)

	// old region is in the front and new region is in the back
	// which ensures it will hit the cache if moving peer or transfer leader occurs with the same replica number

	var tmpItem *HotPeerStat
	storeIDs := f.getAllStoreIDs(region)
	for _, storeID := range storeIDs {
		isExpired := f.isRegionExpired(region, storeID) // transfer read leader or remove write peer
		oldItem := f.getOldHotPeerStat(region.GetID(), storeID)
		if isExpired && oldItem != nil { // it may has been moved to other store, we save it to tmpItem
			tmpItem = oldItem
		}

		// This is used for the simulator. Ignore if report too fast.
		if !isExpired && Denoising && interval < hotRegionReportMinInterval {
			continue
		}

		newItem := &HotPeerStat{
			StoreID:        storeID,
			RegionID:       region.GetID(),
			Kind:           f.kind,
			Loads:          loads,
			LastUpdateTime: time.Now(),
			needDelete:     isExpired,
			isLeader:       region.GetLeader().GetStoreId() == storeID,
		}

		if oldItem == nil {
			if tmpItem != nil { // use the tmpItem cached from the store where this region was in before
				oldItem = tmpItem
			} else { // new item is new peer after adding replica
				for _, storeID := range storeIDs {
					oldItem = f.getOldHotPeerStat(region.GetID(), storeID)
					if oldItem != nil {
						break
					}
				}
			}
		}

		newItem = f.updateHotPeerStat(newItem, oldItem, deltaLoads, time.Duration(interval))
		if newItem != nil {
			ret = append(ret, newItem)
		}
	}

	return ret
}

func (f *hotPeerCache) IsRegionHot(region *core.RegionInfo, hotDegree int) bool {
	switch f.kind {
	case PeerCache:
		return f.isRegionHotWithAnyPeers(region, hotDegree)
	case LeaderCache:
		return f.isRegionHotWithPeer(region, region.GetLeader(), hotDegree)
	}
	return false
}

func (f *hotPeerCache) CollectMetrics(typ string) {
	for storeID, peers := range f.peersOfStore {
		store := storeTag(storeID)
		thresholds := f.calcHotThresholds(storeID)
		hotCacheStatusGauge.WithLabelValues("total_length", store, typ).Set(float64(peers.Len()))
		hotCacheStatusGauge.WithLabelValues("byte-rate-threshold", store, typ).Set(thresholds[ByteDim])
		hotCacheStatusGauge.WithLabelValues("key-rate-threshold", store, typ).Set(thresholds[KeyDim])
		// for compatibility
		hotCacheStatusGauge.WithLabelValues("hotThreshold", store, typ).Set(thresholds[ByteDim])
	}
}

func (f *hotPeerCache) getRegionDeltaLoads(region *core.RegionInfo) []float64 {
	statKinds := f.kind.RegionStats()
	ret := make([]float64, len(statKinds))
	for i := range ret {
		ret[i] = float64(f.getRegionStat(region, statKinds[i]))
	}
	return ret
}

func (f *hotPeerCache) getRegionStat(region *core.RegionInfo, k RegionStatKind) uint64 {
	switch k {
	case RegionReadBytes:
		return region.GetBytesRead()
	case RegionReadKeys:
		return region.GetKeysRead()
	case RegionWriteBytes:
		return region.GetBytesWritten()
	case RegionWriteKeys:
		return region.GetKeysWritten()
	}
	return 0
}

func (f *hotPeerCache) getOldHotPeerStat(regionID, storeID uint64) *HotPeerStat {
	if hotPeers, ok := f.peersOfStore[storeID]; ok {
		if v := hotPeers.Get(regionID); v != nil {
			return v.(*HotPeerStat)
		}
	}
	return nil
}

func (f *hotPeerCache) isRegionExpired(region *core.RegionInfo, storeID uint64) bool {
	switch f.kind {
	case PeerCache:
		return region.GetStorePeer(storeID) == nil
	case LeaderCache:
		return region.GetLeader().GetStoreId() != storeID
	}
	return false
}

func (f *hotPeerCache) calcHotThresholds(storeID uint64) []float64 {
	statKinds := f.kind.RegionStats()
	mins := make([]float64, len(statKinds))
	for i, k := range statKinds {
		mins[i] = minHotThresholds[k]
	}
	tn, ok := f.peersOfStore[storeID]
	if !ok || tn.Len() < topNN {
		return mins
	}
	ret := make([]float64, len(statKinds))
	for i := range ret {
		ret[i] = math.Max(tn.GetTopNMin(i).(*HotPeerStat).GetLoad(i)*hotThresholdRatio, mins[i])
	}
	return ret
}

// gets the storeIDs, including old region and new region
func (f *hotPeerCache) getAllStoreIDs(region *core.RegionInfo) []uint64 {
	storeIDs := make(map[uint64]struct{})
	ret := make([]uint64, 0, len(region.GetPeers()))
	// old stores
	ids, ok := f.storesOfRegion[region.GetID()]
	if ok {
		for storeID := range ids {
			storeIDs[storeID] = struct{}{}
			ret = append(ret, storeID)
		}
	}

	// new stores
	for _, peer := range region.GetPeers() {
		// LeaderCache no need consider the followers.
		if f.kind == LeaderCache && peer.GetStoreId() != region.GetLeader().GetStoreId() {
			continue
		}
		if _, ok := storeIDs[peer.GetStoreId()]; !ok {
			storeIDs[peer.GetStoreId()] = struct{}{}
			ret = append(ret, peer.GetStoreId())
		}
	}

	return ret
}

func (f *hotPeerCache) isRegionHotWithAnyPeers(region *core.RegionInfo, hotDegree int) bool {
	for _, peer := range region.GetPeers() {
		if f.isRegionHotWithPeer(region, peer, hotDegree) {
			return true
		}
	}
	return false
}

func (f *hotPeerCache) isRegionHotWithPeer(region *core.RegionInfo, peer *metapb.Peer, hotDegree int) bool {
	if peer == nil {
		return false
	}
	storeID := peer.GetStoreId()
	if peers, ok := f.peersOfStore[storeID]; ok {
		if stat := peers.Get(region.GetID()); stat != nil {
			return stat.(*HotPeerStat).HotDegree >= hotDegree
		}
	}
	return false
}

func (f *hotPeerCache) getDefaultTimeMedian() *movingaverage.TimeMedian {
	return movingaverage.NewTimeMedian(DefaultAotSize, rollingWindowsSize, RegionHeartBeatReportInterval)
}

func (f *hotPeerCache) updateHotPeerStat(newItem, oldItem *HotPeerStat, deltaLoads []float64, interval time.Duration) *HotPeerStat {
	thresholds := f.calcHotThresholds(newItem.StoreID)
	// if interval is zero, rate will be NaN, isHot will be false
	isHot := slice.AnyOf(newItem.Loads, func(i int) bool { return newItem.GetLoad(i) >= thresholds[i] })
	if newItem.needDelete {
		return newItem
	}

	if oldItem != nil {
		newItem.rollingLoads = oldItem.rollingLoads
		if isHot {
			newItem.HotDegree = oldItem.HotDegree + 1
			newItem.AntiCount = hotRegionAntiCount
		} else if interval != 0 {
			newItem.HotDegree = oldItem.HotDegree - 1
			newItem.AntiCount = oldItem.AntiCount - 1
			if newItem.AntiCount <= 0 {
				newItem.needDelete = true
			}
		}
	} else {
		if !isHot {
			return nil
		}
		newItem.rollingLoads = make([]*movingaverage.TimeMedian, DimLen)
		for i := 0; i < DimLen; i++ {
			newItem.rollingLoads[i] = f.getDefaultTimeMedian()
		}
		newItem.AntiCount = hotRegionAntiCount
		newItem.isNew = true
	}
	for i := 0; i < DimLen; i++ {
		newItem.rollingLoads[i].Add(deltaLoads[i], interval*time.Second)
	}

	return newItem
}
