// Copyright 2018 TiKV Project Authors.
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
	"math/rand"

	"github.com/tikv/pd/server/core"
)

// Denoising is an option to calculate flow base on the real heartbeats. Should
// only turned off by the simulator and the test.
var Denoising = true

// HotCache is a cache hold hot regions.
type HotCache struct {
	peerCache   *hotPeerCache
	leaderCache *hotPeerCache
}

// NewHotCache creates a new hot spot cache.
func NewHotCache() *HotCache {
	return &HotCache{
		peerCache:   NewHotStoresStats(PeerCache),
		leaderCache: NewHotStoresStats(LeaderCache),
	}
}

// CheckWrite checks the write status, returns update items.
func (w *HotCache) CheckWrite(region *core.RegionInfo) []*HotPeerStat {
	return w.peerCache.CheckRegionFlow(region)
}

// CheckRead checks the read status, returns update items.
func (w *HotCache) CheckRead(region *core.RegionInfo) []*HotPeerStat {
	return w.leaderCache.CheckRegionFlow(region)
}

// Update updates the cache.
func (w *HotCache) Update(item *HotPeerStat) {
	switch item.Kind {
	case PeerCache:
		w.peerCache.Update(item)
	case LeaderCache:
		w.leaderCache.Update(item)
	}

	if item.IsNeedDelete() {
		w.incMetrics("remove_item", item.StoreID, item.Kind)
	} else if item.IsNew() {
		w.incMetrics("add_item", item.StoreID, item.Kind)
	} else {
		w.incMetrics("update_item", item.StoreID, item.Kind)
	}
}

// RegionStats returns hot items according to kind
func (w *HotCache) RegionStats(kind HotCacheKind) map[uint64][]*HotPeerStat {
	switch kind {
	case PeerCache:
		return w.peerCache.RegionStats()
	case LeaderCache:
		return w.leaderCache.RegionStats()
	}
	return nil
}

// RandHotRegionFromStore random picks a hot region in specify store.
func (w *HotCache) RandHotRegionFromStore(storeID uint64, kind HotCacheKind, hotDegree int) *HotPeerStat {
	if stats, ok := w.RegionStats(kind)[storeID]; ok {
		for _, i := range rand.Perm(len(stats)) {
			if stats[i].HotDegree >= hotDegree {
				return stats[i]
			}
		}
	}
	return nil
}

// IsRegionHot checks if the region is hot.
func (w *HotCache) IsRegionHot(region *core.RegionInfo, hotDegree int) bool {
	return w.peerCache.IsRegionHot(region, hotDegree) ||
		w.leaderCache.IsRegionHot(region, hotDegree)
}

// CollectMetrics collects the hot cache metrics.
func (w *HotCache) CollectMetrics() {
	w.peerCache.CollectMetrics("peer")
	w.leaderCache.CollectMetrics("leader")
}

// ResetMetrics resets the hot cache metrics.
func (w *HotCache) ResetMetrics() {
	hotCacheStatusGauge.Reset()
}

func (w *HotCache) incMetrics(name string, storeID uint64, kind HotCacheKind) {
	store := storeTag(storeID)
	switch kind {
	case PeerCache:
		hotCacheStatusGauge.WithLabelValues(name, store, "peer").Inc()
	case LeaderCache:
		hotCacheStatusGauge.WithLabelValues(name, store, "leader").Inc()
	}
}

// GetFilledPeriod returns filled period.
func (w *HotCache) GetFilledPeriod(kind HotCacheKind) int {
	switch kind {
	case PeerCache:
		return w.peerCache.getDefaultTimeMedian().GetFilledPeriod()
	case LeaderCache:
		return w.leaderCache.getDefaultTimeMedian().GetFilledPeriod()
	}
	return 0
}
