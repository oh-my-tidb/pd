// Copyright 2020 TiKV Project Authors.
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

// HotStat contains cluster's hotspot statistics.
type HotStat struct {
	*HotCache
	*StoresStats
}

// NewHotStat creates the container to hold cluster's hotspot statistics.
func NewHotStat() *HotStat {
	return &HotStat{
		HotCache:    NewHotCache(),
		StoresStats: NewStoresStats(),
	}
}

// GetStoresLoads returns all stores loads for scheduling.
func (s *HotStat) GetStoresLoads(minHotDegree int) map[uint64][]float64 {
	ret := s.StoresStats.getStoresLoads()
	peers := s.HotCache.RegionStats(PeerCache, minHotDegree)
	for id := range ret {
		ret[id][StoreWriteBytesSum] = 0
		ret[id][StoreWriteKeysSum] = 0
		ret[id][StoreLeaderWriteBytesSum] = 0
		ret[id][StoreLeaderWriteKeysSum] = 0
		for _, peer := range peers[id] {
			ret[id][StoreWriteBytesSum] += peer.GetLoad(RegionWriteBytes)
			ret[id][StoreWriteKeysSum] += peer.GetLoad(RegionWriteKeys)
			if peer.isLeader {
				ret[id][StoreLeaderWriteBytesSum] += peer.GetLoad(RegionWriteBytes)
				ret[id][StoreLeaderWriteKeysSum] += peer.GetLoad(RegionWriteKeys)
			}
		}
	}

	leaders := s.HotCache.RegionStats(LeaderCache, minHotDegree)
	for id := range ret {
		ret[id][StoreReadBytesSum] = 0
		ret[id][StoreReadKeysSum] = 0
		for _, leader := range leaders[id] {
			ret[id][StoreReadBytesSum] += leader.GetLoad(RegionReadBytes)
			ret[id][StoreReadKeysSum] += leader.GetLoad(RegionReadKeys)

		}
	}
	return ret
}
