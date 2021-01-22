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

// HotCacheKind identifies HotCache types.
type HotCacheKind int

// Different hot cache kinds.
const (
	PeerCache HotCacheKind = iota
	LeaderCache

	CacheKindCount
)

func (k HotCacheKind) String() string {
	switch k {
	case PeerCache:
		return "peer"
	case LeaderCache:
		return "leader"
	}
	return "unimplemented"
}

// RegionStats returns region statistics kinds that corresponding cache interests.
func (k HotCacheKind) RegionStats() []RegionStatKind {
	switch k {
	case PeerCache:
		return []RegionStatKind{RegionWriteBytes, RegionWriteKeys}
	case LeaderCache:
		return []RegionStatKind{RegionReadBytes, RegionReadKeys}
	}
	return nil
}

// RegionStatKind represents the statistics type of region.
type RegionStatKind int

// Different region statistics kinds.
const (
	RegionReadBytes RegionStatKind = iota
	RegionReadKeys
	RegionWriteBytes
	RegionWriteKeys

	RegionStatCount
)

func (k RegionStatKind) String() string {
	switch k {
	case RegionReadBytes:
		return "read_bytes"
	case RegionReadKeys:
		return "read_keys"
	case RegionWriteBytes:
		return "write_bytes"
	case RegionWriteKeys:
		return "write_keys"
	}
	return "unknown RegionStatKind"
}

// StoreStatKind represents the statistics type of store.
type StoreStatKind int

// Different store statistics kinds.
const (
	StoreReadBytes StoreStatKind = iota
	StoreReadKeys
	StoreWriteBytes
	StoreWriteKeys
	StoreCPUUsage
	StoreDiskReadRate
	StoreDiskWriteRate

	// summary of region stats
	StoreReadBytesSum
	StoreReadKeysSum
	StoreWriteBytesSum
	StoreWriteKeysSum
	StoreLeaderWriteBytesSum
	StoreLeaderWriteKeysSum

	StoreStatCount
)

func (k StoreStatKind) String() string {
	switch k {
	case StoreReadBytes:
		return "store_read_bytes"
	case StoreReadKeys:
		return "store_read_keys"
	case StoreWriteBytes:
		return "store_write_bytes"
	case StoreWriteKeys:
		return "store_write_keys"
	case StoreCPUUsage:
		return "store_cpu_usage"
	case StoreDiskReadRate:
		return "store_disk_read_rate"
	case StoreDiskWriteRate:
		return "store_disk_write_rate"
	case StoreReadBytesSum:
		return "store_read_bytes_sum"
	case StoreReadKeysSum:
		return "store_read_keys_sum"
	case StoreWriteBytesSum:
		return "store_write_bytes_sum"
	case StoreWriteKeysSum:
		return "store_write_keys_sum"
	case StoreLeaderWriteBytesSum:
		return "store_leader_write_Bytes_sum"
	case StoreLeaderWriteKeysSum:
		return "store_leader_write_keys_sum"
	}

	return "unknown StoreStatKind"
}
