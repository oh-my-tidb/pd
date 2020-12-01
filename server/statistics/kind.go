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

type RegionStatKind int

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

type StoreStatKind int

const (
	StoreReadBytes StoreStatKind = iota
	StoreReadKeys
	StoreWriteBytes
	StoreWriteKeys
	StoreSumLeaderBytes
	StoreSumLeaderKeys

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
	case StoreSumLeaderBytes:
		return "store_leader_write_bytes"
	case StoreSumLeaderKeys:
		return "store_leader_write_keys"
	}

	return "unknown StoreStatKind"
}

type HotCacheKind int

const (
	PeerCache HotCacheKind = iota
	LeaderCache

	HotCacheCount
)

func (k HotCacheKind) String() string {
	switch k {
	case PeerCache:
		return "peer"
	case LeaderCache:
		return "leader"
	}
	return ""
}
