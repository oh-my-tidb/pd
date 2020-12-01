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

// HotCacheKind is a identify HotCache types.
type HotCacheKind int

// Flags for hotCache.
const (
	PeerCache HotCacheKind = iota
	LeaderCache
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
