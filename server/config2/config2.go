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

package config2

var schedulerMap = make(map[string]struct{})

// RegisterScheduler registers the scheduler type.
func RegisterScheduler(typ string) {
	schedulerMap[typ] = struct{}{}
}

// IsSchedulerRegistered check where the named scheduler type is registered.
func IsSchedulerRegistered(name string) bool {
	_, ok := schedulerMap[name]
	return ok
}

// StoreLimitType indicates the type of store limit
type StoreLimitType int

const (
	// AddPeer indicates the type of store limit that limits the adding peer rate
	AddPeer StoreLimitType = iota
	// RemovePeer indicates the type of store limit that limits the removing peer rate
	RemovePeer
)

// LimitTypeValue indicates the name of store limit type and the enum value
var LimitTypeValue = map[string]StoreLimitType{
	"add-peer":    AddPeer,
	"remove-peer": RemovePeer,
}

// String returns the representation of the StoreLimitType
func (t StoreLimitType) String() string {
	for n, v := range LimitTypeValue {
		if v == t {
			return n
		}
	}
	return ""
}
