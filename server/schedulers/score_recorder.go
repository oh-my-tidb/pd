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

package schedulers

// type record struct {
// 	size        int
// 	idx         int
// 	time, value []float64
// }

// func (r *record) push(v float64) {
// 	if len(r.value) < r.size {
// 		r.value = append(r.value, v)
// 		r.time = append(r.time, float64(time.Now().Unix()))
// 		r.idx++
// 		return
// 	}
// 	if r.idx >= r.size {
// 		r.idx = 0
// 	}
// 	r.value[r.idx] = v
// 	r.time[r.idx] = float64(time.Now().Unix())
// 	r.idx++
// }

// type scoreRecorder struct {
// 	size   int
// 	scores map[uint64]*record
// }

// func newScoreRecorder(size int) *scoreRecorder {
// 	return &scoreRecorder{
// 		size:   size,
// 		scores: make(map[uint64]*record),
// 	}
// }

// func (r *scoreRecorder) record() {

// }
