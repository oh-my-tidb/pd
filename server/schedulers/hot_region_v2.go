// Copyright 2021 TiKV Project Authors.
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

import (
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/statistics"
	"go.uber.org/zap"
)

type hotSchedulerV2 struct {
	*BaseScheduler
	pending map[uint64]hotPendingInfluence
	conf    *hotRegionSchedulerConfig
}

func newHotSchedulerV2(opController *schedule.OperatorController, conf *hotRegionSchedulerConfig) *hotSchedulerV2 {
	base := NewBaseScheduler(opController)
	return &hotSchedulerV2{
		BaseScheduler: base,
		pending:       make(map[uint64]hotPendingInfluence),
		conf:          conf,
	}
}

func (s *hotSchedulerV2) GetName() string {
	return HotRegionName
}

func (s *hotSchedulerV2) GetType() string {
	return HotRegionType
}

func (s *hotSchedulerV2) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.conf.ServeHTTP(w, r)
}

func (s *hotSchedulerV2) GetMinInterval() time.Duration {
	return minHotScheduleInterval
}

func (s *hotSchedulerV2) GetNextInterval(interval time.Duration) time.Duration {
	return intervalGrow(s.GetMinInterval(), maxHotScheduleInterval, exponentialGrowth)
}

func (s *hotSchedulerV2) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.allowBalanceLeader(cluster) || s.allowBalanceRegion(cluster)
}

func (s *hotSchedulerV2) allowBalanceLeader(cluster opt.Cluster) bool {
	return s.OpController.OperatorCount(operator.OpHotRegion) < cluster.GetOpts().GetHotRegionScheduleLimit() &&
		s.OpController.OperatorCount(operator.OpLeader) < cluster.GetOpts().GetLeaderScheduleLimit()
}

func (s *hotSchedulerV2) allowBalanceRegion(cluster opt.Cluster) bool {
	return s.OpController.OperatorCount(operator.OpHotRegion) < cluster.GetOpts().GetHotRegionScheduleLimit()
}

func (s *hotSchedulerV2) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	storeLoads := s.summaryStoresLoads(cluster)
	weights := []float64{0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1}

	var bestPlan *hotPlan
	for _, src := range randNStores(cluster.GetStores(), 10) {
		srcLoad, ok := storeLoads[src.GetID()]
		if !ok {
			continue
		}
		for _, hotRegion := range srcLoad.hotRegions {
			region := s.getValidRegion(cluster, hotRegion)
			if region == nil {
				continue
			}

			selectPlan := func(ty hotPlanType) {
				for _, dst := range s.getDstCandidates(cluster, region, ty, src) {
					plan := newPlan(ty, src.GetID(), dst.GetID(), hotRegion, region, storeLoads, weights)
					if bestPlan == nil || plan.score > bestPlan.score {
						bestPlan = plan
					}
				}
			}

			if hotRegion.IsLeader() && s.allowBalanceLeader(cluster) {
				selectPlan(planTransferLeader)
			}
			if !hotRegion.IsLeader() && s.allowBalanceRegion(cluster) {
				selectPlan(planMovePeer)
			}
			if hotRegion.IsLeader() && s.allowBalanceLeader(cluster) && s.allowBalanceRegion(cluster) {
				selectPlan(planMoveLeader)
			}
		}
	}
	if bestPlan == nil || bestPlan.score <= 0 {
		return nil
	}
	op := s.buildOperator(cluster, bestPlan)
	if op == nil {
		return nil
	}
	return []*operator.Operator{op}
}

func (s *hotSchedulerV2) summaryStoresLoads(cluster opt.Cluster) map[uint64]*hotStoreLoad {
	storeLoads := make(map[uint64]*hotStoreLoad)
	hotLeaders := cluster.HotLeaderStats()
	hotPeers := cluster.HotPeerStats()
	for id, loads := range cluster.GetStoresLoads() {
		l := &hotStoreLoad{loads: loads}
		if leaders, ok := hotLeaders[id]; ok {
			l.hotRegions = append(l.hotRegions, randNHotRegions(leaders, 10)...)
		}
		if peers, ok := hotPeers[id]; ok {
			l.hotRegions = append(l.hotRegions, randNHotRegions(peers, 10)...)
		}
		storeLoads[id] = l
	}

	keys := make(map[uint64]struct{})
	pendingOperators := s.OpController.GetOperators()
	pendingOperators = append(pendingOperators, s.OpController.GetWaitingOperators()...)
	for _, op := range pendingOperators {
		if inf, ok := s.pending[op.RegionID()]; ok {
			keys[op.RegionID()] = struct{}{}
			if load, ok := storeLoads[inf.fromStore]; ok {
				load.loads = addLoads(load.loads, inf.loads, -1)
			}
			if load, ok := storeLoads[inf.toStore]; ok {
				load.loads = addLoads(load.loads, inf.loads, 1)
			}
		}
	}
	for k := range s.pending {
		if _, ok := keys[k]; !ok {
			delete(s.pending, k)
		}
	}
	return storeLoads
}
func (s *hotSchedulerV2) getValidRegion(cluster opt.Cluster, hotRegion *statistics.HotPeerStat) *core.RegionInfo {
	region := cluster.GetRegion(hotRegion.ID())
	if region == nil {
		return nil
	}
	if _, ok := s.pending[hotRegion.ID()]; ok {
		return nil
	}
	if !opt.IsHealthyAllowPending(cluster, region) {
		return nil
	}
	if !opt.IsRegionReplicated(cluster, region) {
		return nil
	}
	if region.GetStorePeer(hotRegion.StoreID) == nil {
		return nil
	}
	if hotRegion.IsLeader() && region.GetLeader().GetStoreId() != hotRegion.StoreID {
		return nil
	}
	return region
}

func (s *hotSchedulerV2) getDstCandidates(cluster opt.Cluster, region *core.RegionInfo, ty hotPlanType, srcStore *core.StoreInfo) []*core.StoreInfo {
	var candidates []*core.StoreInfo

	hasTransferLeader := ty == planTransferLeader || ty == planMoveLeader
	hasMovePeer := ty == planMovePeer || ty == planMoveLeader

	filters := []filter.Filter{
		&filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: hasTransferLeader, MoveRegion: hasMovePeer},
		filter.NewSpecialUseFilter(s.GetName(), filter.SpecialUseHotRegion),
	}
	if hasMovePeer {
		filters = append(filters,
			filter.NewExcludedFilter(s.GetName(), region.GetStoreIds(), region.GetStoreIds()),
			filter.NewPlacementSafeguard(s.GetName(), cluster, region, srcStore),
		)
	}
	if hasTransferLeader {
		if leaderFilter := filter.NewPlacementLeaderSafeguard(s.GetName(), cluster, region, cluster.GetLeaderStore(region)); leaderFilter != nil {
			filters = append(filters, leaderFilter)
		}
	}

	if ty == planTransferLeader {
		candidates = cluster.GetFollowerStores(region)
	} else {
		candidates = cluster.GetStores()
	}

	candidates = filter.NewCandidates(candidates).FilterTarget(cluster.GetOpts(), filters...).Stores
	return randNStores(candidates, 10)
}

func (s *hotSchedulerV2) buildOperator(cluster opt.Cluster, plan *hotPlan) *operator.Operator {
	var (
		op       *operator.Operator
		counters []prometheus.Counter
		err      error
	)
	switch plan.ty {
	case planTransferLeader:
		if plan.region.GetStoreVoter(plan.toStore) == nil {
			return nil
		}
		op, err = operator.CreateTransferLeaderOperator("transfer-hot-leader", cluster, plan.region, plan.fromStore, plan.toStore, operator.OpHotRegion)
		counters = append(counters,
			hotDirectionCounter.WithLabelValues("transfer-leader", "", strconv.FormatUint(plan.fromStore, 10), "out"),
			hotDirectionCounter.WithLabelValues("transfer-leader", "", strconv.FormatUint(plan.toStore, 10), "in"))
	case planMovePeer:
		srcPeer := plan.region.GetStorePeer(plan.fromStore) // checked in getRegionAndSrcPeer
		dstPeer := &metapb.Peer{StoreId: plan.toStore, Role: srcPeer.Role}
		op, err = operator.CreateMovePeerOperator("move-hot-peer", cluster, plan.region, operator.OpHotRegion, plan.fromStore, dstPeer)
		counters = append(counters,
			hotDirectionCounter.WithLabelValues("move-peer", "", strconv.FormatUint(plan.fromStore, 10), "out"),
			hotDirectionCounter.WithLabelValues("move-peer", "", strconv.FormatUint(plan.toStore, 10), "in"))
	case planMoveLeader:
		srcPeer := plan.region.GetStorePeer(plan.fromStore) // checked in getRegionAndSrcPeer
		dstPeer := &metapb.Peer{StoreId: plan.toStore, Role: srcPeer.Role}
		op, err = operator.CreateMoveLeaderOperator("move-hot-leader", cluster, plan.region, operator.OpHotRegion, plan.fromStore, dstPeer)
		counters = append(counters,
			hotDirectionCounter.WithLabelValues("move-leader", "", strconv.FormatUint(plan.fromStore, 10), "out"),
			hotDirectionCounter.WithLabelValues("move-leader", "", strconv.FormatUint(plan.toStore, 10), "in"))
	}
	if err != nil {
		log.Debug("fail to create operator", zap.Stringer("op-type", plan.ty), errs.ZapError(err))
		schedulerCounter.WithLabelValues(s.GetName(), "create-operator-fail").Inc()
		return nil
	}
	op.SetPriorityLevel(core.HighPriority)
	op.Counters = append(op.Counters, counters...)
	op.Counters = append(op.Counters,
		schedulerCounter.WithLabelValues(s.GetName(), "new-operator"),
		schedulerCounter.WithLabelValues(s.GetName(), ""))

	loads := make([]float64, int(statistics.StoreStatCount))
	if plan.ty == planTransferLeader || plan.ty == planMoveLeader {
		loads[statistics.StoreReadBytesSum] = plan.hotRegion.GetLoad(statistics.RegionReadBytes)
		loads[statistics.StoreReadKeysSum] = plan.hotRegion.GetLoad(statistics.RegionReadKeys)
		loads[statistics.StoreLeaderWriteBytesSum] = plan.hotRegion.GetLoad(statistics.RegionWriteBytes)
		loads[statistics.StoreLeaderWriteKeysSum] = plan.hotRegion.GetLoad(statistics.RegionWriteKeys)
	}
	if plan.ty == planMovePeer || plan.ty == planMoveLeader {
		loads[statistics.StoreWriteBytes] = plan.hotRegion.GetLoad(statistics.RegionWriteBytes)
		loads[statistics.StoreWriteKeys] = plan.hotRegion.GetLoad(statistics.RegionWriteKeys)
	}
	s.pending[plan.hotRegion.RegionID] = hotPendingInfluence{
		fromStore: plan.fromStore,
		toStore:   plan.toStore,
		loads:     loads,
	}

	return op
}

func randNStores(stores []*core.StoreInfo, n int) []*core.StoreInfo {
	if len(stores) <= n {
		return stores
	}
	rand.Shuffle(len(stores), func(i, j int) {
		stores[i], stores[j] = stores[j], stores[i]
	})
	return stores[:n]
}

func randNHotRegions(regions []*statistics.HotPeerStat, n int) []*statistics.HotPeerStat {
	if len(regions) <= n {
		return regions
	}
	rand.Shuffle(len(regions), func(i, j int) {
		regions[i], regions[j] = regions[j], regions[i]
	})
	return regions[:n]
}

type hotPendingInfluence struct {
	fromStore, toStore uint64
	loads              []float64
}

type hotStoreLoad struct {
	loads      []float64
	hotRegions []*statistics.HotPeerStat
}

type hotPlanType int

const (
	planTransferLeader hotPlanType = iota
	planMovePeer
	planMoveLeader
)

func (t hotPlanType) String() string {
	switch t {
	case planTransferLeader:
		return "trasnfer-leader"
	case planMovePeer:
		return "move-peer"
	case planMoveLeader:
		return "move-leader"
	}
	return "unknown-plan-type"
}

type hotPlan struct {
	ty                 hotPlanType
	fromStore, toStore uint64
	hotRegion          *statistics.HotPeerStat
	region             *core.RegionInfo
	score              float64
}

func newPlan(ty hotPlanType, from, to uint64, hotRegion *statistics.HotPeerStat, region *core.RegionInfo, loads map[uint64]*hotStoreLoad, weights []float64) *hotPlan {
	p := &hotPlan{
		ty:        ty,
		fromStore: from,
		toStore:   to,
		hotRegion: hotRegion,
		region:    region,
	}
	p.calculateScore(loads, weights)
	return p
}

func (p *hotPlan) calculateScore(loads map[uint64]*hotStoreLoad, weights []float64) {
	if p.ty == planTransferLeader || p.ty == planMoveLeader {
		p.score += p.dimScore(loads, weights, statistics.StoreReadBytesSum, statistics.RegionReadBytes)
		p.score += p.dimScore(loads, weights, statistics.StoreReadKeysSum, statistics.RegionReadKeys)
		p.score += p.dimScore(loads, weights, statistics.StoreLeaderWriteBytesSum, statistics.RegionWriteBytes)
		p.score += p.dimScore(loads, weights, statistics.StoreLeaderWriteKeysSum, statistics.RegionWriteKeys)
	}
	if p.ty == planMovePeer || p.ty == planMoveLeader {
		p.score += p.dimScore(loads, weights, statistics.StoreWriteBytesSum, statistics.RegionWriteBytes)
		p.score += p.dimScore(loads, weights, statistics.StoreWriteKeysSum, statistics.RegionWriteKeys)
	}
}

func (p *hotPlan) dimScore(loads map[uint64]*hotStoreLoad, weights []float64, storeIdx statistics.StoreStatKind, regionIdx statistics.RegionStatKind) float64 {
	fromLoads := loads[p.fromStore].loads
	toLoads := loads[p.toStore].loads
	from := fromLoads[storeIdx] - p.hotRegion.GetLoad(regionIdx)
	to := toLoads[storeIdx] + p.hotRegion.GetLoad(regionIdx)
	weight := weights[storeIdx]
	if from > to && 0.95*from > to {
		d := weight * (from - to) * 100.0 / from
		return d * d
	}
	if from < to && 0.95*to > from {
		d := weight * (from - to) * 100 / to
		return -d * d
	}
	return 0
}

func addLoads(a, b []float64, w float64) []float64 {
	loads := make([]float64, len(a))
	for i := range loads {
		loads[i] = a[i] + w*b[i]
	}
	return loads
}
