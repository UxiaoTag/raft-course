package shardctrler

import "sort"

type CtrlerStateMachine struct {
	Configs []Config
}

func NewCtrlerStateMachine() *CtrlerStateMachine {
	cf := &CtrlerStateMachine{Configs: make([]Config, 1)}
	cf.Configs[0] = DefaultConfig()
	return cf
}

func (csm *CtrlerStateMachine) Query(num int) (Config, Err) {
	if num >= len(csm.Configs) || num < 0 {
		return csm.Configs[len(csm.Configs)-1], OK
	}
	return csm.Configs[num], OK
}

func (csm *CtrlerStateMachine) Join(groups map[int][]string) Err {
	num := len(csm.Configs)
	lastConfig := csm.Configs[num-1]
	//构建一个新配置
	NewConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	//将新group加入到groups
	for gid, server := range groups {
		if _, ok := NewConfig.Groups[gid]; !ok {
			newServer := make([]string, len(server))
			//这里排错发现copy(server,newServer)
			copy(newServer, server)
			NewConfig.Groups[gid] = newServer
		}
	}

	//构造gid->shardid的映射
	//从shard->gid
	// 0 1
	// 1 1
	// 2 2
	// 3 3
	// 变成gid->shardid的映射
	// 1 0,1
	// 2 2
	// 3 3
	// 4
	gidToShards := make(map[int][]int)

	for gid := range NewConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}

	for shard, gid := range NewConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	//对gid->shardid的映射进行负载均衡修改
	//like
	// gid->shardid的映射
	// 1 0,1
	// 2 2
	// 3 3
	// 4
	//max 1 0,1->1 1
	//min 4 -> 4 0
	// 1 1
	// 2 2
	// 3 3
	// 4 0
	for {
		maxGid, minGid := gidToShardsMaxMin(gidToShards)
		//当maxGid不等于0且差值小于1，表示负载均衡
		if maxGid != 0 && len(gidToShards[maxGid])-len(gidToShards[minGid]) <= 1 {
			break
		}
		//最少shard的gid+1个shard
		gidToShards[minGid] = append(gidToShards[minGid], gidToShards[maxGid][0])
		//最多shard的gid-1个shard
		gidToShards[maxGid] = gidToShards[maxGid][1:]
	}

	//再将gid->shardid的映射变回shard->gid，重新存储
	var newShard [NShards]int
	for gid, shards := range gidToShards {
		for _, shard := range shards {
			newShard[shard] = gid
		}
	}

	NewConfig.Shards = newShard
	csm.Configs = append(csm.Configs, NewConfig)

	return OK
}

func (csm *CtrlerStateMachine) Leave(gids []int) Err {
	num := len(csm.Configs)
	lastConfig := csm.Configs[num-1]
	//构建一个新配置
	NewConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	//构造gid->shardid的映射
	gidToShards := make(map[int][]int)

	for gid := range NewConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}

	for shard, gid := range NewConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	//将旧group移除groups
	var unassignedShards []int
	for _, gid := range gids {
		//	如果 gid 在 Group 中，则删除掉
		if _, ok := NewConfig.Groups[gid]; ok {
			delete(NewConfig.Groups, gid)
		}
		// 取出shard
		if shards, ok := gidToShards[gid]; ok {
			unassignedShards = append(unassignedShards, shards...)
			delete(gidToShards, gid)
		}
	}

	//重新分配新的删除gid的对应的shard
	var newShard [NShards]int
	if len(NewConfig.Groups) != 0 {
		for _, unassignedShard := range unassignedShards {
			_, MinGid := gidToShardsMaxMin(gidToShards)
			gidToShards[MinGid] = append(gidToShards[MinGid], unassignedShard)
		}

		//再将gid->shardid的映射变回shard->gid，重新存储
		for gid, shards := range gidToShards {
			for _, shard := range shards {
				newShard[shard] = gid
			}
		}
		//把赋值和append放在了这个地方导致bug
	}
	NewConfig.Shards = newShard
	csm.Configs = append(csm.Configs, NewConfig)
	return OK
}

func (csm *CtrlerStateMachine) Move(Shard, gid int) Err {
	num := len(csm.Configs)
	lastConfig := csm.Configs[num-1]
	//构建一个新配置
	NewConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}
	NewConfig.Shards[Shard] = gid
	csm.Configs = append(csm.Configs, NewConfig)
	return OK
}

func copyGroups(groups map[int][]string) map[int][]string {
	//保证复制是新建一个，而不是指针复制
	newGroups := make(map[int][]string, len(groups))
	for gid, server := range groups {
		newserver := make([]string, len(server))
		copy(newserver, server)
		newGroups[gid] = newserver
	}
	return newGroups
}

//这里将max和min写在一起
func gidToShardsMaxMin(gidToShards map[int][]int) (int, int) {
	//这里是必要的判断嘛？主要是对取到0之后直接返回0的操作不太理解,意思是不希望gidToShards[0]有东西，有就把gidToShards[0]当max清空先？？？
	//考虑到可能会出问题，写在一起做个flag把
	ZeroNoEmpty := false
	if shard, ok := gidToShards[0]; ok && len(shard) > 0 {
		ZeroNoEmpty = true
	}

	//我觉得这里也是非必要的，排序与否，我们都能找出最大和最小，这里的意思是保证每个节点的最大最小一致的意思嘛？
	var gids []int
	for gid := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	maxGid, maxShard, minGid, minShard := -1, -1, -1, NShards+1
	for _, gid := range gids {
		if len(gidToShards[gid]) > maxShard {
			maxGid, maxShard = gid, len(gidToShards[gid])
		}
		if gid != 0 && len(gidToShards[gid]) < minShard {
			minGid, minShard = gid, len(gidToShards[gid])
		}
	}

	if ZeroNoEmpty {
		return 0, minGid
	}
	return maxGid, minGid
}
