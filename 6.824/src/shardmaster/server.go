package shardmaster


import "raft"
import "labrpc"
import "sync"
import "labgob"
import "fmt"
import "time"

const DebugOn bool = false

func (sm *ShardMaster) Log (format string, a ...interface{}) (n int, err error) {
    if DebugOn {
        fmt.Printf("[master%d] %s", sm.me,
            fmt.Sprintf(format, a...))
    }
    return
}

type Session struct {
    LastApplied     map[int64]int64
}

type ShardMaster struct {
    mu              sync.Mutex
    me              int
    rf              *raft.Raft
    applyCh         chan raft.ApplyMsg

    // Your data here.
    session         Session

    configs         []Config // indexed by config num

    replyMapMtx     sync.Mutex

    replyMap        map[Info]chan chan OpReply

    opCh            chan Op
}

func (config *Config) Copy () Config {
    n := Config{}
    n.Num = config.Num

    for i, val := range config.Shards {
        n.Shards[i] = val
    }

    n.Groups = make(map[int][]string)
    for key, value := range config.Groups {
        n.Groups[key] = value
    }

    return n
}

func (sm *ShardMaster) Join (args *JoinArgs, reply *JoinReply) {
    // Your code here.
    opRequest := &Op{Type: ReqJoin, ArgsJoin: *args}
    opReply := sm.HandleRequest(opRequest)
    reply.Fill(&opReply)
}

func (sm *ShardMaster) Leave (args *LeaveArgs, reply *LeaveReply) {
    // Your code here.
    opRequest := &Op{Type: ReqLeave, ArgsLeave: *args}
    opReply := sm.HandleRequest(opRequest)
    reply.Fill(&opReply)
}

func (sm *ShardMaster) Move (args *MoveArgs, reply *MoveReply) {
    // Your code here.
    opRequest := &Op{Type: ReqMove, ArgsMove: *args}
    opReply := sm.HandleRequest(opRequest)
    reply.Fill(&opReply)
}

func (sm *ShardMaster) Query (args *QueryArgs, reply *QueryReply) {
    // Your code here.
    opRequest := &Op{Type: ReqQuery, ArgsQuery: *args}
    opReply := sm.HandleRequest(opRequest)
    reply.Fill(&opReply)
}

func (sm *ShardMaster) RegisterReplyCh (info Info, replyCh chan chan OpReply) {
    sm.replyMapMtx.Lock()
    defer sm.replyMapMtx.Unlock()

    _, ok := sm.replyMap[info]
    if ok {
        panic("already has uuid in map!")
    }
    //sm.Log("reg: %d:%d %v\n", info.Clerk, info.ID, replyCh)
    sm.replyMap[info] = replyCh
}

func (sm *ShardMaster) UnregisterReplyCh (info Info) {
    sm.replyMapMtx.Lock()
    defer sm.replyMapMtx.Unlock()

    replyCh, ok := sm.replyMap[info]
    if ok {
        close(replyCh)
        //sm.Log("unreg: %d:%d %v\n", info.Clerk, info.ID, replyCh)
        delete(sm.replyMap, info)
    }
}

func (sm *ShardMaster) UnregisterNotUsedReplyCh (info Info) bool {
    sm.replyMapMtx.Lock()
    defer sm.replyMapMtx.Unlock()

    replyCh, ok := sm.replyMap[info]
    if !ok {
        panic("no uuid in map!")
    }

    if len(replyCh) > 0 {
        close(replyCh)
        sm.Log("unreg: %d:%d %v\n", info.Clerk, info.ID, replyCh)
        delete(sm.replyMap, info)
        return true
    } else {
        return false
    }
}

func (sm *ShardMaster) GetReplyCh (info Info) (chan chan OpReply, bool) {
    sm.replyMapMtx.Lock()
    defer sm.replyMapMtx.Unlock()

    ch, ok := sm.replyMap[info]
    return ch, ok
}

func (sm *ShardMaster) HandleRequest (request *Op) OpReply {

    reqType := request.Type
    info := request.GetInfo()

    replyCh := make(chan chan OpReply, 1)
    sm.RegisterReplyCh(info, replyCh)
    defer sm.UnregisterReplyCh(info)

    _, _, isLeader := sm.rf.Start(*request)
    if isLeader {
        opReplyCh := make(chan OpReply)
        replyCh <-opReplyCh

        timeoutCnt := 0
        for {
            ticker := time.NewTicker(time.Duration(1) * time.Second)
            select {
            case opReply := <-opReplyCh:
                return opReply

            case <-ticker.C:
                sm.Log("HandleRequest: %s timeout!\n", reqType)
                timeoutCnt += 1
                if timeoutCnt > 1 {
                    panic("too much timeout!")
                }

                if sm.UnregisterNotUsedReplyCh(info) {
                    return OpReply{Type: request.Type,
                                   WrongLeader: false}   // FIXME
                }
            }
        }
    } else {
        return OpReply{Type: request.Type,
                       WrongLeader: true}
    }

    panic(0)
    return OpReply{}
}

func (sm *ShardMaster) JoinAdjustShards (shards *[NShards]int, gids []int) {
    //sm.Log("before JoinAdjust: %v\n", shards)

    gid_map := map[int]int{}
    for _, gid := range shards {
        if gid != 0 {
            if _, ok := gid_map[gid]; ok {
                gid_map[gid] += 1
            } else {
                gid_map[gid] = 1
            }
        }
    }

    for _, new_gid := range gids {
        if _, ok := gid_map[new_gid]; !ok {
            length := len(gid_map) + 1
            min := NShards/length
            max := min
            if NShards%length > 0 {
                max += 1
            }
            if min > 0 {
                gid_map[new_gid] = 0
                for i, gid := range shards {
                    if _, ok := gid_map[gid]; ok {
                        if gid_map[gid] > min {
                            shards[i] = new_gid
                            gid_map[gid] -= 1
                            gid_map[new_gid] += 1
                            if gid_map[new_gid] >= max {
                                break
                            }
                        }
                    } else {
                        shards[i] = new_gid
                        gid_map[new_gid] += 1
                        if gid_map[new_gid] >= max {
                            break
                        }
                    }
                }
            }
        }
    }

    //sm.Log("after JoinAdjust: %v\n", shards)
}

func (sm *ShardMaster) LeaveAdjustShards (shards *[NShards]int, gids []int, unused_gids []int) {
    //sm.Log("before LeaveAdjust: %v\n", shards)

    gid_map := map[int]int{}
    for _, gid := range shards {
        if gid != 0 {
            if _, ok := gid_map[gid]; ok {
                gid_map[gid] += 1
            } else {
                gid_map[gid] = 1
            }
        }
    }

    for _, rm_gid := range gids {
        if _, ok := gid_map[rm_gid]; ok {
            if len(unused_gids) > 0 {
                new_gid := unused_gids[0]
                gid_map[new_gid] = 0
                for i, _ := range shards {
                    if shards[i] == rm_gid {
                        shards[i] = new_gid
                        gid_map[new_gid] += 1
                    }
                }
                unused_gids = unused_gids[1:]
            } else {
                length := len(gid_map) - 1
                if length > 0 {
                    min := NShards/length
                    max := min
                    if NShards%length > 0 {
                        max += 1
                    }

                    var gid_to_add []int
                    for used_gid, cnt := range gid_map {
                        if used_gid != rm_gid {
                            for i := 0; i < max - cnt; i++ {
                                gid_to_add = append(gid_to_add, used_gid)
                            }
                        }
                    }

                    for i, tgid := range shards {
                        if tgid == rm_gid {
                            gid_added := gid_to_add[0]
                            shards[i] = gid_added
                            gid_to_add = gid_to_add[1:]
                            gid_map[gid_added] += 1
                        }
                    }
                } else {
                    for i := 0; i < NShards; i++ {
                        shards[i] = 0
                    }
                }
            }
            delete(gid_map, rm_gid)
        }
    }

    //sm.Log("after LeaveAdjust: %v\n", shards)
}

func (sm *ShardMaster) ApplyJoin (args *JoinArgs) OpReply {
    sm.Log("applyJoin %v\n", args)
    last := len(sm.configs) - 1
    config := sm.configs[last].Copy()
    config.Num += 1

    var gids []int
    for gid, server := range args.Servers {
        config.Groups[gid] = server
        gids = append(gids, gid)
    }

    sm.JoinAdjustShards(&config.Shards, gids)

    sm.configs = append(sm.configs, config)

    return OpReply {Type: ReqJoin,
                    WrongLeader: false,
                    Err: OK}
}

func (sm *ShardMaster) ApplyLeave (args *LeaveArgs) OpReply {
    sm.Log("applyLeave %v\n", args)
    last := len(sm.configs) - 1
    config := sm.configs[last].Copy()
    config.Num += 1

    unused_gids := config.GetUnusedGid()
    leave_map := map[int]bool{}
    for _, gid := range args.GIDs {
        leave_map[gid] = true
    }
    for i := 0; i < len(unused_gids); {
        gid := unused_gids[i]
        if _, ok := leave_map[gid]; ok {
            unused_gids = append(unused_gids[:i], unused_gids[i+1:]...)
        } else {
            i++
        }
    }

    var gids []int
    for _, gid := range args.GIDs {
        delete(config.Groups, gid)
        gids = append(gids, gid)
    }

    sm.LeaveAdjustShards(&config.Shards, gids, unused_gids)

    sm.configs = append(sm.configs, config)
    return OpReply {Type: ReqLeave,
                    WrongLeader: false,
                    Err: OK}
}

func (sm *ShardMaster) ApplyMove (args *MoveArgs) OpReply {
    sm.Log("applyMove %v\n", args)
    last := len(sm.configs) - 1
    config := sm.configs[last].Copy()
    config.Num += 1

    config.Shards[args.Shard] = args.GID

    sm.configs = append(sm.configs, config)
    return OpReply {Type: ReqMove,
                    WrongLeader: false,
                    Err: OK}
}

func (sm *ShardMaster) ApplyQuery (args *QueryArgs) OpReply {
    sm.Log("applyQuery %v\n", args)
    last := len(sm.configs) - 1
    num := args.Num

    if num == -1 || num > last {
        return OpReply {Type: ReqQuery,
                        WrongLeader: false,
                        Err: OK,
                        Config: sm.configs[last]}
    } else {
        return OpReply {Type: ReqQuery,
                        WrongLeader: false,
                        Err: OK,
                        Config: sm.configs[num]}
    }
}

func (sm *ShardMaster) ApplyOp (op *Op) OpReply {

    info := op.GetInfo()

    if op.Type != ReqQuery {
        if sm.session.LastApplied[info.Clerk] == info.ID {
            sm.Log("skip duplicated operation: %v\n", *op)
            return OpReply{Type: op.Type,
                           Err: OK}
        }
    }

    var ret OpReply

    switch op.Type {
    case ReqJoin:
        ret = sm.ApplyJoin(&op.ArgsJoin)
    case ReqLeave:
        ret = sm.ApplyLeave(&op.ArgsLeave)
    case ReqMove:
        ret = sm.ApplyMove(&op.ArgsMove)
    case ReqQuery:
        ret = sm.ApplyQuery(&op.ArgsQuery)
    default:
        panic(0)
    }

    return ret
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) PollingApplyCh () {

    for {
        ticker := time.NewTicker(time.Duration(1) * time.Second)
        select {

        case applyMsg := <-sm.applyCh:

            if op, valid := applyMsg.Command.(Op); valid {

                opReply := sm.ApplyOp(&op)
                info := op.GetInfo()

                replyCh, ok := sm.GetReplyCh(info)

                if ok {
                    ch := <-replyCh
                    Loop:
                    for {
                        ticker := time.NewTicker(time.Duration(1) * time.Second)
                        select {
                        case ch <- opReply:
                            break Loop
                        case <-ticker.C:
                            sm.Log("stuck in apply loop on info:%v\n", info)
                        }
                    }
                }

                if op.Type != ReqQuery {
                    sm.session.LastApplied[info.Clerk] = info.ID
                }

            } else {
                panic("invalid applyMsg!")
            }

        case <-ticker.C:
            //sm.Log("No request timeout!\n")
        }
    }
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
    sm := new(ShardMaster)
    sm.me = me

    sm.session = Session{LastApplied: make(map[int64]int64)}

    sm.configs = make([]Config, 1)
    sm.configs[0].Groups = map[int][]string{}

    labgob.Register(Op{})
    sm.applyCh = make(chan raft.ApplyMsg)
    sm.rf = raft.Make(servers, me, persister, sm.applyCh)

    // Your code here.
    sm.replyMap = make(map[Info]chan chan OpReply)
    sm.opCh = make(chan Op)

    go sm.PollingApplyCh()

    return sm
}
