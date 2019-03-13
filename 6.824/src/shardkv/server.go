package shardkv

import (
    "bytes"
    "fmt"
    "labgob"
    "labrpc"
    "raft"
    "shardmaster"
    "sync"
    "time"
)

const Debug = true

func (kv *ShardKV) Log (format string, a ...interface{}) (n int, err error) {
    kv.configMtx.Lock()
    curConfig := kv.config
    kv.configMtx.Unlock()
    if Debug {
        fmt.Printf("[server-%d-%d:cfg-%d:%v] %s",
            kv.gid, kv.me, curConfig.Num, kv.shardToRecv,
            fmt.Sprintf(format, a...))
    }
    return
}

type Session struct {
    LastApplied     map[int64]int64
}

type ShardToRecv struct {
    Gid             int
    ConfigNum       int // FIXME
    Unfinished      map[int]int    // shardId -> from_gid
}

func (sr ShardToRecv) String () string {
    recv := []int{}
    for shardId, _ := range sr.Unfinished {
        recv = append(recv, shardId)
    }
    return fmt.Sprintf("toRecv%v", recv)
}

func (sr *ShardToRecv) Empty () bool {
    return len(sr.Unfinished) == 0
}

func (sr *ShardToRecv) Update (oldConfig, newConfig *shardmaster.Config) bool {
    if !sr.Empty() {
        return false
    }
    oldShards := oldConfig.Shards
    newShards := newConfig.Shards
    for id, tgid := range newShards {
        if tgid == sr.Gid && oldShards[id] != 0 && oldShards[id] != sr.Gid {
            sr.Unfinished[id] = tgid
        }
    }
    sr.ConfigNum = newConfig.Num
    return true
}

func (sr *ShardToRecv) IsReceived (shardId int) bool {
    if _, ok := sr.Unfinished[shardId]; ok {
        return false
    } else {
        return true
    }
}

func (sr *ShardToRecv) Remove (shardId int) bool {
    if _, ok := sr.Unfinished[shardId]; ok {
        delete(sr.Unfinished, shardId)
        return true
    } else {
        return false
    }
}

type ShardKV struct {

    mu              sync.Mutex

    me              int

    rf              *raft.Raft

    applyCh         chan raft.ApplyMsg

    make_end        func(string) *labrpc.ClientEnd

    gid             int

    masters         []*labrpc.ClientEnd

    maxraftstate    int // snapshot if log grows this big

    // Your definitions here.
    configMtx       sync.Mutex

    config          shardmaster.Config  // persistent

    shardToRecv     ShardToRecv         // persistent, record shards to be accepted

    mck             *shardmaster.Clerk

    db              map[string]string   // persistent

    session         Session             // persistent

    replyMapMtx     sync.Mutex

    replyMap        map[Info]chan chan OpReply

    opCh            chan Op
}


func (kv *ShardKV) Get (args *GetArgs, reply *GetReply) {
    opRequest := &Op{Type: ReqGet,
                     ArgsGet: *args}
    opReply := kv.HandleRequest(opRequest)
    reply.Fill(&opReply)
}

func (kv *ShardKV) PutAppend (args *PutAppendArgs, reply *PutAppendReply) {
    opRequest := &Op{Type: ReqPutAppend,
                     ArgsPutAppend: *args}
    opReply := kv.HandleRequest(opRequest)
    reply.Fill(&opReply)
}

func (kv *ShardKV) MoveShards (args *MoveShardsArgs, reply *MoveShardsReply) {
    opRequest := &Op{Type: ReqMoveShard,
                     ArgsMoveShards: *args}
    opReply := kv.HandleRequest(opRequest)
    reply.Fill(&opReply)
}

func (kv *ShardKV) RegisterReplyCh (info Info, replyCh chan chan OpReply) {
    kv.replyMapMtx.Lock()
    defer kv.replyMapMtx.Unlock()

    _, ok := kv.replyMap[info]
    if ok {
        panic("already has uuid in map!")
    }
    //kv.Log("reg: %d:%d %v\n", info.Clerk, info.ID, replyCh)
    kv.replyMap[info] = replyCh
}

func (kv *ShardKV) UnregisterReplyCh (info Info) {
    kv.replyMapMtx.Lock()
    defer kv.replyMapMtx.Unlock()

    replyCh, ok := kv.replyMap[info]
    if ok {
        close(replyCh)
        //kv.Log("unreg: %d:%d %v\n", info.Clerk, info.ID, replyCh)
        delete(kv.replyMap, info)
    }
}

func (kv *ShardKV) UnregisterNotUsedReplyCh (info Info) bool {
    kv.replyMapMtx.Lock()
    defer kv.replyMapMtx.Unlock()

    replyCh, ok := kv.replyMap[info]
    if !ok {
        panic("no uuid in map!")
    }

    if len(replyCh) > 0 {
        close(replyCh)
        kv.Log("unreg: %d:%d %v\n", info.Clerk, info.ID, replyCh)
        delete(kv.replyMap, info)
        return true
    } else {
        return false
    }
}

func (kv *ShardKV) GetReplyCh (info Info) (chan chan OpReply, bool) {
    kv.replyMapMtx.Lock()
    defer kv.replyMapMtx.Unlock()

    ch, ok := kv.replyMap[info]
    return ch, ok
}

func (kv *ShardKV) HandleRequest (request *Op) OpReply {

    // optimize for get/put/append
    //key := request.GetKey()
    //shard := key2shard(key)
    //kv.configMtx.Lock()
    //gid := kv.config.Shards[shard]
    //kv.configMtx.Unlock()
    //if gid != kv.gid {
    //    kv.Log("gid:%v, gid:%d\n", gid, kv.gid)
    //    return OpReply{Type: request.Type,
    //                   Err: ErrWrongGroup}
    //}

    reqType := request.Type
    info, err := request.GetInfo()
    if err != nil {
        panic("should have no err")
    }

    replyCh := make(chan chan OpReply, 1)
    kv.RegisterReplyCh(info, replyCh)
    defer kv.UnregisterReplyCh(info)

    _, _, isLeader := kv.rf.Start(*request)
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
                kv.Log("HandleRequest: %s timeout!\n", reqType)
                timeoutCnt += 1
                if timeoutCnt > 1 {
                    panic("too much timeout!")
                }

                if kv.UnregisterNotUsedReplyCh(info) {
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

func (kv *ShardKV) CheckWrongGroup (key string) bool {
    shardId := key2shard(key)

    kv.configMtx.Lock()
    curConfig := kv.config
    kv.configMtx.Unlock()

    // curConfig is not support shardId
    if curConfig.Shards[shardId] != kv.gid {
       return true
    } else {
        // not recv shards yet
        kv.configMtx.Lock()
        defer kv.configMtx.Unlock()
        if kv.shardToRecv.IsReceived(shardId) == false {
            return true
        } else {
            return false
        }
    }
}

func (kv *ShardKV) ApplyGet (args *GetArgs) OpReply {
    if kv.CheckWrongGroup(args.Key) {
        return OpReply{Type: ReqGet,
                       Err: ErrWrongGroup}
    }

    if v, ok := kv.db[args.Key]; ok {
        return OpReply{Type: ReqGet,
                       WrongLeader: false,
                       Err: OK,
                       Value: v}
    } else {
        return OpReply{Type: ReqGet,
                       WrongLeader: false,
                       Err: ErrNoKey}
    }
}

func (kv *ShardKV) ApplyPutAppend (args *PutAppendArgs) OpReply {
    key := args.Key
    value := args.Value

    if kv.CheckWrongGroup(args.Key) {
        return OpReply{ Type: ReqPutAppend,
                        Err: ErrWrongGroup}
    }

    if args.Op == "Put" {
        kv.db[key] = value
        return OpReply{Type: ReqPutAppend,
                       WrongLeader: false,
                       Err: OK,
                       Value: value}
    } else if args.Op == "Append" {
        if v, ok := kv.db[key]; ok {
            kv.db[key] = v + value
        } else {
            kv.db[key] = value
        }
        return OpReply{Type: ReqPutAppend,
                       WrongLeader: false,
                       Err: OK,
                       Value: value}
    } else {
        panic("wrong args!")
    }
    return OpReply{}
}

// append shard to out map
func (kv *ShardKV) GetShard (shardId int) Shard {
    // need't lock kv.db
    out := map[string]string{}
    for key, value := range kv.db {     // TODO: optimize
        if key2shard(key) == shardId {
            if _, ok := out[key]; !ok {
                out[key] = value
            } else {
                panic("shouldn't exist key")
            }
        }
    }
    return Shard{shardId, out}
}

func (kv *ShardKV) SendShards (curConfig, newConfig shardmaster.Config) {
    curShards := curConfig.Shards
    newShards := newConfig.Shards

    // to_gid -> shards
    shardMap := map[int][]Shard{}
    for id, gid := range curShards {
        to := newShards[id]
        if gid == kv.gid && to != gid && to != 0 {
            shardMap[to] = append(shardMap[to], kv.GetShard(id))
        }
    }

    for gid, shards := range shardMap {

        go func (to int, _shards []Shard, config shardmaster.Config) {

            info := Info{Clerk: nrand(), ID: nrand()}   // FIXME: use unique id
            args := MoveShardsArgs{Info: info, From: kv.gid, To: to, ConfigNum: config.Num ,Shards: _shards}
            serverNames := config.Groups[to]

            count := 20
            i := 0
            numServerName := len(serverNames)
            for {
                name := serverNames[i]
                srv := kv.make_end(name)
                var reply MoveShardsReply
                ok := srv.Call("ShardKV.MoveShards", &args, &reply)
                if ok && reply.WrongLeader == false {
                    if reply.Err == OK {
                        return
                    } else if reply.Err == ErrRetryConfig {
                        // keep i unchanged
                        time.Sleep(100 * time.Millisecond)
                    } else {
                        i = (i+1)%numServerName
                    }
                } else {
                    i = (i+1)%numServerName
                }
                count -= 1
                if count < 0 {
                    panic("shard cannot be sent")
                }
            }

        } (gid, shards, newConfig)

    }
}

func (kv *ShardKV) ApplyCfgChange (args *CfgChangeArgs) OpReply {
    //kv.Log("recv %v\n", *args)
    curConfig := kv.config
    newConfig := args.Config

    kv.configMtx.Lock()
    if newConfig.Num == kv.config.Num+1 {
        kv.shardToRecv.Update(&curConfig, &newConfig)
        kv.config = newConfig
    }
    kv.configMtx.Unlock()

    if _, isLeader := kv.rf.GetState(); isLeader {
        kv.SendShards(curConfig, newConfig)
    }

    return OpReply{Type: ReqCfgChange,
                   Err: OK}
}

func (kv *ShardKV) ApplyMoveShards (args *MoveShardsArgs) OpReply {
    //kv.Log("recv %v\n", *args)
    kv.configMtx.Lock()
    curConfig := kv.config
    kv.configMtx.Unlock()

    if args.ConfigNum > curConfig.Num {
        return OpReply{ Type: ReqMoveShard,
                        Err: ErrRetryConfig}
    }

    if args.To == kv.gid {
       //info := args.Info
       //from := args.From
       shards := args.Shards

       // TODO: detect duplication
       for _, shard := range shards {

           // add to db
           id := shard.Id
           db := shard.Data
           for key, value := range db {
               kv.db[key] = value
           }

           // remove from shardToRecv
           kv.configMtx.Lock()
           kv.shardToRecv.Remove(id)
           kv.configMtx.Unlock()
       }
    } else {
        panic("recv wrong move cmd!")
    }

    return OpReply{ Type: ReqMoveShard,
                    WrongLeader: false,
                    Err: OK}
}

func (kv *ShardKV) ApplyOp (op *Op) OpReply {

    info, err := op.GetInfo()
    if err == nil {
        if op.Type != ReqGet {
            if kv.session.LastApplied[info.Clerk] == info.ID {
                kv.Log("skip duplicated operation: %v\n", *op)
                return OpReply{Type: op.Type,
                    Err: OK}
            }
        }
    }

    var ret OpReply

    switch op.Type {
    case ReqGet:
        ret = kv.ApplyGet(&op.ArgsGet)
    case ReqPutAppend:
        ret = kv.ApplyPutAppend(&op.ArgsPutAppend)
    case ReqCfgChange:
        ret = kv.ApplyCfgChange(&op.ArgsCfgChange)
    case ReqMoveShard:
        ret = kv.ApplyMoveShards(&op.ArgsMoveShards)
    default:
        panic(0)
    }

    kv.Log("apply: %v ==> %v\n", *op, ret)

    return ret
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// if shards are not received completely, dont update new config
func (kv *ShardKV) TryUpdateConfig () {

    kv.configMtx.Lock()
    curConfig := kv.config
    kv.configMtx.Unlock()

    newConfig := kv.mck.Query(curConfig.Num+1)
    if newConfig.Num == curConfig.Num+1 {
        if _, isLeader := kv.rf.GetState(); isLeader {
            recvFinished := false
            kv.configMtx.Lock()
            recvFinished = kv.shardToRecv.Empty() == true
            kv.configMtx.Unlock()

            if recvFinished {
                request := CfgChangeArgs{newConfig}
                kv.rf.Start(Op{Type: ReqCfgChange, ArgsCfgChange: request})
            }
        }
    }
}

func (kv *ShardKV) PollingShardConfig () {
    for {
        ticker := time.NewTicker(time.Duration(100) * time.Millisecond)
        select {
        case <-ticker.C:
            kv.TryUpdateConfig()
        }
    }
}

func (kv *ShardKV) PollingApplyCh () {

    for {
        ticker := time.NewTicker(time.Duration(1) * time.Second)
        select {

        case applyMsg := <-kv.applyCh:

            if op, valid := applyMsg.Command.(Op); valid {

                opReply := kv.ApplyOp(&op)
                info, err := op.GetInfo()
                if err == nil {
                    replyCh, ok := kv.GetReplyCh(info)
                    if ok {
                        ch := <-replyCh
                    Loop:
                        for {
                            ticker := time.NewTicker(time.Duration(1) * time.Second)
                            select {
                            case ch <- opReply:
                                break Loop
                            case <-ticker.C:
                                kv.Log("stuck in apply loop on info:%v\n", info)
                            }
                        }
                    }

                    if opReply.Err == OK && op.Type != ReqGet {
                        kv.session.LastApplied[info.Clerk] = info.ID
                    }
                }

            } else {
                panic("invalid applyMsg!")
            }

        case <-ticker.C:
            //kv.Log("No request timeout!\n")
        }
    }
}

func (kv *ShardKV) SaveSnapshot (lastIndex int) {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)

    e.Encode(kv.config)
    e.Encode(kv.shardToRecv)
    e.Encode(kv.db)
    e.Encode(kv.session)

    data := w.Bytes()

    kv.Log("savesnapshot:%d, size:%d\n",
        lastIndex, len(data))

    kv.rf.SaveSnapshot(data, lastIndex)
}

func (kv *ShardKV) ParseSnapshot (data []byte) {

    if data == nil || len(data) < 1 {
        kv.Log("ParseSnapshot nothing!\n")
        return
    }

    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)

    d.Decode(&kv.session)
    d.Decode(&kv.db)
    d.Decode(&kv.shardToRecv)
    d.Decode(&kv.config)
}

func (kv *ShardKV) LoadSnapshot () {
    if ok, data := kv.rf.LoadSnapshot(); ok {
        kv.ParseSnapshot(data)
    }
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
    // call labgob.Register on structures you want
    // Go's RPC library to marshall/unmarshall.
    labgob.Register(Op{})

    kv := new(ShardKV)
    kv.me = me
    kv.maxraftstate = maxraftstate
    kv.make_end = make_end
    kv.gid = gid
    kv.masters = masters

    // Your initialization code here.
    kv.db = make(map[string]string)
    kv.session = Session{LastApplied: make(map[int64]int64)}
    kv.replyMap = make(map[Info]chan chan OpReply)
    kv.shardToRecv = ShardToRecv{Gid: kv.gid, ConfigNum:-1, Unfinished:make(map[int]int)}
    kv.opCh = make(chan Op)

    // Use something like this to talk to the shardmaster:
    kv.config = shardmaster.Config{Num:-1}
    kv.mck = shardmaster.MakeClerk(kv.masters)

    kv.applyCh = make(chan raft.ApplyMsg)
    kv.rf = raft.Make(servers, me, persister, kv.applyCh)

    go kv.PollingApplyCh()
    go kv.PollingShardConfig()

    return kv
}
