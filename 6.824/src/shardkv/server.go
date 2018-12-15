package shardkv


// import "shardmaster"
import "labrpc"
import "raft"
import "shardmaster"
import "sync"
import "labgob"
import "fmt"
import "time"

const Debug = true

func (kv *ShardKV) Log (format string, a ...interface{}) (n int, err error) {
    if Debug {
        fmt.Printf("[server%d] %s", kv.me,
            fmt.Sprintf(format, a...))
    }
    return
}

type Session struct {
    LastApplied     map[int64]int64
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

    config          shardmaster.Config

    mck             *shardmaster.Clerk

    db              map[string]string

    session         Session

    replyMapMtx     sync.Mutex

    replyMap        map[Info]chan chan OpReply

    opCh            chan Op
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
    // Your code here.
    opRequest := &Op{Type: ReqGet,
                     ArgsGet: *args}
    opReply := kv.HandleRequest(opRequest)
    reply.Fill(&opReply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    // Your code here.
    opRequest := &Op{Type: ReqPutAppend,
                     ArgsPutAppend: *args}
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

    key := request.GetKey()
    shard := key2shard(key)
    kv.configMtx.Lock()
    gid := kv.config.Shards[shard]
    kv.configMtx.Unlock()
    if gid != kv.gid {
        kv.Log("gid:%v, gid:%d\n", gid, kv.gid)
        return OpReply{Type: request.Type,
                       Err: ErrWrongGroup}
    }

    reqType := request.Type
    info := request.GetInfo()

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

func (kv *ShardKV) ApplyGet (args *GetArgs) OpReply {
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

func (kv *ShardKV) ApplyOp (op *Op) OpReply {

    kv.Log("apply: %v\n", *op)

    info := op.GetInfo()

    if op.Type != ReqGet {
        if kv.session.LastApplied[info.Clerk] == info.ID {
            kv.Log("skip duplicated operation: %v\n", *op)
            return OpReply{Type: op.Type,
                           Err: OK}
        }
    }

    var ret OpReply

    switch op.Type {
    case ReqGet:
        ret = kv.ApplyGet(&op.ArgsGet)
    case ReqPutAppend:
        ret = kv.ApplyPutAppend(&op.ArgsPutAppend)
    default:
        panic(0)
    }

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

func (kv *ShardKV) PollingShardConfig () {

    for {
        ticker := time.NewTicker(time.Duration(100) * time.Millisecond)
        select {
        case <-ticker.C:
            config := kv.mck.Query(-1)
            kv.configMtx.Lock()
            if config.Num != kv.config.Num {
                kv.config = config
            }
            kv.configMtx.Unlock()
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
                info := op.GetInfo()

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

                if op.Type != ReqGet {
                    kv.session.LastApplied[info.Clerk] = info.ID
                }

            } else {
                panic("invalid applyMsg!")
            }

        case <-ticker.C:
            //kv.Log("No request timeout!\n")
        }
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
