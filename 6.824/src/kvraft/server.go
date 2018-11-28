package raftkv

import (
    "labgob"
    "labrpc"
    "bytes"
    "time"
    "fmt"
    "raft"
    "sync"
    //"sync/atomic"
)

const Debug = false

func (kv *KVServer) Log (format string, a ...interface{}) (n int, err error) {
    if Debug {
        fmt.Printf("[server%d] %s", kv.me,
            fmt.Sprintf(format, a...))
    }
    return
}

const (
    C_Put       string = "Put"
    C_Append    string = "Append"
    C_Get       string = "Get"
)

type Op struct {
    // Your definitions here.
    // Field names must start with capital letters,
    // otherwise RPC will break.
    ID          int64
    Clerk       int64
    Type        string
    Key         string
    Value       string
}

type OpReply struct {
    Err         Err
    Value       string
}

type KVPersistence struct {
    Db          map[string]string
    Session     Session
}

type OpReplyWrapper struct {
    IsLeader    bool
    OpReplyCh   chan OpReply
}

type Session struct {
    LastApplied     map[int64]int64     // clerk -> args.ID
}

type KVServer struct {
    mu              sync.Mutex

    me              int

    rf              *raft.Raft

    applyCh         chan raft.ApplyMsg

    maxraftstate    int // snapshot if log grows this big

    // Your definitions here.
    db              map[string]string

    session         Session

    opReplyMapMtx   sync.Mutex
    opReplyMap      map[int64]chan OpReplyWrapper

    getCh           chan GetRequest
    putAppendCh     chan PutAppendRequest
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    // Your code here.
    request := GetRequest{Args: args,
                          ReplyCh: make(chan GetReply)}
    *reply = kv.GetInLoop(&request)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    // Your code here.
    request := PutAppendRequest{Args: args,
                                ReplyCh: make(chan PutAppendReply)}
    *reply = kv.PutAppendInLoop(&request)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


func (kv *KVServer) RegisterOpReplyWrapperCh (uuid int64, opReplyWrapperCh chan OpReplyWrapper) {
    kv.opReplyMapMtx.Lock()
    defer kv.opReplyMapMtx.Unlock()

    _, ok := kv.opReplyMap[uuid]
    if ok {
        panic("already has uuid in map!")
    }
    kv.Log("reg: %d %v\n", uuid, opReplyWrapperCh)
    kv.opReplyMap[uuid] = opReplyWrapperCh
}


func (kv *KVServer) UnregisterOpReplyWrapperCh (uuid int64) {
    kv.opReplyMapMtx.Lock()
    defer kv.opReplyMapMtx.Unlock()

    opReplyWrapperCh, ok := kv.opReplyMap[uuid]
    if ok {
        close(opReplyWrapperCh)
        kv.Log("unreg: %d %v\n", uuid, opReplyWrapperCh)
        delete(kv.opReplyMap, uuid)
    }
}


func (kv *KVServer) UnregisterNotUsedOpReplyWrapperCh (uuid int64) bool {
    kv.opReplyMapMtx.Lock()
    defer kv.opReplyMapMtx.Unlock()

    opReplyWrapperCh, ok := kv.opReplyMap[uuid]
    if !ok {
        panic("no uuid in map!")
    }

    if len(opReplyWrapperCh) > 0 {
        kv.Log("unreg not used: %d %v\n", uuid, opReplyWrapperCh)
        delete(kv.opReplyMap, uuid)
        return true
    } else {
        return false
    }
}


func (kv *KVServer) GetOpReplyWrapperCh (uuid int64) (chan OpReplyWrapper, bool) {
    kv.opReplyMapMtx.Lock()
    defer kv.opReplyMapMtx.Unlock()

    ch, ok := kv.opReplyMap[uuid]
    return ch, ok
}


func (kv *KVServer) GetInLoop (request *GetRequest) GetReply {
    args := request.Args

    uuid := args.ID
    opReplyWrapperCh := make(chan OpReplyWrapper, 1)
    kv.RegisterOpReplyWrapperCh(uuid, opReplyWrapperCh)
    defer kv.UnregisterOpReplyWrapperCh(uuid)

    op := Op{ID: args.ID,
             Clerk: args.Clerk,
             Type: C_Get,
             Key: args.Key}

    index, _, isLeader := kv.rf.Start(op)

    opReplyCh := make(chan OpReply)
    if isLeader {
        // NOTE: opReplyWrapperCh is buffering to avoid blocking here
        opReplyWrapperCh <- OpReplyWrapper{IsLeader: true,
                                           OpReplyCh: opReplyCh}
    }

    kv.Log("GetInLoop Start return %d %v isLeader:%v\n",
            uuid, opReplyCh, isLeader)

    replySucc := false
    if isLeader {
        kv.Log("wait on %v chan %v\n", uuid, opReplyCh)
        timeoutCnt := 0
        for {
            ticker := time.NewTicker(time.Duration(1) * time.Second)
            select {
            case opReply := <-opReplyCh:
                return GetReply{WrongLeader: false,
                                Err: opReply.Err,
                                Index: index,
                                Value: opReply.Value}
                replySucc = true

            case <-ticker.C:
                kv.Log("GetInLoop timeout! on chan %v\n", opReplyCh)
                timeoutCnt += 1
                if timeoutCnt > 1 {
                    panic("too much timout!")
                }

                if kv.UnregisterNotUsedOpReplyWrapperCh(uuid) {
                    return GetReply{WrongLeader: true}  // FIXME: maybe wrongleader is false
                }
            }
            kv.Log("wait on %v chan %v quit... leader:%v, succ: %v\n",
                    uuid, opReplyCh, isLeader, replySucc)
        }
    } else {
        return GetReply{WrongLeader: true}
    }

    return GetReply{}
}

func (kv *KVServer) PutAppendInLoop (request *PutAppendRequest) PutAppendReply {
    args := request.Args

    uuid := args.ID
    opReplyWrapperCh := make(chan OpReplyWrapper, 1)
    kv.RegisterOpReplyWrapperCh(uuid, opReplyWrapperCh)
    defer kv.UnregisterOpReplyWrapperCh(uuid)

    op := Op{ID: args.ID,
             Clerk: args.Clerk,
             Type: args.Op,
             Key: args.Key,
             Value: args.Value}

    index, _, isLeader := kv.rf.Start(op)

    opReplyCh := make(chan OpReply)
    if isLeader {
        // NOTE: opReplyWrapperCh is buffering to avoid blocking here
        opReplyWrapperCh <- OpReplyWrapper{IsLeader: true,
                                           OpReplyCh: opReplyCh}
    }

    kv.Log("PutAppendInLoop Start return %v %v isLeader:%v\n",
            uuid, opReplyCh, isLeader)

    replySucc := false
    if isLeader {
        ticker := time.NewTicker(time.Duration(1) * time.Second)
        kv.Log("wait on %v chan %v\n", uuid, opReplyCh)
        timeoutCnt := 0
        for {
            select {
            case opReply := <-opReplyCh:
                return PutAppendReply{WrongLeader: false,
                                      Err: opReply.Err,
                                      Index: index}
                replySucc = true

            case <-ticker.C:
                kv.Log("PutAppendInLoop timeout! on %v chan %v\n", uuid, opReplyCh)
                timeoutCnt += 1
                if timeoutCnt > 1 {
                    panic("too much timeout!")
                }

                if kv.UnregisterNotUsedOpReplyWrapperCh(uuid) {
                    return PutAppendReply{WrongLeader: true}
                }
            }
        }
        kv.Log("wait on %v chan %v quit... leader:%v, succ: %v\n",
                uuid, opReplyCh, isLeader, replySucc)
    } else {
        return PutAppendReply{WrongLeader: true}
    }

    return PutAppendReply{}
}


func (kv *KVServer) ApplyOp (op Op) OpReply {
    clerk := op.Clerk
    id := op.ID

    // duplicated request
    // TODO: add Session for client
    if op.Type != C_Get {
        if kv.session.LastApplied[clerk] == id {
            kv.Log("skip duplicated operation: %v\n", op)
            return OpReply{Err: OK,
                           Value: kv.db[op.Key]}
        }
    }

    switch op.Type {
    case C_Put:
        kv.db[op.Key] = op.Value
        return OpReply{Err: OK,
                       Value: op.Value}

    case C_Append:
        v, ok := kv.db[op.Key]
        if ok {
            kv.db[op.Key] = v + op.Value
            //kv.Log("append to db {%v, %v}\n", op.Key, kv.db[op.Key])
        } else {
            kv.db[op.Key] = op.Value
        }
        return OpReply{Err: OK,
                       Value: kv.db[op.Key]}

    case C_Get:
        v, ok := kv.db[op.Key]
        if ok {
            return OpReply{Err: OK,
                           Value: v}
        } else {
            return OpReply{Err: ErrNoKey}
        }
    default:
        panic(0)
    }
}

func (kv *KVServer) SaveSnapshot () {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)

    obj := KVPersistence{Db: kv.db,
                         Session: kv.session}
    e.Encode(obj)

    data := w.Bytes()

    kv.rf.SaveSnapshot(data)
}

func (kv *KVServer) LoadSnapshot () {
    data := kv.rf.LoadSnapshot()

    if data == nil || len(data) < 1 {   // bootstrap without any state
        kv.Log("LoadSnapshot nothing!\n")
        return
    }

    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)

    p := KVPersistence{}
    if d.Decode(&p) != nil {
        panic("load Snapshot error!")
    } else {
        kv.db = p.Db
        kv.session = p.Session
        kv.Log("LoadSnapshot\n")
    }
}


//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
    // call labgob.Register on structures you want
    // Go's RPC library to marshall/unmarshall.
    labgob.Register(Op{})

    kv := new(KVServer)
    kv.me = me
    kv.maxraftstate = maxraftstate

    // You may need initialization code here.
    kv.getCh = make(chan GetRequest)
    kv.putAppendCh = make(chan PutAppendRequest)
    kv.db = make(map[string]string)
    kv.session = Session{LastApplied: make(map[int64]int64)}
    kv.opReplyMap = make(map[int64]chan OpReplyWrapper)
    kv.applyCh = make(chan raft.ApplyMsg)

    kv.rf = raft.Make(servers, me, persister, kv.applyCh)

    // You may need initialization code here.
    //kv.LoadSnapshot()

    go func () {
        for {
            ticker := time.NewTicker(time.Duration(1) * time.Second)
            select {

            case applyMsg := <-kv.applyCh:

                if op, valid := applyMsg.Command.(Op); valid {

                    opReply := kv.ApplyOp(op)
                    uuid := op.ID

                    //kv.SaveSnapshot()

                    opReplyWrapperCh, ok := kv.GetOpReplyWrapperCh(uuid)

                    // NOTE:
                    // 1. ch should not exist if request is sent to non-leader
                    // 2. op can be duplicated
                    if ok {
                        opReplyWrapper := <-opReplyWrapperCh
                        if opReplyWrapper.IsLeader {
                            ch := opReplyWrapper.OpReplyCh
                            Loop:
                            for {
                                ticker := time.NewTicker(time.Duration(1) * time.Second)
                                kv.Log("send to %d chan %v\n", uuid, ch)
                                select {
                                case ch <- opReply:
                                    kv.Log("send to %d chan %v succ\n", uuid, ch)
                                    break Loop
                                case <-ticker.C:
                                    kv.Log("stuck in apply loop on uuid %d chan %v, op %v !!!\n",
                                            uuid, ch, op)
                                }
                            }
                        }
                    }

                    // TODO
                    if op.Type != C_Get {
                        kv.session.LastApplied[op.Clerk] = op.ID
                    }

                } else {
                    panic("invalid applyMsg!")
                }

            case <-ticker.C:
                //kv.Log("kvserver: No request timeout!\n")
            }
        }
    }()

    return kv
}
