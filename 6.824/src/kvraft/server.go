package raftkv

import (
    "labgob"
    "labrpc"
    "bytes"
    "time"
    "fmt"
    "raft"
    "sync"
    "sync/atomic"
)

const Debug = true

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
    Uuid        int32
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
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

        // Your definitions here.
        db              map[string]string
        lastApplied     map[int64]int64     // clerk -> args.ID

        uuid    int32

        opReplyMapMtx   sync.Mutex
        opReplyMap      map[int32]chan OpReply

        getCh           chan GetRequest
        putAppendCh     chan PutAppendRequest
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    // Your code here.
    request := GetRequest{Args: args,
                          ReplyCh: make(chan GetReply)}
    kv.getCh <- request
    resp := <-request.ReplyCh

    kv.Log("wrongLeader: %v\n", reply.WrongLeader)

    *reply = resp
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    // Your code here.
    request := PutAppendRequest{Args: args,
                                ReplyCh: make(chan PutAppendReply)}
    kv.putAppendCh <- request
    resp := <-request.ReplyCh

    kv.Log("value: %v, wrongLeader: %v\n", args.Value, reply.WrongLeader)

    *reply = resp
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


func (kv *KVServer) RegisterOpReplyCh (uuid int32, opReplyCh chan OpReply) {
    kv.opReplyMapMtx.Lock()
    _, ok := kv.opReplyMap[uuid]
    if ok {
        //kv.Log("uuid has registerd: %d\n", uuid)
        panic("already has uuid in map!")
    }
    kv.Log("reg: %d %v\n", uuid, opReplyCh)
    kv.opReplyMap[uuid] = opReplyCh
    kv.opReplyMapMtx.Unlock()
}


func (kv *KVServer) UnregisterOpReplyCh (uuid int32) {
    kv.opReplyMapMtx.Lock()
    opReplyCh, ok := kv.opReplyMap[uuid]
    close(opReplyCh)
    if !ok {
        panic("no uuid in map!")
    }
    kv.Log("unreg: %d %v\n", uuid, opReplyCh)
    delete(kv.opReplyMap, uuid)
    _, ok2 := kv.opReplyMap[uuid]
    if ok2 {
        panic("uuid in map not deleted!")
    }
    kv.opReplyMapMtx.Unlock()
}


func (kv *KVServer) GetOpReplyCh (uuid int32) (chan OpReply, bool) {
    kv.opReplyMapMtx.Lock()
    ch, ok := kv.opReplyMap[uuid]
    kv.opReplyMapMtx.Unlock()
    return ch, ok
}


func (kv *KVServer) GetInLoop (request *GetRequest) {
    args := request.Args
    replyCh := request.ReplyCh

    uuid := atomic.AddInt32(&kv.uuid, 1)

    op := Op{ID: args.ID,
             Clerk: args.Clerk,
             Uuid: uuid,
             Type: C_Get,
             Key: args.Key}

    index, _, isLeader := kv.rf.Start(op)

    // NOTE: applyMsg logic and GetInLoop should in the same for-loop
    if isLeader {
        kv.RegisterOpReplyCh(uuid, make(chan OpReply))
    }

    go func () {
        replySucc := false
        if isLeader {
            ticker := time.NewTicker(time.Duration(1) * time.Second)
            opReplyCh, ok := kv.GetOpReplyCh(uuid)
            if !ok {
                panic("chan is not registered!")
            }
            kv.Log("wait on %d chan %v\n", uuid, opReplyCh)
            select {
            case opReply := <-opReplyCh:
                replyCh <- GetReply{WrongLeader: false,
                                    Err: opReply.Err,
                                    Index: index,
                                    Value: opReply.Value}
                replySucc = true

            case <-ticker.C:
                kv.Log("GetInLoop timeout! on chan %v\n", opReplyCh)
                replyCh <- GetReply{WrongLeader: true}
                <-opReplyCh    // FIXME
            }
            kv.UnregisterOpReplyCh(uuid)
            kv.Log("wait on %d chan %v quit... leader:%v, succ: %v\n",
                    uuid, opReplyCh, isLeader, replySucc)
        } else {
            replyCh <- GetReply{WrongLeader: true}
        }
    }()
}


func (kv *KVServer) PutAppendInLoop (request *PutAppendRequest) {
    args := request.Args
    replyCh := request.ReplyCh

    uuid := atomic.AddInt32(&kv.uuid, 1)

    op := Op{ID: args.ID,
             Clerk: args.Clerk,
             Uuid: uuid,
             Type: args.Op,
             Key: args.Key,
             Value: args.Value}

    index, _, isLeader := kv.rf.Start(op)

    // NOTE: applyMsg logic and GetInLoop should in the same for-loop
    if isLeader {
        kv.RegisterOpReplyCh(uuid, make(chan OpReply))
    }

    go func () {
        replySucc := false
        if isLeader {
            ticker := time.NewTicker(time.Duration(1) * time.Second)
            opReplyCh, ok := kv.GetOpReplyCh(uuid)
            if !ok {
                panic("chan is not registered!")
            }
            kv.Log("wait on %d chan %v\n", uuid, opReplyCh)
            select {
            case opReply := <-opReplyCh:
                replyCh <- PutAppendReply{WrongLeader: false,
                                          Err: opReply.Err,
                                          Index: index}
                replySucc = true

            case <-ticker.C:
                kv.Log("PutAppendInLoop timeout! on %d chan %v\n", uuid, opReplyCh)
                replyCh <- PutAppendReply{WrongLeader: true}
                <-opReplyCh    // FIXME
            }
            kv.UnregisterOpReplyCh(uuid)
            kv.Log("wait on %d chan %v quit... leader:%v, succ: %v\n",
                    uuid, opReplyCh, isLeader, replySucc)
        } else {
            replyCh <- PutAppendReply{WrongLeader: true}
        }
    }()
}


func (kv *KVServer) ApplyOp (op Op) OpReply {
    clerk := op.Clerk
    id := op.ID

    // duplicated request
    // TODO: add Session for client
    if kv.lastApplied[clerk] == id {
        kv.Log("skip duplicated operation: %v\n", op)
        if op.Type != C_Get {
            return OpReply{Err: OK,
                           Value: kv.db[op.Key]}
        }
    }
    //else {
    //    kv.lastApplied[clerk] = id
    //}

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

    obj := KVPersistence{Db: kv.db}
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
        kv.Log("LoadSnapshot Get 0 : %v\n", kv.db["0"])
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
    kv.uuid = 0
    kv.db = make(map[string]string)
    kv.lastApplied = make(map[int64]int64)
    kv.opReplyMap = make(map[int32]chan OpReply)
    kv.applyCh = make(chan raft.ApplyMsg)

    kv.rf = raft.Make(servers, me, persister, kv.applyCh)

    // You may need initialization code here.
    kv.LoadSnapshot()

    go func () {
        for {
            ticker := time.NewTicker(time.Duration(3) * time.Second)
            select {
            case getRequest := <-kv.getCh:
                kv.GetInLoop(&getRequest)

            case putAppendRequest := <-kv.putAppendCh:
                kv.PutAppendInLoop(&putAppendRequest)

            case applyMsg := <-kv.applyCh:

                if op, valid := applyMsg.Command.(Op); valid {

                    opReply := kv.ApplyOp(op)
                    uuid := op.Uuid

                    kv.SaveSnapshot()

                    ch, ok := kv.GetOpReplyCh(uuid)

                    // NOTE:
                    // 1. ch should not exist if request is sent to non-leader
                    // 2. op can be duplicated
                    if ok && kv.lastApplied[op.Clerk] != op.ID {
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

                    // TODO
                    kv.lastApplied[op.Clerk] = op.ID

                } else {
                    panic("invalid applyMsg!")
                }

            case <-ticker.C:
                //kv.Log("kvserver: No request timeout!\n")
            }
        }
    }()

    //go func () {
    //    for {
    //        select {
    //        case applyMsg := <-kv.applyCh:

    //            if op, valid := applyMsg.Command.(Op); valid {

    //                opReply := kv.ApplyOp(op)
    //                uuid := op.Uuid

    //                kv.SaveSnapshot()

    //                ch, ok := kv.GetOpReplyCh(uuid)

    //                // NOTE:
    //                // 1. ch should not exist if request is sent to non-leader
    //                // 2. op can be duplicated

    //                if ok && kv.lastApplied[op.Clerk] != op.ID {
    //                    Loop:
    //                    for {
    //                        ticker := time.NewTicker(time.Duration(1) * time.Second)
    //                        kv.Log("send to %d chan %v\n", uuid, ch)
    //                        select {
    //                        case ch <- opReply:
    //                            kv.Log("send to %d chan %v succ\n", uuid, ch)
    //                            break Loop
    //                        case <-ticker.C:
    //                            kv.Log("stuck in apply loop on uuid %d chan %v, op %v !!!\n",
    //                                    uuid, ch, op)
    //                        }
    //                    }
    //                }

    //            } else {
    //                panic("invalid applyMsg!")
    //            }
    //        }
    //    }
    //}()

    return kv
}
