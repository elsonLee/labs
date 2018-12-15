package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
    OK            = "OK"
    ErrNoKey      = "ErrNoKey"
    ErrWrongGroup = "ErrWrongGroup"
)

type Err string

type ReqType string
const (
    ReqGet          ReqType = "get"
    ReqPutAppend    ReqType = "put_append"
)

type Op struct {
    Type            ReqType
    ArgsGet         GetArgs
    ArgsPutAppend   PutAppendArgs
}

type OpReply struct {
    Type            ReqType
    WrongLeader     bool
    Err             Err
    Value           string
}

type Info struct {
    Clerk       int64
    ID          int64
}

// Put or Append
type PutAppendArgs struct {
    // You'll have to add definitions here.
    Info        Info
    Key         string
    Value       string
    Op          string // "Put" or "Append"
    // You'll have to add definitions here.
    // Field names must start with capital letters,
    // otherwise RPC will break.
}

type GetArgs struct {
    Info        Info
    Key         string
    // You'll have to add definitions here.
}

type PutAppendReply struct {
    WrongLeader bool
    Err         Err
}

type GetReply struct {
    WrongLeader bool
    Err         Err
    Value       string
}

func (op *Op) GetInfo () Info {
    switch op.Type {
    case ReqGet:
        return op.ArgsGet.Info
    case ReqPutAppend:
        return op.ArgsPutAppend.Info
    default:
        panic(0)
        return Info{}
    }
}

func (gr *GetReply) Fill (opReply *OpReply) {
    if opReply.Type == ReqGet {
        gr.WrongLeader = opReply.WrongLeader
        gr.Err = opReply.Err
        gr.Value = opReply.Value
    }
}

func (pr *PutAppendReply) Fill (opReply *OpReply) {
    if opReply.Type == ReqPutAppend {
        pr.WrongLeader = opReply.WrongLeader
        pr.Err = opReply.Err
    }
}
