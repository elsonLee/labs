package shardkv

import (
    "fmt"
)

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
    ReqMoveShard    ReqType = "move_shards"
)

type Op struct {
    Type            ReqType
    ArgsGet         GetArgs
    ArgsPutAppend   PutAppendArgs
    ArgsMoveShards  MoveShardsArgs
}

func (op Op) String () string {
    switch op.Type {
    case ReqGet:
        return op.ArgsGet.String()
    case ReqPutAppend:
        return op.ArgsPutAppend.String()
    case ReqMoveShard:
        return op.ArgsMoveShards.String()
    default:
        panic(0)
    }
}

type OpReply struct {
    Type            ReqType
    WrongLeader     bool
    Err             Err
    Value           string
}

func (opReply OpReply) String () string {
    return fmt.Sprintf("isLeader:%v, err:%v, Value:%v",
        !opReply.WrongLeader, opReply.Err, opReply.Value)
}

type Info struct {
    Clerk       int64
    ID          int64
}

func (info Info) String () string {
    return fmt.Sprintf("ck:%d, id:%d", info.Clerk, info.ID)
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

func (arg PutAppendArgs) String () string {
    return fmt.Sprintf("[%s] key:%s, val:%s, info:%v", arg.Op, arg.Key, arg.Value, arg.Info)
}

type GetArgs struct {
    Info        Info
    Key         string
    // You'll have to add definitions here.
}

func (arg GetArgs) String () string {
    return fmt.Sprintf("[Get] key:%s, info:%v", arg.Key, arg.Info)
}

type MoveShardsArgs struct  {
    Info        Info
    From        int     // gid
    To          int     // gid
    Shards      map[string]string
}

func (arg MoveShardsArgs) String () string {
    return fmt.Sprintf("[Move] from:%d, to:%d, shards:%v", arg.From, arg.To, arg.Shards)
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

type MoveShardsReply struct {
    WrongLeader bool
    Err         Err
}

func (op *Op) GetKey () string {
    switch op.Type {
    case ReqGet:
        return op.ArgsGet.Key
    case ReqPutAppend:
        return op.ArgsPutAppend.Key
    default:
        panic(0)
        return ""
    }
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

func (ms *MoveShardsReply) Fill (opReply *OpReply) {
    if opReply.Type == ReqMoveShard {
        ms.WrongLeader = opReply.WrongLeader
        ms.Err = opReply.Err
    }
}
