package shardmaster

import "fmt"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

type GroupInfo struct {
    Gid		    int
    Shards      []int
}

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
    Num         int              // config number
    Shards      [NShards]int     // shard -> gid (group)
    Groups      map[int][]string // gid -> servers[]
}

func (config Config) String () string {
    return fmt.Sprintf("id:%d, shards:%v", config.Num, config.Shards)
}

func (conf *Config) GetUnusedGid () []int {
    gidSet := map[int]bool{}
    for _, gid := range conf.Shards {
        gidSet[gid] = true
    }
    unusedGids := []int{}
    for gid, _ := range conf.Groups {
        if _, ok := gidSet[gid]; !ok {
            unusedGids = append(unusedGids, gid)
        }
    }
    return unusedGids
}

const (
    OK = "OK"
    NoConfig = "No Config"
)

type Err string

type ReqType string
const (
    ReqJoin     ReqType = "join"
    ReqLeave    ReqType = "leave"
    ReqMove     ReqType = "move"
    ReqQuery    ReqType = "query"
)

type Op struct {
    Type        ReqType
    ArgsJoin    JoinArgs
    ArgsLeave   LeaveArgs
    ArgsMove    MoveArgs
    ArgsQuery   QueryArgs
}

type Info struct {
    Clerk       int64
    ID          int64
}

func (info Info) String () string {
    return fmt.Sprintf("ck:%d, id:%d", info.Clerk, info.ID)
}

type JoinArgs struct {
    Info        Info
    Servers     map[int][]string // new GID -> servers mappings
}

type LeaveArgs struct {
    Info        Info
    GIDs        []int
}

type MoveArgs struct {
    Info        Info
    Shard       int
    GID         int
}

type QueryArgs struct {
    Info        Info
    Num         int // desired config number
}

type OpReply struct {
    Type        ReqType
    WrongLeader bool
    Err         Err
    Config      Config
}

type JoinReply struct {
    WrongLeader bool
    Err         Err
}

type LeaveReply struct {
    WrongLeader bool
    Err         Err
}

type MoveReply struct {
    WrongLeader bool
    Err         Err
}

type QueryReply struct {
    WrongLeader bool
    Err         Err
    Config      Config
}

func (reply QueryReply) String () string {
    return fmt.Sprintf("isLeader:%v, config:%v", !reply.WrongLeader, reply.Config)
}

func (op *Op) GetInfo () Info {
    switch op.Type {
    case ReqJoin:
        return op.ArgsJoin.Info
    case ReqLeave:
        return op.ArgsLeave.Info
    case ReqMove:
        return op.ArgsMove.Info
    case ReqQuery:
        return op.ArgsQuery.Info
    default:
        panic(0)
        return Info{}
    }
}

func (jr *JoinReply) Fill (opReply *OpReply) {
    if opReply.Type == ReqJoin {
        jr.WrongLeader = opReply.WrongLeader
        jr.Err = opReply.Err
    }
}

func (lr *LeaveReply) Fill (opReply *OpReply) {
    if opReply.Type == ReqLeave {
        lr.WrongLeader = opReply.WrongLeader
        lr.Err = opReply.Err
    }
}

func (mr *MoveReply) Fill (opReply *OpReply) {
    if opReply.Type == ReqMove {
        mr.WrongLeader = opReply.WrongLeader
        mr.Err = opReply.Err
    }
}

func (qr *QueryReply) Fill (opReply *OpReply) {
    if opReply.Type == ReqQuery {
        qr.WrongLeader = opReply.WrongLeader
        qr.Err = opReply.Err
        qr.Config = opReply.Config
    }
}
