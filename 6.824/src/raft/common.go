package raft

import "sync/atomic"

const (
    None            int = -1    // for votedFor
)

type Status int
const (
    Candidate       Status = 0
    Leader          Status = 1
    Follower        Status = 2
    Invalid         Status = 3
)


func StatusName (s Status) string {
    switch s {
    case Candidate: return "cand"
    case Leader :   return "lead"
    case Follower : return "foll"
    default:        return "invalid"
    }
}


func AbbrStatusName (s Status) string {
    switch s {
    case Candidate: return "C"
    case Leader:    return "L"
    case Follower:  return "F"
    default:        return "I"
    }
}


type MsgType string
const (
    MsgAE           MsgType = "AE"  // appendEntries
    MsgHB           MsgType = "HB"  // heartbeat
    MsgSaveSnapshot MsgType = "SaveSnapshot"
    MsgLoadSnapshot MsgType = "LoadSnapshot"
)

type CmdType string
const (
    CmdLoad         CmdType = "Load"
    CmdSave         CmdType = "Save"
)


type SnapshotCmdRequest struct {
    Type            CmdType
    LastIndex       int
    Snapshot        []byte
    ReplyCh         chan SnapshotCmdReply
}


type SnapshotCmdReply struct {
    Succ            bool
    Snapshot        []byte
}


type InstallSnapshotArgs struct {
    Term            int
    LeaderId        int
    LastIndex       int
    LastTerm        int
    Data            []byte  // only use one RPC request for snapshot
}

type InstallSnapshotReply struct {
    Term            int
}

type InstallSnapshotWrapper struct {
    Args            *InstallSnapshotArgs
    Reply           *InstallSnapshotReply
    Done            chan bool
}


type CommandReply struct {
    Term            int         // command term
    Index           int         // command position
    IsLeader        bool
}


type CommandRequest struct {
    IsNoOp          bool
    Command         interface{}
    ReplyCh         chan CommandReply
}


// RequestVote
type RequestVoteArgs struct {
    Term            int
    CandidateId     int
    LastLogIndex    int
    LastLogTerm     int
}


type RequestVoteReply struct {
    Term            int
    VoteGranted     bool
}


type RequestVoteWrapper struct {
    Args            *RequestVoteArgs
    Reply           *RequestVoteReply
    Done            chan bool
}


// AppendEntries
type AppendEntriesArgs struct {
    Uuid            int32
    Type            MsgType
    Term            int
    LeaderId        int
    PrevLogIndex    int
    PrevLogTerm     int
    Entries         []LogEntry
    LeaderCommit    int
}


type AppendEntriesReply struct {
    //Term          int     // Term is useless
    MatchIndex      int     // MatchIndex return the index that data has been replicated
    NextIndexHint   int     // quick conflict detection result
    Success         bool
}


type AppendEntriesReplyWrapper struct {
    Server          int
    Index           int
    Reply           AppendEntriesReply
}


type AppendEntriesWrapper struct {
    Args            *AppendEntriesArgs
    Reply           *AppendEntriesReply
    Done            chan bool
}


// functions
func Min (x, y int) int {
    if x < y {
        return x
    } else {
        return y
    }
}


func Max (x, y int) int {
    if x > y {
        return x
    } else {
        return y
    }
}


// Atomic
type AtomicBool struct { val int32 }

func (a *AtomicBool) Set (val bool) {
    var ival int32 = 0
    if val { ival = 1 }
    atomic.StoreInt32(&a.val, ival)
}


func (a *AtomicBool) Get () bool {
    if atomic.LoadInt32(&a.val) == 1 {
        return true
    } else {
        return false
    }
}
