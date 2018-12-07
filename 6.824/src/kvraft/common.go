package raftkv

const (
    OK       = "OK"
    ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
    ID          int64
    Clerk       int64
    Key         string
    Value       string
    Op          string // "Put" or "Append"
    // You'll have to add definitions here.
    // Field names must start with capital letters,
    // otherwise RPC will break.
}

type PutAppendReply struct {
    WrongLeader bool
    Err         Err
    Index       int
}

type PutAppendRequest struct {
    Args        *PutAppendArgs
}

type GetArgs struct {
    ID          int64
    Clerk       int64
    Key         string
    // You'll have to add definitions here.
}

type GetReply struct {
    WrongLeader bool
    Err         Err
    Index       int
    Value       string
}

type GetRequest struct {
    Args        *GetArgs
}
