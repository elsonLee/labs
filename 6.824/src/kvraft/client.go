package raftkv

import "fmt"
import "labrpc"
import "crypto/rand"
import "math/big"

var DebugOn bool = false

type Clerk struct {
    servers     []*labrpc.ClientEnd
    // You will have to modify this struct.
    me             int64
    leaderHint     int
}

func nrand() int64 {
    max := big.NewInt(int64(1) << 62)
    bigx, _ := rand.Int(rand.Reader, max)
    x := bigx.Int64()
    return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
    ck := new(Clerk)
    ck.servers = servers
    // You'll have to add code here.
    ck.me = nrand()
    ck.leaderHint = 0

    return ck
}

func (ck *Clerk) Log (format string, a ...interface{}) {
    if DebugOn {
        fmt.Printf("[clerk] %s",
                    fmt.Sprintf(format, a...))
    }
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

    // You will have to modify this function.
    args := GetArgs{ID: nrand(),
                    Clerk: ck.me,
                    Key: key}

    //ck.Log("==> Get %v id:%v\n", key, args.ID)

    hasLeader := false
    oldleaderHint := ck.leaderHint
    for {
        i := ck.leaderHint
        reply := GetReply{}

        ck.Log("==> Get %v, id:%v, server%d\n",
                key, args.ID, i)
        ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
        if ok {
            if reply.WrongLeader == false {
                hasLeader = true
                if reply.Err == OK {
                    ck.Log("<== Get %v, id:%v, index:%d, {%v}\n",
                                key, args.ID, reply.Index, reply.Value)
                    return reply.Value
                } else {
                    ck.Log("<== Get %v, id:%v, err:%v\n", key, args.ID, reply.Err)
                    return ""
                }
            } else {
                ck.Log("<== Get %v, id:%v, not leader\n", key, args.ID)
            }
        } else {
            ck.Log("<== Get %v, id:%v, RPC timeout\n", key, args.ID)
        }
        ck.leaderHint = (ck.leaderHint + 1) % len(ck.servers)

        if ck.leaderHint == oldleaderHint && !hasLeader {
            ck.Log("Get %v id:%v: no leader!\n", key, args.ID)
        }
    }

    return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
    // You will have to modify this function.
    args := PutAppendArgs{ID: nrand(),
                          Clerk: ck.me,
                          Key: key,
                          Value: value,
                          Op: op}
    hasLeader := false
    oldleaderHint := ck.leaderHint
    for {
        i := ck.leaderHint
        reply := PutAppendReply{}

        ck.Log("==> %s %v, id:%v, server%d\n",
                op, key, args.ID, i)
        ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
        if ok {
            if reply.WrongLeader == false {
                hasLeader = true
                if reply.Err == OK {
                    ck.Log("<== %s %v, id:%v, value:%v, index:%d\n",
                            op, key, args.ID, args.Value, reply.Index)
                } else {
                    ck.Log("<== %s %v, id:%v, value:%v, err:%v\n",
                            op, key, args.ID, args.Value, reply.Err)
                }
                return
            } else {
                ck.Log("<== %s %v, id:%v, value:%v, not leader\n",
                        op, key, args.ID, args.Value)
            }
        } else {
            ck.Log("<== %s %v, id:%v, value:%v, RPC timeout\n",
                    op, key, args.ID, args.Value)
        }
        ck.leaderHint = (ck.leaderHint + 1) % len(ck.servers)

        if ck.leaderHint == oldleaderHint && !hasLeader {
            ck.Log("%s %v %v id:%v: no leader!\n", op, key, value, args.ID)
        }
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
