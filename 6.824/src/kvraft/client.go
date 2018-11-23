package raftkv

import "fmt"
import "labrpc"
import "crypto/rand"
import "math/big"


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
    if true {
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

    ck.Log("==> Get %v id:%v\n", key, args.ID)

    hasLeader := false
    oldleaderHint := ck.leaderHint
    for {
        i := ck.leaderHint
        reply := GetReply{}

        ck.Log(" => Get request server %d\n", i)
        ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
        if ok {
            if reply.WrongLeader == false {
                hasLeader = true
                if reply.Err == OK {
                    ck.Log("<== Get %v, index:%d, {%v}\n",
                                key, reply.Index, reply.Value)
                    return reply.Value
                } else {
                    ck.Log("<xx Get %v, err:%v\n", key, reply.Err)
                    return ""
                }
            }
        } else {
            ck.Log("<xx Get server %d, network timeout!\n", i)
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
    ck.Log("==> %s key:%v value:%v id:%v\n", op, key, value, args.ID)
    hasLeader := false
    oldleaderHint := ck.leaderHint
    for {
        i := ck.leaderHint
        reply := PutAppendReply{}

        ck.Log(" => %s request server %d\n", op, i)
        ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
        if ok {
            ck.Log("server %d, value:%v, WrongLeader: %v\n", i, value, reply.WrongLeader)
            if reply.WrongLeader == false {
                hasLeader = true
                if reply.Err == OK {
                    ck.Log("<== %s %v %v, index:%d\n", op, key, value, reply.Index)
                    return
                } else {
                    ck.Log("<xx %s %v %v, err:%v\n", op, key, value, reply.Err)
                    return
                }
            }
        } else {
            ck.Log("<xx %s server %d, value:%v, network timeout!\n", op, i, value)
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
