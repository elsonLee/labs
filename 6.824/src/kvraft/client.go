package raftkv

import "fmt"
import "labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	return ck
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
    args := GetArgs{Key: key}

    fmt.Printf("==> Get %v\n", key)
    for {
        hasLeader := false
        for i, _ := range ck.servers {
            reply := GetReply{}
            ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
            if ok {
                if reply.WrongLeader == false {
                    hasLeader = true
                    if reply.Err == OK {
                        fmt.Printf("<== Get %v, index:%d, {%v}\n",
                                    key, reply.Index, reply.Value)
                        return reply.Value
                    } else {
                        fmt.Printf("<xx Get %v, err:%v\n", key, reply.Err)
                        return ""
                    }
                }
            }
        }

        if !hasLeader {
            fmt.Printf("    Get %v : no leader!\n", key)
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
    args := PutAppendArgs{Key: key,
                          Value: value,
                          Op: op}
    fmt.Printf("==> %s %v %v\n", op, key, value)
    for {
        hasLeader := false
        for i, _ := range ck.servers {
            reply := PutAppendReply{}
            ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
            if ok {
                if reply.WrongLeader == false {
                    hasLeader = true
                    if reply.Err == OK {
                        fmt.Printf("<== %s %v %v, index:%d\n", op, key, value, reply.Index)
                        return
                    } else {
                        fmt.Printf("<xx %s %v %v, err:%v\n", op, key, value, reply.Err)
                        return
                    }
                }
            }
        }

        if !hasLeader {
            fmt.Printf("    %s %v %v : no leader!\n", op, key, value)
        }
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
