package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "fmt"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
    servers     []*labrpc.ClientEnd
    // Your data here.
    me          int64
    leaderHint  int
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
    // Your code here.
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

func (ck *Clerk) Query(num int) Config {

    // Your code here.
    info := Info{Clerk: ck.me, ID: nrand()}
    args := &QueryArgs{Info: info, Num: num}

    for {
        // try each known server.
        for _, srv := range ck.servers {
            var reply QueryReply
            ok := srv.Call("ShardMaster.Query", args, &reply)
            if ok && reply.WrongLeader == false {
                ck.Log("return %v\n", reply)
                return reply.Config
            }
        }
        time.Sleep(100 * time.Millisecond)
    }
}

func (ck *Clerk) Join(servers map[int][]string) {
    // Your code here.
    info := Info{Clerk: ck.me, ID: nrand()}
    args := &JoinArgs{Info: info,
                      Servers: servers}

    for {
        // try each known server.
        for _, srv := range ck.servers {
            var reply JoinReply
            ok := srv.Call("ShardMaster.Join", args, &reply)
            if ok && reply.WrongLeader == false {
                return
            }
        }
        time.Sleep(100 * time.Millisecond)
    }
}

func (ck *Clerk) Leave(gids []int) {
    // Your code here.
    info := Info{Clerk: ck.me, ID: nrand()}
    args := &LeaveArgs{Info: info,
                       GIDs: gids}

    for {
        // try each known server.
        for _, srv := range ck.servers {
            var reply LeaveReply
            ok := srv.Call("ShardMaster.Leave", args, &reply)
            if ok && reply.WrongLeader == false {
                return
            }
        }
        time.Sleep(100 * time.Millisecond)
    }
}

func (ck *Clerk) Move(shard int, gid int) {
    // Your code here.
    info := Info{Clerk: ck.me, ID: nrand()}
    args := &MoveArgs{Info: info,
                      Shard: shard,
                      GID: gid}

    for {
        // try each known server.
        for _, srv := range ck.servers {
            var reply MoveReply
            ok := srv.Call("ShardMaster.Move", args, &reply)
            if ok && reply.WrongLeader == false {
                return
            }
        }
        time.Sleep(100 * time.Millisecond)
    }
}
