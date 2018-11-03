package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "fmt"
import "math/rand"
import "time"
import "sync"
import "labrpc"

// import "bytes"
// import "labgob"

var kTick int = 100
var kHeartBeatTimeout int = 200
var kminElectionTimeout int = 500
var kmaxElectionTimeout int = 800

func generateElectionTimeout() int {
    min := kminElectionTimeout
    max := kmaxElectionTimeout
    return rand.Intn(max-min) + min
}

type Status int
const (
    Candidate Status = 0
    Leader    Status = 1
    Follower  Status = 2
    Invalid   Status = 3
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
        Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

        status          Status

        electionTimeout  int
        heartbeatTimeout int

        // Persistent state on all servers
        currentTerm     int
        votedFor        int
        log             []LogEntry

        // Volatile state on all servers
        commitIndex     int
        lastApplied     int

        // Volatile state on leaders
        nextIndex       []int
        matchIndex      []int

        // queue for request
        voteQueue       chan *RequestVoteWrapper
        appendQueue     chan *AppendEntriesWrapper
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
        term = rf.currentTerm
        if rf.status == Leader {
            isleader = true
        } else {
            isleader = false
        }

	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


type AppendEntriesArgs struct {
        Term            int
        LeaderId        int
        PrevLogIndex    int
        PrevLogTerm     int
        Entries         []LogEntry
        LeaderCommit    int
}


type AppendEntriesReply struct {
        Term            int
        Success         bool
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
        Term            int
        CandidateId     int
        LastLogIndex    int
        LastLogTerm     int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
        Term            int
        VoteGranted     bool
}


func (rf *Raft) IsUptodate(args *RequestVoteArgs) bool {
    lastLogIndex := -1
    lastLogTerm := -1
    lastIndex := len(rf.log)-1
    if lastIndex >= 0 {
        lastLogIndex = lastIndex
        lastLogTerm = rf.log[lastLogIndex].Term
    }

    if args.LastLogTerm > lastLogTerm {
        return true
    } else if args.LastLogTerm == lastLogTerm {
        if args.LastLogIndex >= lastLogIndex {
            return true
        } else {
            return false
        }
    } else {
        return false
    }
}

type RequestVoteWrapper struct {
    args        *RequestVoteArgs
    reply       *RequestVoteReply
    done        chan bool
}

func (rf *Raft) HandleRequestVoteWrapper(voteWrapper *RequestVoteWrapper) Status {

    nextStatus := rf.status
    args := voteWrapper.args
    reply := voteWrapper.reply

    if args.Term < rf.currentTerm {
        reply.VoteGranted = false
    } else {
        // ??? why check votedFor
        if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.IsUptodate(args) {
            rf.votedFor = args.CandidateId
            reply.Term = rf.currentTerm
            nextStatus = Follower
            reply.VoteGranted = true
        } else {
            reply.VoteGranted = false
        }
    }

    voteWrapper.done <- true
    //fmt.Printf("[%d:%d] <- [%d:%d] vote succ\n", rf.me, rf.currentTerm, voteWrapper.args.CandidateId, voteWrapper.args.Term)

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        if reply.VoteGranted {
            rf.votedFor = args.CandidateId
        } else {
            rf.votedFor = -1
        }
        nextStatus = Follower
    }

    return nextStatus
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here (2A, 2B).
    voteWrapper := &RequestVoteWrapper{args: args,
                                       reply: reply}
    voteWrapper.done = make(chan bool)

    rf.voteQueue <- voteWrapper
    <-voteWrapper.done
}

type AppendEntriesWrapper struct {
    args        *AppendEntriesArgs
    reply       *AppendEntriesReply
    done        chan bool
}

func (rf *Raft) HandleAppendEntries (appendWrapper *AppendEntriesWrapper) {

    args := appendWrapper.args
    reply := appendWrapper.reply

    if args.Term < rf.currentTerm {
        reply.Success = false
    } else {
        rf.electionTimeout = generateElectionTimeout()
        reply.Term = rf.currentTerm
        reply.Success = true
    }

    appendWrapper.done <- true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

    //fmt.Printf("[%d] -> [%d] append\n", args.LeaderId, rf.me)

    appendWrapper := &AppendEntriesWrapper{args: args,
                                           reply: reply}
    appendWrapper.done = make(chan bool)

    rf.appendQueue <- appendWrapper
    <-appendWrapper.done
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}


func (rf *Raft) ActAsFollower () Status {

    fmt.Printf("[%d:%d]%d -> foll \n", rf.me, rf.currentTerm, rf.votedFor);

    ticker := time.NewTicker(time.Duration(kTick) * time.Millisecond)
    rf.electionTimeout = generateElectionTimeout()

    for {
        select {

        case appendWrapper := <-rf.appendQueue:
            rf.HandleAppendEntries(appendWrapper)

        case voteWrapper := <-rf.voteQueue:
            return rf.HandleRequestVoteWrapper(voteWrapper)

        case <-ticker.C:
            rf.electionTimeout -= kTick
            if rf.electionTimeout <= 0 {
                return Candidate
            }
        }
    }
}


func (rf *Raft) SendVote (voteReplyCh chan *RequestVoteReply) {

    lastLogIndex := len(rf.log)-1
    lastLogTerm  := -1
    if lastLogIndex >= 0 {
        lastLogTerm = rf.log[lastLogIndex].Term
    }

    voteRequest := &RequestVoteArgs{Term: rf.currentTerm,
                                    CandidateId: rf.me,
                                    LastLogIndex: lastLogIndex,
                                    LastLogTerm: lastLogTerm}

    // send request
    for i := 0; i < len(rf.peers); i++ {
        if i != rf.me {
            go func(idx int, replyCh chan *RequestVoteReply) {
                voteReply := &RequestVoteReply{}
                if rf.sendRequestVote(idx, voteRequest, voteReply) == true {
                    replyCh <- voteReply
                }
            }(i, voteReplyCh)
        }
    }
}


func (rf *Raft) ActAsCandidate () Status {

    rf.currentTerm += 1
    rf.votedFor = rf.me

    fmt.Printf("[%d:%d] -> cand\n", rf.me, rf.currentTerm);

    voteReplyCh := make(chan *RequestVoteReply, len(rf.peers))

    // wait reply
    //sendVoteTimeout := 200
    rf.electionTimeout = generateElectionTimeout()
    ticker := time.NewTicker(time.Duration(kTick) * time.Millisecond)

    voteGrantCnt := 1

    rf.SendVote(voteReplyCh);
    for {
        select {

        case appendWrapper := <-rf.appendQueue:
            rf.HandleAppendEntries(appendWrapper)

        case voteWrapper := <-rf.voteQueue:
            return rf.HandleRequestVoteWrapper(voteWrapper)

        case voteReply := <-voteReplyCh:
            if voteReply.VoteGranted {
                voteGrantCnt += 1
            }

            if voteGrantCnt > len(rf.peers)/2 {
                fmt.Printf("[%d] recv majority\n", rf.me);
                return Leader
            }

        case <-ticker.C:
            rf.electionTimeout -= kTick
            if rf.electionTimeout <= 0 {
                return Candidate
            }
        }
    }
}


func (rf *Raft) ActAsLeader () Status {

    fmt.Printf("[%d:%d] -> lead\n", rf.me, rf.currentTerm);

    lastLogIndex := len(rf.log)-1
    lastLogTerm  := -1
    if lastLogIndex >= 0 {
        lastLogTerm = rf.log[lastLogIndex].Term
    }

    appendRequest := &AppendEntriesArgs{Term: rf.currentTerm,
                                        LeaderId: rf.me,
                                        PrevLogIndex: lastLogIndex,
                                        PrevLogTerm: lastLogTerm,
                                        LeaderCommit: rf.commitIndex}

    rf.heartbeatTimeout = kHeartBeatTimeout
    ticker := time.NewTicker(time.Duration(kTick) * time.Millisecond)

    for {
        select {

        case appendWrapper := <-rf.appendQueue:
            rf.HandleAppendEntries(appendWrapper)

        case voteWrapper := <-rf.voteQueue:
            return rf.HandleRequestVoteWrapper(voteWrapper)

        case <-ticker.C:
            rf.heartbeatTimeout -= kTick
            if rf.heartbeatTimeout <= 0 {
                rf.heartbeatTimeout = kHeartBeatTimeout
                // send request
                for i := 0; i < len(rf.peers); i++ {
                    if i != rf.me {
                        go func(idx int) {
                            appendReply := &AppendEntriesReply{}
                            if rf.sendAppendEntries(idx, appendRequest, appendReply) == true {
                                //replyCh <- appendReply
                            }
                        }(i)
                    }
                }
            }
        }
    }
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
        rf.currentTerm = 0
        rf.votedFor = -1
        rf.commitIndex = 0
        rf.lastApplied = 0
        rf.status = Candidate
        rf.electionTimeout = generateElectionTimeout()
        rf.voteQueue = make(chan *RequestVoteWrapper, len(rf.peers))
        rf.appendQueue = make(chan *AppendEntriesWrapper, len(rf.peers))

        nextStatus := Follower

        go func(rf *Raft) {
            for {
                rf.status = nextStatus
                switch nextStatus {
                case Follower:
                    nextStatus = rf.ActAsFollower()
                case Candidate:
                    nextStatus = rf.ActAsCandidate()
                case Leader:
                    nextStatus = rf.ActAsLeader()
                }
            }
        }(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

