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

import "bytes"
import "labgob"

var origTime time.Time = time.Now()
func timeStampInMs () int64 {
    return time.Since(origTime).Nanoseconds()/1000000
}

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

var kTick int = 100
var kHeartBeatTimeout int = 200
var kminElectionTimeout int = 300
var kmaxElectionTimeout int = 700

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

func StatusName (s Status) string {
    switch s {
    case Candidate: return "cand"
    case Leader: return "lead"
    case Follower: return "foll"
    default: return "unknown"
    }
}

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
    CommandValid    bool
    Command         interface{}
    CommandIndex    int
}

type LogEntry struct {
    Term        int
    Command     interface{}
}

type HeartBeatReply struct {
    Server          int
    PrevIndex       int
    NextTryIndex    int
    Success         bool
}

type CommandReply struct {
    Term        int
    Index       int
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

        applyCh         chan ApplyMsg

        debugOn         bool

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
        hbQueue         chan HeartBeatReply

        cmdQueue        chan interface{}
        cmdReplyQueue   chan CommandReply
}

func (rf *Raft) Log (format string, a ...interface{}) {
    if rf.debugOn {
        fmt.Printf("%d ms [%d:%d] %s",
            timeStampInMs(), rf.me, rf.currentTerm,
            fmt.Sprintf(format, a...))
    }
}

func (rf *Raft) LastLogIndex () int {
    return len(rf.log)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState () (int, bool) {

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
        w := new(bytes.Buffer)
        e := labgob.NewEncoder(w)
        e.Encode(rf.currentTerm)
        e.Encode(rf.votedFor)
        e.Encode(rf.log)
        data := w.Bytes()
        rf.persister.SaveRaftState(data)
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
        r := bytes.NewBuffer(data)
        d := labgob.NewDecoder(r)
        var currentTerm int
        var votedFor int
        var log []LogEntry
        if d.Decode(&currentTerm) != nil ||
           d.Decode(&votedFor) != nil ||
           d.Decode(&log) != nil {
            panic("read presist error!")
        } else {
            rf.currentTerm = currentTerm
            rf.votedFor = votedFor
            rf.log = log
        }
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
    NextTryIndex    int     //! for quick conflict position search
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


func (rf *Raft) IsUptodate (args *RequestVoteArgs) bool {
    lastLogIndex := -1
    lastLogTerm := -1
    lastIndex := len(rf.log)
    if lastIndex > 0 {
        lastLogIndex = lastIndex
        lastLogTerm = rf.log[lastLogIndex-1].Term
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

func (rf *Raft) HandleRequestVoteWrapper (voteWrapper *RequestVoteWrapper) Status {

    nextStatus := rf.status
    args := voteWrapper.args
    reply := voteWrapper.reply

    reply.Term = rf.currentTerm     // original currentTerm

    if args.Term < rf.currentTerm {
        reply.VoteGranted = false
    } else {

        if args.Term > rf.currentTerm {
            rf.currentTerm = args.Term
            rf.votedFor = -1
            nextStatus = Follower
        }

        if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.IsUptodate(args) {
            rf.votedFor = args.CandidateId
            reply.VoteGranted = true
        } else {
            reply.VoteGranted = false
        }
    }

    rf.Log("[%s:%d:%v] vote to %d, %v\n",
            StatusName(rf.status), rf.votedFor, rf.IsUptodate(args), args.CandidateId, reply.VoteGranted)

    rf.persist()

    voteWrapper.done <- true
    //fmt.Printf("[%d:%d] <- [%d:%d] vote succ\n", rf.me, rf.currentTerm, voteWrapper.args.CandidateId, voteWrapper.args.Term)

    return nextStatus
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote (args *RequestVoteArgs, reply *RequestVoteReply) {
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

func (rf *Raft) HandleAppendEntries (appendWrapper *AppendEntriesWrapper) Status {

    nextStatus := rf.status
    args := appendWrapper.args
    reply := appendWrapper.reply

    reply.NextTryIndex = -1

    if args.Term < rf.currentTerm {
        reply.Success = false
    } else {

        reply.Term = rf.currentTerm

        if args.PrevLogIndex <= 0 {
            if len(rf.log) > 0 {
                rf.log = rf.log[:0]     //! clear
            }
            reply.Success = true
        } else {
            if rf.LastLogIndex() < args.PrevLogIndex {
                reply.NextTryIndex = rf.LastLogIndex() + 1
                reply.Success = false
            } else {
                if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
                    for i :=args.PrevLogIndex; i >= 1; i-- {
                        if rf.log[i-1].Term == rf.log[args.PrevLogIndex-1].Term {
                            reply.NextTryIndex = i+1
                        } else {
                            break
                        }
                    }
                    rf.log = rf.log[0:args.PrevLogIndex-1]
                    reply.Success = false
                } else {
                    rf.log = rf.log[0:args.PrevLogIndex]
                    reply.Success = true
                }
            }
        }

        //! reset election timeout when succ
        rf.electionTimeout = generateElectionTimeout()
    }

    if reply.Success {
        if len(args.Entries) > 0 {
            rf.Log("app %d, %v\n", args.LeaderId, args.Entries)
            rf.log = append(rf.log, args.Entries...)
        }
        oldCommitIndex := rf.commitIndex
        if args.LeaderCommit > rf.commitIndex {
            if args.LeaderCommit < rf.LastLogIndex() {
                rf.commitIndex = args.LeaderCommit
            } else {
                rf.commitIndex = rf.LastLogIndex()
            }

            for i := oldCommitIndex+1; i <= rf.commitIndex; i++ {
                msg := ApplyMsg{CommandValid: true,
                                Command: rf.log[i-1].Command,
                                CommandIndex: i}
                rf.Log("apply msg: %d %v\n", msg.CommandIndex, msg.Command)
                rf.applyCh <- msg
                rf.lastApplied = rf.commitIndex
            }
        }

        nextStatus = Follower
    }

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        nextStatus = Follower
        if reply.Success {
            rf.votedFor = args.LeaderId
        } else {
            rf.votedFor = -1
        }
    }

    rf.persist()

    appendWrapper.done <- true

    return nextStatus
}

func (rf *Raft) AppendEntries (args *AppendEntriesArgs, reply *AppendEntriesReply) {

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
func (rf *Raft) Start (command interface{}) (int, int, bool) {
    index := -1
    term := -1
    isLeader := true

    // Your code here (2B).
    if rf.status != Leader {
        isLeader = false
    } else {
        rf.Log("Start cmd: %v\n", command)
        rf.cmdQueue <- command
        cmdReply := <-rf.cmdReplyQueue
        index = cmdReply.Index
        term = cmdReply.Term
    }

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
    rf.debugOn = false

    //! reset origTime
    origTime = time.Now()

    rf.Log("%v\n", rf.log)
}


func (rf *Raft) ActAsFollower () Status {

    //rf.Log("%d -> foll, %d\n", rf.votedFor, rf.log)
    rf.Log("-> foll vote: %d\n", rf.votedFor)

    ticker := time.NewTicker(time.Duration(kTick) * time.Millisecond)
    rf.electionTimeout = generateElectionTimeout()

    nextStatus := rf.status
    for {
        select {

        case appendWrapper := <-rf.appendQueue:
            nextStatus = rf.HandleAppendEntries(appendWrapper)
            if nextStatus != Follower {
                return nextStatus
            }

        case voteWrapper := <-rf.voteQueue:
            nextStatus = rf.HandleRequestVoteWrapper(voteWrapper)
            if nextStatus != Follower {
                return nextStatus
            }

        case <-ticker.C:
            rf.electionTimeout -= kTick
            if rf.electionTimeout <= 0 {
                return Candidate
            }
        }
    }
}


func (rf *Raft) SendVote (voteReplyCh chan *RequestVoteReply) {

    lastLogIndex := len(rf.log)
    lastLogTerm := -1
    if lastLogIndex > 0 {
        lastLogTerm = rf.log[lastLogIndex-1].Term
    }

    voteRequest := &RequestVoteArgs{Term: rf.currentTerm,
                                    CandidateId: rf.me,
                                    LastLogIndex: lastLogIndex,
                                    LastLogTerm: lastLogTerm}
    // send request
    for i := 0; i < len(rf.peers); i++ {
        if i != rf.me {
            go func (idx int, replyCh chan *RequestVoteReply) {
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

    //rf.Log("-> cand, %v\n", rf.log)
    rf.Log("-> cand\n")

    voteReplyCh := make(chan *RequestVoteReply, len(rf.peers))

    // wait reply
    rf.electionTimeout = generateElectionTimeout()
    ticker := time.NewTicker(time.Duration(kTick) * time.Millisecond)

    voteGrantCnt := 1

    rf.SendVote(voteReplyCh);
    nextStatus := rf.status
    for {
        select {

        case appendWrapper := <-rf.appendQueue:
            nextStatus = rf.HandleAppendEntries(appendWrapper)
            if nextStatus != Candidate {
                return nextStatus
            }

        case voteWrapper := <-rf.voteQueue:
            nextStatus = rf.HandleRequestVoteWrapper(voteWrapper)
            if nextStatus != Candidate {
                return nextStatus
            }

        case voteReply := <-voteReplyCh:
            //rf.Log("recv voteReply: %v\n", voteReply.VoteGranted)
            if voteReply.VoteGranted {
                voteGrantCnt += 1
            }

            if voteGrantCnt > len(rf.peers)/2 {
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


// new entry must be saved in log first
func (rf Raft) PrepareAppendEntries (index int, len int) *AppendEntriesArgs {

    prevLogIndex := index-1
    prevLogTerm := -1
    if prevLogIndex > 0 {
        prevLogTerm = rf.log[prevLogIndex-1].Term
    }

    appendRequest := &AppendEntriesArgs{Term: rf.currentTerm,
                                        LeaderId: rf.me,
                                        PrevLogIndex: prevLogIndex,
                                        PrevLogTerm: prevLogTerm,
                                        LeaderCommit: rf.commitIndex}
    for i := 0; i < len; i++ {
        if index+i-1 >= rf.LastLogIndex() {
            rf.Log("index: %d, len: %d, lastLogIndex: %d\n",
                    index, i, rf.LastLogIndex())
            panic(0)
        }
        newEntry := rf.log[index+i-1]
        appendRequest.Entries = append(appendRequest.Entries, newEntry)
    }

    return appendRequest
}


func (rf *Raft) RequestHeartBeat (server int, appendRequest *AppendEntriesArgs) {

    appendReply := &AppendEntriesReply{}
    if rf.sendAppendEntries(server, appendRequest, appendReply) == true {
        //rf.Log("recv hb from %d, %v\n", server, appendReply.Success)
        rf.hbQueue <- HeartBeatReply{Server: server,
                                     PrevIndex: appendRequest.PrevLogIndex,
                                     NextTryIndex: appendReply.NextTryIndex,
                                     Success: appendReply.Success}
        if appendReply.Success == false {
            rf.Log("hb failed from %d, previdx: %d, nextry: %d\n",
                    server, appendRequest.PrevLogIndex, appendReply.NextTryIndex)
        }
    }
}


func (rf *Raft) RequestCommands (server int, appendRequest *AppendEntriesArgs) *AppendEntriesReply {

    appendReply := &AppendEntriesReply{}

    if rf.sendAppendEntries(server, appendRequest, appendReply) == true {
        if appendReply.Success == true {
            rf.nextIndex[server] = Min(appendRequest.PrevLogIndex+2,
                                       rf.LastLogIndex()+1)
        } else {
            //rf.nextIndex[server] = Max(1, appendReqeust.PrevLogIndex)
        }
    }

    return appendReply
}


func (rf *Raft) AppendCommand (command interface{}) {

    newEntry := LogEntry{Term: rf.currentTerm,
                         Command: command}

    rf.log = append(rf.log, newEntry)
    rf.cmdReplyQueue <- CommandReply{Term: rf.currentTerm,
                                     Index: rf.LastLogIndex()}

    cntCh := make(chan int, 1)
    for server := 0; server < len(rf.peers); server++ {
        if server != rf.me {
            go func (server_ int, cntCh_ chan int) {
                request := rf.PrepareAppendEntries(rf.LastLogIndex(), 1)
                reply := rf.RequestCommands(server_, request)
                if reply.Success {
                    cntCh_ <- 1
                } else {
                    cntCh_ <- 0
                }
            }(server, cntCh)
        }
    }

    ticker := time.NewTicker(time.Duration(kTick) * time.Millisecond)
    recvCnt := 0
    commitCnt := 1
    for {
        select {
        case cnt := <-cntCh:
            recvCnt += 1
            commitCnt += cnt
            if commitCnt > len(rf.peers)/2 {
                oldCommitIndex := rf.commitIndex
                rf.commitIndex = rf.LastLogIndex()
                for i := oldCommitIndex+1; i <= rf.commitIndex; i++ {
                    msg := ApplyMsg{CommandValid: true,
                                    Command: rf.log[i-1].Command,
                                    CommandIndex: i}
                    rf.Log("apply msg: %d %v\n", msg.CommandIndex, msg.Command)
                    rf.persist()
                    rf.applyCh <- msg
                }
                rf.lastApplied = rf.commitIndex
                return
            }
            if recvCnt == len(rf.peers)-1 {
                return
            }
        case <- ticker.C:
            //rf.Log("append entry timeout, commitCnt: %d\n", commitCnt)
            return
        }
    }
}


func (rf *Raft) SendHeartBeat () {

    var hbRequests []*AppendEntriesArgs

    for server := 0; server < len(rf.peers); server++ {
        len := rf.LastLogIndex()-rf.nextIndex[server]+1
        if len < 0 {
            rf.Log("lastLogIndex: %d, nextIndex[%d]: %d\n",
                    rf.LastLogIndex(), server, rf.nextIndex[server])
            panic(0)
        }
        hbRequests = append(hbRequests,
                            rf.PrepareAppendEntries(rf.nextIndex[server], len))
    }

    for server := 0; server < len(rf.peers); server++ {
        if server != rf.me {
            go func (server_ int, hbRequests_ []*AppendEntriesArgs) {
                rf.RequestHeartBeat(server_, hbRequests_[server_])
            }(server, hbRequests)
        }
    }
}


func (rf *Raft) ActAsLeader () Status {

    rf.Log("-> lead\n")

    for i := 0; i < len(rf.peers); i++ {
        rf.nextIndex[i] = rf.LastLogIndex()+1
    }

    rf.heartbeatTimeout = kHeartBeatTimeout
    ticker := time.NewTicker(time.Duration(kTick) * time.Millisecond)

    rf.SendHeartBeat()

    nextStatus := rf.status
    for {
        select {

        case hbReply := <-rf.hbQueue:
            if hbReply.Success == true {
                rf.nextIndex[hbReply.Server] = Min(hbReply.PrevIndex+2,
                                                   rf.LastLogIndex()+1)
            } else {
                if hbReply.NextTryIndex != -1 {
                    rf.nextIndex[hbReply.Server] = Min(rf.nextIndex[hbReply.Server],
                                                       Max(1, hbReply.NextTryIndex))
                } else {
                    rf.nextIndex[hbReply.Server] = Max(1, hbReply.PrevIndex)
                }
            }

        case command := <-rf.cmdQueue:
            rf.AppendCommand(command)

        case appendWrapper := <-rf.appendQueue:
            nextStatus = rf.HandleAppendEntries(appendWrapper)
            if nextStatus != Leader {
                return nextStatus
            }

        case voteWrapper := <-rf.voteQueue:
            nextStatus = rf.HandleRequestVoteWrapper(voteWrapper)
            if nextStatus != Leader {
                return nextStatus
            }

        case <-ticker.C:
            rf.heartbeatTimeout -= kTick
            if rf.heartbeatTimeout <= 0 {
                rf.heartbeatTimeout = kHeartBeatTimeout
                rf.SendHeartBeat()
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
        rf.applyCh = applyCh
        rf.debugOn = true
        //rf.debugOn = false
        rf.currentTerm = 0
        rf.votedFor = -1
        rf.commitIndex = 0
        rf.lastApplied = 0
        rf.status = Follower
        rf.electionTimeout = generateElectionTimeout()
        rf.voteQueue = make(chan *RequestVoteWrapper, len(rf.peers))
        rf.appendQueue = make(chan *AppendEntriesWrapper, len(rf.peers))
        rf.hbQueue = make(chan HeartBeatReply, len(rf.peers))
        rf.cmdQueue = make(chan interface{}, len(rf.peers))
        rf.cmdReplyQueue = make(chan CommandReply, len(peers))
        rf.nextIndex = make([]int, len(peers))
        rf.matchIndex = make([]int, len(peers))

        nextStatus := Follower

        go func (rf *Raft) {
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

