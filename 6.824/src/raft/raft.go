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

import (
    "math/rand"
    "fmt"
    "time"
    "sync"
    "bytes"
    "sort"

    "labrpc"
    "labgob"
)


var kTick int = 100
var kHeartbeatTimeout int = 200
var kminElectionTimeout int = 300
var kmaxElectionTimeout int = 700

var origTime time.Time = time.Now()
func timeStampInMs () int64 {
    return time.Since(origTime).Nanoseconds()/1000000
}

func generateElectionTimeout() int {
    min := kminElectionTimeout
    max := kmaxElectionTimeout
    return rand.Intn(max-min) + min
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

type State struct {
    Term        int
    IsLeader    bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {

    mu                  sync.Mutex          // Lock to protect shared access to this peer's state

    peers               []*labrpc.ClientEnd // RPC end points of all peers

    persister           *Persister          // Object to hold this peer's persisted state

    me                  int                 // this peer's index into peers[]

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    applyCh             chan ApplyMsg

    debugOn             AtomicBool

    status              Status

    electionTimeout     int

    heartbeatTimeout    int

    // Persistent state on all servers
    currentTerm         int

    votedFor            int

    log                 []LogEntry

    // Volatile state on all servers
    commitIndex         int

    lastApplied         int

    // Volatile state on leaders
    nextIndex           []int

    matchIndex          []int       // indicates data has been replicated to peer, doesn't indicate commited

    // channels
    voteCh              chan RequestVoteWrapper

    appendCh            chan AppendEntriesWrapper

    appendReplyCh       chan AppendEntriesReplyWrapper

    heartbeatReplyCh    chan AppendEntriesReplyWrapper

    commandCh           chan CommandRequest

    stateCh             chan chan State
}


func (rf *Raft) Log (format string, a ...interface{}) {
    if rf.debugOn.Get() {
        fmt.Printf("%d ms [%s%d:t%d:i%d:t%d] %s",
            timeStampInMs(), AbbrStatusName(rf.status), rf.me, rf.currentTerm,
            rf.LastLogIndex(), rf.LastLogTerm(),
            fmt.Sprintf(format, a...))
    }
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState () (int, bool) {

    // Your code here (2A).
    c := make(chan State)
    select {
    case rf.stateCh <- c:
        state := <-c
        return state.Term, state.IsLeader
    }
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
        //e.Encode(rf.matchIndex)
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
        //var matchIndex []int
        if d.Decode(&currentTerm) != nil ||
           d.Decode(&votedFor) != nil ||
           d.Decode(&log) != nil {
           //d.Decode(&matchIndex) != nil {
            panic("read presist error!")
        } else {
            rf.currentTerm = currentTerm
            rf.votedFor = votedFor
            rf.log = log
            //rf.matchIndex = matchIndex
        }
}


func (rf *Raft) LogEntry (index int) LogEntry {
    if index <= 0 || index > rf.LastLogIndex() {
        panic("LogEntry out of range!")
    }
    return rf.log[index - 1]
}


func (rf *Raft) LastLogIndex () int {
    return len(rf.log)
}


func (rf *Raft) LastLogTerm () int {
    lastLogIndex := rf.LastLogIndex()
    if lastLogIndex >= 1 {
        return rf.LogEntry(lastLogIndex).Term
    } else {
        return 0
    }
}


func (rf *Raft) IsUptodate (args *RequestVoteArgs) bool {
    lastLogIndex := rf.LastLogIndex()
    lastLogTerm := rf.LastLogTerm()

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


func (rf *Raft) TryCommitAndApply (leaderCommited int) {

    oldCommitIndex := rf.commitIndex
    if leaderCommited > rf.commitIndex {
        rf.commitIndex = Min(leaderCommited, rf.LastLogIndex())
        for i := oldCommitIndex + 1; i <= rf.commitIndex; i++ {
            msg := ApplyMsg{CommandValid: true,
                            Command: rf.LogEntry(i).Command,
                            CommandIndex: i}
            rf.Log("apply msg: %d %v\n", msg.CommandIndex, msg.Command)
            rf.applyCh <- msg
            rf.lastApplied = rf.commitIndex
        }
    }
}


func (rf *Raft) FindConflictIndex (index int, entries []LogEntry) int {
    for i, entry := range entries {
        if index + i <= rf.LastLogIndex() {
            if rf.LogEntry(index + i).Term != entry.Term {
                return index + i
            }
        } else {
            return rf.LastLogIndex() + 1
        }
    }
    return 0
}


func (rf *Raft) HandleRequestVoteWrapper (voteWrapper RequestVoteWrapper) Status {

    nextStatus := rf.status
    args := voteWrapper.Args
    reply := voteWrapper.Reply

    oldTerm := rf.currentTerm
    oldVotedFor := rf.votedFor

    if args.CandidateId == rf.me {
        panic("args.CandidateId shouldn't be equal to rf.me!")
    }

    reply.Term = rf.currentTerm     // original currentTerm

    if args.Term < rf.currentTerm {
        reply.VoteGranted = false
    } else {

        // update currentTerm to max value, for node that recovered from error or parition with 
        // big term value, it won't be accepted to the system without this update
        if args.Term > rf.currentTerm {
            rf.currentTerm = args.Term
            // votedFor must be None, cuz the vote can be sent by candidate with non up-to-date log
            rf.votedFor = None
            nextStatus = Follower
        }

        // IsUptodate ensures the leader has the latest log
        // votedFor == None or CandidateId ensures node can only vote for one another node, for election safety
        if (rf.votedFor == None || rf.votedFor == args.CandidateId) && rf.IsUptodate(args) {
            rf.votedFor = args.CandidateId
            reply.VoteGranted = true
        } else {
            reply.VoteGranted = false
        }
    }

    if reply.VoteGranted {
        rf.electionTimeout = generateElectionTimeout()
    }

    rf.Log("RVresp %d => %d {term:%d -> %d, voteFor:%v -> %v, succ:%v}, %v -> %v\n",
            rf.me,
            args.CandidateId,
            oldTerm, rf.currentTerm,
            oldVotedFor, rf.votedFor, reply.VoteGranted,
            StatusName(rf.status), StatusName(nextStatus))

    rf.persist()

    voteWrapper.Done <- true

    return nextStatus
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote (args *RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here (2A, 2B).
    voteWrapper := RequestVoteWrapper{Args: args,
                                      Reply: reply,
                                      Done: make(chan bool)}
    rf.voteCh <- voteWrapper
    <-voteWrapper.Done
}


func (rf *Raft) HandleAppendEntries (appendWrapper AppendEntriesWrapper) Status {

    nextStatus := rf.status
    args := appendWrapper.Args
    reply := appendWrapper.Reply

    oldTerm := rf.currentTerm
    oldVotedFor := rf.votedFor

    reply.MatchIndex = rf.commitIndex
    reply.NextIndexHint = 0

    if args.Term < rf.currentTerm {
        reply.Success = false
    } else {

        if args.Term > rf.currentTerm {
            rf.currentTerm = args.Term
            //rf.votedFor = args.LeaderId     // AppendEntries must be sent by leader
            rf.votedFor = None      // AE must be sent by leader, but the leader may be recovered just now
            rf.Log("-> foll vote: %d\n", rf.votedFor)
            nextStatus = Follower
        }

        //reply.Term = rf.currentTerm

        if args.PrevLogIndex <= 0 {
            // FIXME
            if len(rf.log) > 0 {
                rf.log = rf.log[:0]     //! clear
            }
            if len(args.Entries) > 0 {
                rf.log = append(rf.log, args.Entries...)
                reply.MatchIndex = rf.LastLogIndex()
            }
            reply.Success = true
        } else if args.PrevLogIndex < rf.commitIndex {  // commited date cannot be overwritten
            reply.Success = true
        } else {

            if rf.LastLogIndex() < args.PrevLogIndex {
                reply.NextIndexHint = rf.LastLogIndex() + 1
                reply.Success = false
            } else {
                if rf.LogEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
                    for i := args.PrevLogIndex; i >= Max(1, rf.commitIndex); i-- {
                        if rf.LogEntry(i).Term == rf.LogEntry(args.PrevLogIndex).Term {
                            reply.NextIndexHint = i + 1
                        } else {
                            break
                        }
                    }
                    rf.log = rf.log[0:args.PrevLogIndex-1]
                    reply.Success = false
                } else {

                    conflictIndex := rf.FindConflictIndex(args.PrevLogIndex + 1, args.Entries)
                    if conflictIndex > 0 {
                        rf.log = rf.log[0:conflictIndex - 1]
                        rf.log = append(rf.log, args.Entries[conflictIndex - (args.PrevLogIndex + 1):]...)
                        reply.MatchIndex = rf.LastLogIndex()
                    } else if conflictIndex == 0 {
                        reply.MatchIndex = args.PrevLogIndex + len(args.Entries)
                    } else {
                        panic("conflictIndex < 0")
                    }
                    reply.Success = true
                }
            }
        }

        //! reset election timeout
        rf.electionTimeout = generateElectionTimeout()
    }

    rf.persist()

    rf.Log("%sresp %d => %d {matchIndex:%d, nextHint:%d, term:%d -> %d, voteFor:%v -> %v, succ:%v}, %v -> %v\n",
            args.Type,
            rf.me,
            args.LeaderId,
            reply.MatchIndex,
            reply.NextIndexHint,
            oldTerm, rf.currentTerm,
            oldVotedFor, rf.votedFor, reply.Success,
            StatusName(rf.status), StatusName(nextStatus))

    if reply.Success {
        // must consider MatchIndex, or will commit wrong data
        rf.TryCommitAndApply(Min(reply.MatchIndex, args.LeaderCommit))
    }

    appendWrapper.Done <- true

    return nextStatus
}


func (rf *Raft) AppendEntries (args *AppendEntriesArgs, reply *AppendEntriesReply) {

    appendWrapper := AppendEntriesWrapper{Args: args,
                                          Reply: reply,
                                          Done: make(chan bool)}
    rf.appendCh <- appendWrapper
    <-appendWrapper.Done
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

    // Your code here (2B).
    replyCh := make(chan CommandReply)
    commandRequest := CommandRequest{Command: command,
                                     ReplyCh: replyCh}
    rf.commandCh <- commandRequest
    reply := <-commandRequest.ReplyCh

    //if reply.IsLeader {
        rf.Log("reply Command: {%d, %v}, isLeader: %v\n", reply.Index, command, reply.IsLeader)
    //}
    return reply.Index, reply.Term, reply.IsLeader
}


//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
    // Your code here, if desired.
    rf.debugOn.Set(false)

    //! reset origTime
    origTime = time.Now()
}


func (rf *Raft) ActAsFollower () Status {

    ticker := time.NewTicker(time.Duration(kTick) * time.Millisecond)
    rf.electionTimeout = generateElectionTimeout()

    nextStatus := rf.status
    for {
        select {

        case c := <-rf.stateCh:
            c <- State{Term: rf.currentTerm,
                       IsLeader: false}

        // ignore
        case <-rf.appendReplyCh:
        case <-rf.heartbeatReplyCh:

        case commandRequest := <-rf.commandCh:
            commandRequest.ReplyCh <- CommandReply{Term: rf.currentTerm,
                                                   Index: -1,
                                                   IsLeader: false}

        case appendWrapper := <-rf.appendCh:
            nextStatus = rf.HandleAppendEntries(appendWrapper)
            if nextStatus != Follower {
                return nextStatus
            }

        case voteWrapper := <-rf.voteCh:
            nextStatus = rf.HandleRequestVoteWrapper(voteWrapper)
            if nextStatus != Follower {
                return nextStatus
            }

        case <-ticker.C:
            rf.electionTimeout -= kTick
            if rf.electionTimeout <= 0 {
                rf.Log("-> cand %d\n", rf.me)
                return Candidate
            }
        }
    }
}


func (rf *Raft) SendVote (voteReplyCh chan *RequestVoteReply) {

    voteRequest := &RequestVoteArgs{Term: rf.currentTerm,
                                    CandidateId: rf.me,
                                    LastLogIndex: rf.LastLogIndex(),
                                    LastLogTerm: rf.LastLogTerm()}
    // send request
    for i := 0; i < len(rf.peers); i++ {
        if i != rf.me {
            go func (server int, replyCh chan *RequestVoteReply) {
                voteReply := &RequestVoteReply{}
                rf.Log("RV %d => %d {term:%d, lastIndex:%d, lastTerm:%d}\n",
                        rf.me, server, voteRequest.Term,
                        voteRequest.LastLogIndex, voteRequest.LastLogTerm)

                if rf.sendRequestVote(server, voteRequest, voteReply) == true {
                    replyCh <- voteReply
                }
            }(i, voteReplyCh)
        }
    }
}


func (rf *Raft) ActAsCandidate () Status {

    rf.currentTerm += 1
    rf.votedFor = rf.me

    voteReplyCh := make(chan *RequestVoteReply, len(rf.peers))

    // wait reply
    rf.electionTimeout = generateElectionTimeout()
    ticker := time.NewTicker(time.Duration(kTick) * time.Millisecond)

    voteGrantCnt := 1

    rf.SendVote(voteReplyCh);
    nextStatus := rf.status
    for {
        select {

        case c := <-rf.stateCh:
            c <- State{Term: rf.currentTerm,
                       IsLeader: false}

        // ignore
        case <-rf.appendReplyCh:
        case <-rf.heartbeatReplyCh:

        case commandRequest := <-rf.commandCh:
            commandRequest.ReplyCh <- CommandReply{Term: rf.currentTerm,
                                                   Index: -1,
                                                   IsLeader: false}

        case appendWrapper := <-rf.appendCh:
            nextStatus = rf.HandleAppendEntries(appendWrapper)
            if nextStatus != Candidate {
                return nextStatus
            }

        case voteWrapper := <-rf.voteCh:
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
                rf.Log("-> lead %d\n", rf.me)
                return Leader
            }

        case <-ticker.C:
            rf.electionTimeout -= kTick
            if rf.electionTimeout <= 0 {
                rf.Log("-> cand %d\n", rf.me)
                return Candidate
            }
        }
    }
}


// new entry must be saved in log first
func (rf Raft) PrepareAppendEntries (msgType MsgType, index int, len int) *AppendEntriesArgs {

    // FIXME
    prevLogIndex := index-1
    prevLogTerm := 0
    if prevLogIndex >= 1 {
        prevLogTerm = rf.LogEntry(prevLogIndex).Term
    }

    appendRequest := &AppendEntriesArgs{Type: msgType,
                                        Term: rf.currentTerm,
                                        LeaderId: rf.me,
                                        PrevLogIndex: prevLogIndex,
                                        PrevLogTerm: prevLogTerm,
                                        LeaderCommit: rf.commitIndex}
    for i := 0; i < len; i++ {
        if index + i - 1 >= rf.LastLogIndex() {
            rf.Log("index: %d, len: %d, lastLogIndex: %d\n",
                    index, i, rf.LastLogIndex())
            panic(0)
        }
        newEntry := rf.LogEntry(index + i)
        appendRequest.Entries = append(appendRequest.Entries, newEntry)
    }

    return appendRequest
}


// immutable
func (rf *Raft) RequestAppend (server int, request *AppendEntriesArgs) {

    reply := AppendEntriesReply{}
    if rf.sendAppendEntries(server, request, &reply) == true {
        rf.appendReplyCh <- AppendEntriesReplyWrapper{Server: server,
                                                      Index: request.PrevLogIndex,
                                                      Reply: reply}
        //if reply.Success == false {
        //    rf.Log("append failed from %d, previdx: %d, nextry: %d\n",
        //            server, request.PrevLogIndex, reply.NextIndexHint)
        //}
    }
}


func (rf *Raft) RequestHeartbeat (server int, request *AppendEntriesArgs) {

    reply := AppendEntriesReply{}
    if rf.sendAppendEntries(server, request, &reply) == true {
        rf.heartbeatReplyCh <- AppendEntriesReplyWrapper{Server: server,
                                                         Index: request.PrevLogIndex,
                                                         Reply: reply}
        //if reply.Success == false {
        //    rf.Log("hb failed from %d, previdx: %d, nextry: %d\n",
        //            server, request.PrevLogIndex, reply.NextIndexHint)
        //}
    }
}


func (rf *Raft) RequestCommands (server int, appendRequest *AppendEntriesArgs) *AppendEntriesReply {

    appendReply := &AppendEntriesReply{}

    if rf.sendAppendEntries(server, appendRequest, appendReply) == true {
        return appendReply
    } else {
        return nil
    }
}


func (rf *Raft) AppendCommand (commandRequest CommandRequest) {

    command := commandRequest.Command
    newEntry := LogEntry{Term: rf.currentTerm,
                         Command: command}

    rf.Log("Command: %v -> %d\n", command, rf.me)

    rf.log = append(rf.log, newEntry)

    // respond to client
    commandRequest.ReplyCh <- CommandReply{Term: rf.currentTerm,
                                           Index: rf.LastLogIndex(),
                                           IsLeader: true}

    rf.BroadcastAppend()
}


func (rf *Raft) SendAppend (server int) {

    if server == rf.me {
        return
    }

    size := rf.LastLogIndex() - rf.nextIndex[server] + 1
    if size < 0 {
        rf.Log("lastLogIndex: %d, nextIndex[%d]: %d\n",
        rf.LastLogIndex(), server, rf.nextIndex[server])
        panic(0)
    }

    request := rf.PrepareAppendEntries(MsgAE, rf.nextIndex[server], size)

    rf.Log("AE %d => %d {prevIndex:%d, commit:%d, entriesLen:%d}\n",
            rf.me, server, request.PrevLogIndex, request.LeaderCommit, len(request.Entries))

    go func () {
        rf.RequestAppend(server, request)
    }()
}


func (rf *Raft) SendHeartbeat (server int) {

    if server == rf.me {
        return
    }

    request := rf.PrepareAppendEntries(MsgHB, rf.nextIndex[server], 0)

    rf.Log("HB %d => %d {prevIndex:%d, prevTerm:%d, commit:%d, entriesLen:%d}\n",
            rf.me, server, request.PrevLogIndex, request.PrevLogTerm,
            request.LeaderCommit, len(request.Entries))

    go func () {
        rf.RequestHeartbeat(server, request)
    }()
}


func (rf *Raft) BroadcastAppend () {

    for server := 0; server < len(rf.peers); server++ {
        if server != rf.me {
            rf.SendAppend(server)
        }
    }
}


func (rf *Raft) BroadcastHeartbeat () {

    for server := 0; server < len(rf.peers); server++ {
        if server != rf.me {
            rf.SendHeartbeat(server)
        }
    }
}


func (rf *Raft) ActAsLeader () Status {

    for i := 0; i < len(rf.peers); i++ {
        rf.matchIndex[i] = 0
        rf.nextIndex[i] = rf.LastLogIndex()+1
    }

    rf.heartbeatTimeout = kHeartbeatTimeout
    ticker := time.NewTicker(time.Duration(kTick) * time.Millisecond)

    //rf.BroadcastAppend()

    nextStatus := rf.status
    for {
        select {

        case c := <-rf.stateCh:
            c <- State{Term: rf.currentTerm,
                       IsLeader: true}

        case voteWrapper := <-rf.voteCh:
            nextStatus = rf.HandleRequestVoteWrapper(voteWrapper)
            if nextStatus != Leader {
                return nextStatus
            }

        case commandRequest := <-rf.commandCh:
            rf.AppendCommand(commandRequest)

        case appendWrapper := <-rf.appendCh:
            nextStatus = rf.HandleAppendEntries(appendWrapper)
            if nextStatus != Leader {
                return nextStatus
            }

        case appendReplyWrapper := <-rf.appendReplyCh:
            server := appendReplyWrapper.Server
            //index := appendReplyWrapper.Index
            reply := appendReplyWrapper.Reply
            if reply.Success {
                updated := false
                if rf.matchIndex[server] < reply.MatchIndex {
                    updated = true
                    rf.matchIndex[server] = reply.MatchIndex
                }
                if rf.nextIndex[server] < reply.MatchIndex + 1 {
                    rf.nextIndex[server] = reply.MatchIndex + 1
                }
                if updated {
                    var matches []int
                    for i := 0; i < len(rf.peers); i++ {
                        if i != rf.me {
                            matches = append(matches, rf.matchIndex[i])
                        }
                    }
                    sort.Ints(matches)
                    newCommitIndex := matches[len(rf.peers)-(len(rf.peers)/2+1)]

                    if newCommitIndex > rf.commitIndex &&
                       rf.LogEntry(newCommitIndex).Term == rf.currentTerm { // Figure 8
                        rf.Log("sorted matches: %v, newCommandIndex:%d\n", matches, newCommitIndex)
                        rf.persist()

                        for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
                            msg := ApplyMsg{CommandValid: true,
                                            Command: rf.LogEntry(i).Command,
                                            CommandIndex: i}
                            rf.Log("apply msg: %d %v\n", msg.CommandIndex, msg.Command)
                            rf.applyCh <- msg
                        }
                        rf.commitIndex = newCommitIndex
                        rf.lastApplied = rf.commitIndex
                        //rf.BroadcastAppend()
                    }
                }
            } else {
                if reply.NextIndexHint > 0 {
                    rf.nextIndex[server] = Min(rf.nextIndex[server],
                                               Max(1, reply.NextIndexHint))
                } else {
                    rf.nextIndex[server] = rf.matchIndex[server] + 1
                }
                rf.SendAppend(server)
            }

        case heartbeatReplyWrapper := <-rf.heartbeatReplyCh:
            server := heartbeatReplyWrapper.Server
            reply := heartbeatReplyWrapper.Reply

            rf.Log("HBresp %d <= %d {matchIndex:%d, nextHint:%d, succ:%v}\n",
                    rf.me, server, reply.MatchIndex, reply.NextIndexHint, reply.Success)

            if reply.Success == true {
                rf.matchIndex[server] = reply.MatchIndex
                rf.nextIndex[server] = rf.matchIndex[server] + 1

                if rf.matchIndex[server] < rf.LastLogIndex() {
                    rf.SendAppend(server)
                }

            } else {
                if reply.NextIndexHint > 0 {
                    rf.nextIndex[server] = Min(rf.nextIndex[server],
                                               Max(1, reply.NextIndexHint))
                } else {
                    rf.nextIndex[server] = rf.matchIndex[server] + 1
                }
                rf.SendAppend(server)
            }

        case voteWrapper := <-rf.voteCh:
            nextStatus = rf.HandleRequestVoteWrapper(voteWrapper)
            if nextStatus != Leader {
                return nextStatus
            }

        case <-ticker.C:
            rf.heartbeatTimeout -= kTick
            if rf.heartbeatTimeout <= 0 {
                rf.heartbeatTimeout = kHeartbeatTimeout
                rf.BroadcastHeartbeat()
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
    rf.debugOn.Set(true)
    //rf.debugOn.Set(false)
    rf.currentTerm = 0
    rf.votedFor = None
    rf.commitIndex = 0
    rf.lastApplied = 0
    rf.status = Follower
    rf.electionTimeout = generateElectionTimeout()
    rf.voteCh = make(chan RequestVoteWrapper)
    rf.appendCh = make(chan AppendEntriesWrapper)
    rf.appendReplyCh = make(chan AppendEntriesReplyWrapper)
    rf.heartbeatReplyCh = make(chan AppendEntriesReplyWrapper)
    rf.commandCh = make(chan CommandRequest)
    rf.stateCh = make(chan chan State)
    rf.nextIndex = make([]int, len(peers))
    rf.matchIndex = make([]int, len(peers))

    nextStatus := Follower

    go func () {
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
    }()

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())           // FIXME: not thread-safe

    return rf
}

