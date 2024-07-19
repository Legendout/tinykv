// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	randomElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	return nil
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.leaderTick()
	case StateCandidate:
		r.candidateTick()
	case StateFollower:
		r.followerTick()
	}
}

func (r *Raft) leaderTick() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		// MessageType_MsgBeat 属于内部消息，不需要经过 RawNode 处理
		r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgBeat})
	}
	//TODO 选举超时 判断心跳回应数量

	//TODO 3A 禅让机制
}
func (r *Raft) candidateTick() {
	r.electionElapsed++
	// 选举超时 发起选举
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		// MessageType_MsgHup 属于内部消息，也不需要经过 RawNode 处理
		r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
	}
}
func (r *Raft) followerTick() {
	r.electionElapsed++
	// 选举超时 发起选举
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		// MessageType_MsgHup 属于内部消息，也不需要经过 RawNode 处理
		r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).

	if term > r.Term {
		// 只有 Term > currentTerm 的时候才需要对 Vote 进行重置
		// 这样可以保证在一个任期内只会进行一次投票
		r.Vote = None
	}
	r.Term = term
	r.State = StateFollower
	r.Lead = lead
	r.electionElapsed = 0
	r.leadTransferee = None
	r.resetRandomizedElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate // 0. 更改自己的状态
	r.Term++                 // 1. 增加自己的任期
	r.Vote = r.id            // 2. 投票给自己
	r.votes[r.id] = true

	r.electionElapsed = 0 // 3. 重置超时选举计时器
	r.resetRandomizedElectionTimeout()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	//领导者应在其任期内提出noop条目
	r.State = StateLeader
	r.Lead = r.id
	//初始化 nextIndex 和 matchIndex
	for id := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1 // 初始化为 leader 的最后一条日志索引（后续出现冲突会往前移动）
		r.Prs[id].Match = 0                        // 初始化为 0 就可以了
	}
	//3A
	r.PendingConfIndex = r.initPendingConfIndex()
	// 成为 Leader 之后立马在日志中追加一条 noop 日志，这是因为
	// 在 Raft 论文中提交 Leader 永远不会通过计算副本的方式提交一个之前任期、并且已经被复制到大多数节点的日志
	// 通过追加一条当前任期的 noop 日志，可以快速的提交之前任期内所有被复制到大多数节点的日志
	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		//Follower 可以接收到的消息：
		//MsgHup、MsgRequestVote、MsgHeartBeat、MsgAppendEntry
		r.followerStep(m)
	case StateCandidate:
		//Candidate 可以接收到的消息：
		//MsgHup、MsgRequestVote、MsgRequestVoteResponse、MsgHeartBeat
		r.candidateStep(m)
	case StateLeader:
		//Leader 可以接收到的消息：
		//MsgBeat、MsgHeartBeatResponse、MsgRequestVote
		r.leaderStep(m)
	}
	return nil
}
func (r *Raft) followerStep(m pb.Message) {
	//Follower 可以接收到的消息：
	//MsgHup、MsgRequestVote、MsgHeartBeat、MsgAppendEntry
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		//Local Msg，用于请求节点开始选举，仅仅需要一个字段。
		//TODO MsgHup
		// 成为候选者，开始发起投票
		r.handleStartElection(m)
	case pb.MessageType_MsgBeat:
		//Local Msg，用于告知 Leader 该发送心跳了，仅仅需要一个字段。
		//TODO Follower No processing required
	case pb.MessageType_MsgPropose:
		//Local Msg，用于上层请求 propose 条目
		//TODO Follower No processing required
	case pb.MessageType_MsgAppend:
		//Common Msg，用于 Leader 给其他节点同步日志条目
		//TODO MsgAppendEntry
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		//Common Msg，用于节点告诉 Leader 日志同步是否成功，和 MsgAppend 对应
		//TODO Follower No processing required
	case pb.MessageType_MsgRequestVote:
		//Common Msg，用于 Candidate 请求投票
		// TODO MsgRequestVote
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		//Common Msg，用于节点告诉 Candidate 投票结果
		//TODO Follower No processing required
	case pb.MessageType_MsgSnapshot:
		//Common Msg，用于 Leader 将快照发送给其他节点
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		//Common Msg，即 Leader 发送的心跳。
		//不同于论文中使用空的追加日志 RPC 代表心跳，TinyKV 给心跳一个单独的 MsgType
		//TODO MsgHeartbeat
		// 接收心跳包，重置超时，称为跟随者，回发心跳包的resp
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		//Common Msg，即节点对心跳的回应
		//TODO Follower No processing required
	case pb.MessageType_MsgTransferLeader:
		//Local Msg，用于上层请求转移 Leader
		//TODO Follower No processing required
		// 非 leader 收到领导权禅让消息，需要转发给 leader
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		//Local Msg，节点收到后清空 r.electionElapsed，并即刻发起选举
		r.handleTimeoutNowRequest(m)
	}
}
func (r *Raft) candidateStep(m pb.Message) {
	//Candidate 可以接收到的消息：
	//MsgHup、MsgRequestVote、MsgRequestVoteResponse、MsgHeartBeat
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		//Local Msg，用于请求节点开始选举，仅仅需要一个字段。
		//TODO MsgHup
		// 成为候选者，开始发起投票
		r.handleStartElection(m)
	case pb.MessageType_MsgBeat:
		//Local Msg，用于告知 Leader 该发送心跳了，仅仅需要一个字段。
		//TODO Candidate No processing required
	case pb.MessageType_MsgPropose:
		//Local Msg，用于上层请求 propose 条目
		//TODO Candidate No processing required
	case pb.MessageType_MsgAppend:
		//Common Msg，用于 Leader 给其他节点同步日志条目
		//TODO MsgAppendEntry
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		//Common Msg，用于节点告诉 Leader 日志同步是否成功，和 MsgAppend 对应
		//TODO Candidate No processing required
	case pb.MessageType_MsgRequestVote:
		//Common Msg，用于 Candidate 请求投票
		// TODO MsgRequestVote
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		//Common Msg，用于节点告诉 Candidate 投票结果
		// TODO MsgRequestVoteResponse
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		//Common Msg，用于 Leader 将快照发送给其他节点
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		//Common Msg，即 Leader 发送的心跳。
		//不同于论文中使用空的追加日志 RPC 代表心跳，TinyKV 给心跳一个单独的 MsgType
		//TODO MsgHeartbeat
		// 接收心跳包，重置超时，称为跟随者，回发心跳包的resp
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		//Common Msg，即节点对心跳的回应
		//TODO Candidate No processing required
	case pb.MessageType_MsgTransferLeader:
		//Local Msg，用于上层请求转移 Leader
		//要求领导转移其领导权
		//TODO Candidate No processing required
		// 非 leader 收到领导权禅让消息，需要转发给 leader
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		//Local Msg，节点收到后清空 r.electionElapsed，并即刻发起选举
		//从领导发送到领导转移目标，以让传输目标立即超时并开始新的选择。
		r.handleTimeoutNowRequest(m)
	}
}
func (r *Raft) leaderStep(m pb.Message) {
	//Leader 可以接收到的消息：
	//MsgBeat、MsgHeartBeatResponse、MsgRequestVote、MsgPropose、MsgAppendResponse、MsgAppend
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		//Local Msg，用于请求节点开始选举，仅仅需要一个字段。
		//TODO Leader No processing required
	case pb.MessageType_MsgBeat:
		//Local Msg，用于告知 Leader 该发送心跳了，仅仅需要一个字段。
		//TODO MsgBeat
		r.broadcastHeartBeat()
	case pb.MessageType_MsgPropose:
		//Local Msg，用于上层请求 propose 条目
		//TODO MsgPropose
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:
		//Common Msg，用于 Leader 给其他节点同步日志条目
		//TODO 网络分区的情况，也是要的
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		//Common Msg，用于节点告诉 Leader 日志同步是否成功，和 MsgAppend 对应
		//TODO MsgAppendResponse
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		//Common Msg，用于 Candidate 请求投票
		// TODO MsgRequestVote
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		//Common Msg，用于节点告诉 Candidate 投票结果
		//TODO Leader No processing required
	case pb.MessageType_MsgSnapshot:
		//Common Msg，用于 Leader 将快照发送给其他节点
		//TODO Leader No processing required
	case pb.MessageType_MsgHeartbeat:
		//Common Msg，即 Leader 发送的心跳。
		//不同于论文中使用空的追加日志 RPC 代表心跳，TinyKV 给心跳一个单独的 MsgType
		//TODO Leader No processing required
	case pb.MessageType_MsgHeartbeatResponse:
		//Common Msg，即节点对心跳的回应
		//TODO MsgHeartBeatResponse
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
		//Local Msg，用于上层请求转移 Leader
		//要求领导转移其领导权
		//TODO project3
		r.handleTransferLeader(m)

	case pb.MessageType_MsgTimeoutNow:
		//Local Msg，节点收到后清空 r.electionElapsed，并即刻发起选举
		//从领导发送到领导转移目标，以让传输目标立即超时并开始新的选择。
		//TODO project3
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
