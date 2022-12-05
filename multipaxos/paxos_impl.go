package paxos

import (
	"sync"
	"time"

	"cos518/proj/common"
)

//
// additions to Paxos state.
//
type PaxosImpl struct {
	Slots        map[int]*PaxosSlot
	Highest_slot int
	Lowest_slot  int
	Done         []int
	View         int // view number
	Miss_count   int
	Leader_dead  bool
}

type Ballot struct {
	Round int // round number
	N     int // ballot number
}

type PaxosSlot struct {
	N         int64
	Highest_N int64
	Np        int64
	Na        int64
	Va        interface{}
	mu        sync.Mutex
	mu_       sync.Mutex
	Value     interface{}
	Status    Fate
}

//
// your px.impl.* initializations here.
//
func (px *Paxos) initImpl() {
	px.impl.View = 1
	px.impl.Highest_slot = -1
	px.impl.Lowest_slot = 0
	px.impl.Miss_count = 0
	px.impl.Leader_dead = false
	px.impl.Slots = make(map[int]*PaxosSlot)
	for i := 0; i < len(px.peers); i++ {
		px.impl.Done = append(px.impl.Done, -1)
	}
	go func() {
		for {
			px.tick()
			time.Sleep(common.PingInterval)
		}
	}()

	go func() {
		for {
			px.check_heartbeart()
			time.Sleep(common.PingInterval)
		}
	}()
}

func (px *Paxos) check_heartbeart() {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.impl.Miss_count++
	// fmt.Printf("i am node %d and my view is %d: my leader is missing for %d \n", px.me, px.impl.View, px.impl.Miss_count)
	if px.impl.Miss_count > 5 {
		// px.impl.View += 1
		if !px.impl.Leader_dead && (px.impl.View+1)%len(px.peers) == px.me {
			// fmt.Printf("i am node %d and my view is %d: my leader is dead and i am prepering \n", px.me, px.impl.View)
			go px.prepare()
		}
		px.impl.Miss_count = 0
		px.impl.Leader_dead = true
	}
}

func (px *Paxos) prepare() {
	for {
		majority_count := 0
		reject_count := 0
		highest_view := int64(-1)
		for i, peer := range px.peers {
			px.mu.Lock()
			args := &PrepareArgs{int64(px.impl.View + 1), px.me}
			px.mu.Unlock()
			reply := &PrepareReply{}
			ok := true
			if i != px.me {
				ok = common.Call(peer, "Paxos.Prepare", args, reply)
			}
			if ok {
				if reply.Status == Reject {
					reject_count++
					if reply.View > highest_view {
						highest_view = reply.View
					}
				} else {
					majority_count++
				}
			}
		}

		px.mu.Lock()
		// fmt.Println("majority_count", majority_count)
		if reject_count > 0 {
			px.impl.View = int(highest_view)
			px.impl.Leader_dead = false
			// fmt.Printf("i am node %d and my new view is %d: my leader is alive \n", px.me, px.impl.View%len(px.peers))
			px.mu.Unlock()
			return
		}
		// fmt.Printf("i am node %d and my view is %d: i have %d majority \n", px.me, px.impl.View, majority_count)
		if majority_count+1 > len(px.peers)/2 {
			if highest_view <= int64(px.impl.View+1) {
				px.impl.Leader_dead = false
				px.impl.View += 1
				px.mu.Unlock()
				return
			} else {
				px.impl.View = int(highest_view)
				px.impl.Leader_dead = false
				px.mu.Unlock()
				return
			}
		}

		px.mu.Unlock()
		time.Sleep(time.Duration(common.Nrand()%100) * time.Millisecond)
	}
}

func (px *Paxos) tick() {
	px.mu.Lock()
	if px.impl.View%len(px.peers) != px.me {
		px.mu.Unlock()
		return
	}
	slots := make(map[int]*PaxosSlot)
	for k, v := range px.impl.Slots {
		if v.Status == Decided {
			decided_value := v.Value
			slot := initSlot(int64(px.me))
			slot.Value = decided_value
			slots[k] = slot
		}
	}
	px.mu.Unlock()
	if px.impl.View%len(px.peers) == px.me {
		for i, peer := range px.peers {
			px.mu.Lock()
			args := &HeartBeatArgs{px.me, px.impl.View, px.impl.Done, slots}
			px.mu.Unlock()
			reply := &HeartBeatReply{}
			if i != px.me {
				common.Call(peer, "Paxos.Tick", args, reply)
			} else {
				px.Tick(args, reply)
			}
		}
	}
}

func initSlot(me int64) *PaxosSlot {
	slot := &PaxosSlot{}
	slot.Status = Pending
	slot.Value = nil
	slot.Np = -1
	slot.Na = -1
	slot.Va = nil
	slot.Highest_N = me
	slot.N = 0
	return slot
}

func (px *Paxos) addSlots(seq int) *PaxosSlot {
	if _, exist := px.impl.Slots[seq]; !exist {
		px.impl.Slots[seq] = initSlot(int64(px.me))
	}
	return px.impl.Slots[seq]
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq < px.impl.Lowest_slot {
		return
	}
	// fmt.Printf("i am node %d and my view is %d: i am starting seq %d with %v\n", px.me, px.impl.View, seq, v)
	slot := px.addSlots(seq)
	if slot.Status != Decided {
		if px.impl.View%len(px.peers) == px.me {
			if !px.impl.Leader_dead {
				go px.StartOnNewSlot(seq, v, slot)
			}
		} else {
			// fmt.Printf("i am node %d and my view is %d: i am forwarding to the leader \n", px.me, px.impl.View)
			args := &ForwardLeaderArgs{seq, v}
			reply := &ForwardLeaderStartReply{}
			go common.Call(px.peers[px.impl.View%len(px.peers)], "Paxos.ForwardLeader", args, reply)
		}
	}
}

func (px *Paxos) StartOnNewSlot(seq int, v interface{}, slot *PaxosSlot) {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.Status == Decided || slot.Status == Forgotten {
		return
	}
	for {
		if slot.Status == Decided || px.isdead() {
			break
		}
		slot.N = int64(px.impl.View)
		majority_count := 0
		reject_count := 0
		highest_view := int64(-1)
		highest_va := v
		var decidedV interface{}
		isDecidedAcc := false
		// accept phase
		majority_count = 0
		reject_count = 0
		for i, peer := range px.peers {
			px.mu.Lock()
			args := &AcceptArgs{seq, slot.N, highest_va, px.me, px.impl.Done[px.me]}
			px.mu.Unlock()
			reply := &AcceptReply{}
			ok := true
			if i == px.me {
				if px.Accept(args, reply) != nil {
					ok = false
				}
			} else {
				ok = common.Call(peer, "Paxos.Accept", args, reply)
			}
			if ok {
				px.Forget(i, reply.LastestDone)
				if reply.Status == OK {
					majority_count += 1
				} else {
					reject_count += 1
					if int64(reply.View) > highest_view {
						highest_view = int64(reply.View)
					}
				}
				if reply.LastestDone >= seq {
					isDecidedAcc = true
					decidedV = reply.V
					break
				}
			}
			if reject_count > len(px.peers)/2 || majority_count > len(px.peers)/2 {
				break
			}
		}

		px.mu.Lock()
		// fmt.Printf("i am node %d and my view is %d: i am starting seq %d and my majority count is %d and my reject count is %d \n", px.me, px.impl.View, seq, majority_count, reject_count)
		if highest_view > int64(px.impl.View) {
			px.impl.View = int(highest_view)
			px.impl.Leader_dead = false
			px.mu.Unlock()
			return
		}
		px.mu.Unlock()

		if majority_count <= len(px.peers)/2 && !isDecidedAcc {
			slot.mu.Unlock()
			time.Sleep(time.Duration(common.Nrand()%100) * time.Millisecond)
			slot.mu.Lock()
			continue
		}

		if isDecidedAcc {
			highest_va = decidedV
		}
		// learn phase
		for i, peer := range px.peers {
			px.mu.Lock()
			args := &DecidedArgs{seq, highest_va, px.me, px.impl.Done[px.me]}
			px.mu.Unlock()
			reply := &DecidedReply{}
			ok := true
			if i == px.me {
				if px.Learn(args, reply) != nil {
					ok = false
				}
			} else {
				ok = common.Call(peer, "Paxos.Learn", args, reply)
			}
			if ok {
				px.Forget(i, reply.LastestDone)
			}
		}
		// fmt.Printf("i am node %d and my view is %d: i started seq %d and I passed \n", px.me, px.impl.View, seq)
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.impl.Done[px.me] = seq
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.impl.Highest_slot
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peer's Min does not reflect another peer's Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers' Min()s will not increase
// even if all reachable peers call Done(). The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefore cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.impl.Lowest_slot
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so, what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq-px.impl.Lowest_slot < 0 {
		return Forgotten, nil
	}
	slot := px.addSlots(seq)
	return slot.Status, slot.Value
}
