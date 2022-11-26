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
	px.impl.Highest_slot = -1
	px.impl.Lowest_slot = 0
	px.impl.Slots = make(map[int]*PaxosSlot)
	for i := 0; i < len(px.peers); i++ {
		px.impl.Done = append(px.impl.Done, -1)
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
	slot := px.addSlots(seq)
	if slot.Status != Decided {
		go px.StartOnNewSlot(seq, v, slot)
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
		for {
			if slot.N > slot.Highest_N {
				break
			}
			slot.N += int64(len(px.peers))
		}
		majority_count := 0
		reject_count := 0
		highest_na := int64(-1)
		highest_va := v
		na_count_map := make(map[int64](int))
		//prepera phase
		isDecidedPrep := false
		var decidedV interface{}
		for i, peer := range px.peers {
			px.mu.Lock()
			args := &PrepareArgs{seq, slot.N, px.impl.Done[px.me], px.me}
			px.mu.Unlock()
			reply := &PrepareReply{}
			ok := true
			if i == px.me {
				if px.Prepare(args, reply) != nil {
					ok = false
				}
			} else {
				ok = common.Call(peer, "Paxos.Prepare", args, reply)
			}
			if ok {
				px.Forget(i, reply.LastestDone)
				if reply.Status == OK {
					majority_count += 1
					if reply.Na > highest_na {
						highest_na = reply.Na
						highest_va = reply.Va
						na_count_map[highest_na] += 1
					}
					// } else if reply.Na == highest_na && reply.Va == highest_va {
					// 	na_count_map[highest_na] += 1
					// }
				} else {
					reject_count += 1
					if slot.Highest_N < reply.Highest_N {
						slot.Highest_N = reply.Highest_N
					}
				}
				if reply.LastestDone >= seq {
					isDecidedPrep = true
					decidedV = reply.V
					break
				}
				if na_count_map[highest_na] > len(px.peers)/2 {
					isDecidedPrep = true
					decidedV = highest_va
					break
				}
			}
			if reject_count > len(px.peers)/2 || majority_count > len(px.peers)/2 || na_count_map[highest_na] > len(px.peers)/2 {
				break
			}
		}
		if majority_count <= len(px.peers)/2 && !isDecidedPrep {
			slot.mu.Unlock()
			time.Sleep(time.Duration(common.Nrand()%100) * time.Millisecond)
			slot.mu.Lock()
			continue
		}
		isDecidedAcc := false
		if !isDecidedPrep {
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
			if majority_count <= len(px.peers)/2 && !isDecidedAcc {
				slot.mu.Unlock()
				time.Sleep(time.Duration(common.Nrand()%100) * time.Millisecond)
				slot.mu.Lock()
				continue
			}
		}

		if isDecidedAcc || isDecidedPrep {
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
