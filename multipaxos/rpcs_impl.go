package multipaxos

import (
	"errors"
	"fmt"
)

// In all data types that represent RPC arguments/reply, field names
// must start with capital letters, otherwise RPC will break.

const (
	OK     = "OK"
	Reject = "Reject"
	Done   = "Done"
)

type Response string

type ElectArgs struct {
	View int64 // view number
	Id   int
}

type ElectReply struct {
	View                 int64  // view number
	Status               string // OK or Reject
	Highest_accepted_seq int    // highest accepted sequence number
}

type PrepareArgs struct {
	Seq         int
	N           int64
	LastestDone int
	Me          int
}

type PrepareReply struct {
	Status      string
	Na          int64
	Va          interface{}
	Highest_N   int64
	LastestDone int
	V           interface{}
}
type AcceptArgs struct {
	Seq         int
	N           int64 // view number
	V           interface{}
	Me          int
	LastestDone int
}

type AcceptReply struct {
	Status      string
	LastestDone int
	V           interface{}
	View        int64
}

type DecidedArgs struct {
	Seq         int
	V           interface{}
	Me          int
	LastestDone int
}

type DecidedReply struct {
	LastestDone int
}

type ForwardLeaderArgs struct {
	Seq int
	V   interface{}
}

type ForwardLeaderStartReply struct {
	Status string
}

type HeartBeatArgs struct {
	Id    int
	View  int
	Done  []int
	Slots map[int]*PaxosSlot
}

type HeartBeatReply struct {
	Status string
	Slots  map[int]*PaxosSlot
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	if args.Seq < px.impl.Lowest_slot {
		px.mu.Unlock()
		return errors.New("this slot has been garbage collected")
	}
	reply.LastestDone = px.impl.Done[px.me]
	slot := px.addSlots(args.Seq)
	px.mu.Unlock()
	slot.mu_.Lock()
	defer slot.mu_.Unlock()
	if args.N > slot.Np {
		reply.Status = OK
		slot.Np = args.N
		reply.Na = slot.Na
		reply.Va = slot.Va
		reply.Highest_N = args.N
	} else {
		reply.Status = Reject
		reply.Na = slot.Na
		reply.Va = slot.Va
		reply.Highest_N = slot.Np
	}
	if slot.Status == Decided {
		reply.V = slot.Value
	} else {
		reply.V = nil
	}
	return nil
}

func (px *Paxos) Tick(args *HeartBeatArgs, reply *HeartBeatReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.me == args.Id {
		px.impl.Miss_count = 0
		return nil
	}

	if px.impl.View > args.View {
		return errors.New("your view is lower than mine")
	} else if px.impl.View < args.View {
		px.impl.Miss_count = 5
		fmt.Printf("your view is higher than mine\n")
	} else {
		px.impl.Miss_count = 0
	}

	px.impl.Done = args.Done

	reply_slots := make(map[int]*PaxosSlot)

	for k, v := range px.impl.Slots {
		if v.Status == Decided {
			if _, ok := args.Slots[k]; !ok {
				decided_value := v.Value
				slot := initSlot(px.impl.View)
				slot.Value = decided_value
				slot.Status = Decided
				reply_slots[k] = slot
			}
		}
	}

	for k, v := range args.Slots {
		slot := px.addSlots(k)
		slot.mu_.Lock()
		if slot.Status != Decided {
			slot.Status = Decided
			slot.Value = v.Value
		}
		slot.mu_.Unlock()
	}
	reply.Status = OK
	reply.Slots = reply_slots

	go px.Forget(args.Id, args.Done[args.Id])
	return nil
}

func (px *Paxos) ForwardLeader(args *ForwardLeaderArgs, reply *ForwardLeaderStartReply) error {
	px.Start(args.Seq, args.V)
	return nil
}

func (px *Paxos) Elect(args *ElectArgs, reply *ElectReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	if args.View > int64(px.impl.View) {
		reply.Status = OK
		px.impl.View = int(args.View)
		reply.View = int64(px.impl.View)
		reply.Highest_accepted_seq = px.impl.Highest_accepted_seq
	} else {
		reply.Status = Reject
		reply.View = int64(px.impl.View)
	}
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	if args.Seq < px.impl.Lowest_slot {
		px.mu.Unlock()
		return errors.New("this slot has been garbage collected")
	}
	if args.N < int64(px.impl.View) {
		px.mu.Unlock()
		return errors.New("you have lower view than me")
	}
	reply.LastestDone = px.impl.Done[px.me]
	slot := px.addSlots(args.Seq)
	if args.N > int64(px.impl.View) {
		px.impl.View = int(args.N)
	}
	px.mu.Unlock()
	slot.mu_.Lock()
	defer slot.mu_.Unlock()
	reply.View = slot.Na
	reply.V = slot.Va
	if args.N >= slot.Na {
		slot.Na = args.N
		slot.Va = args.V
		reply.Status = OK
		px.mu.Lock()
		if args.Seq > px.impl.Highest_accepted_seq {
			px.impl.Highest_accepted_seq = args.Seq
		}
		px.mu.Unlock()
	} else {
		reply.Status = Reject
	}
	if slot.Status == Decided {
		reply.V = slot.Value
	} else {
		reply.V = nil
	}
	return nil
}

func (px *Paxos) Learn(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	if args.Seq < px.impl.Lowest_slot {
		px.mu.Unlock()
		return errors.New("this slot has been garbage collected")
	}
	reply.LastestDone = px.impl.Done[px.me]
	slot := px.addSlots(args.Seq)
	if args.Seq > px.impl.Highest_slot {
		px.impl.Highest_slot = args.Seq
	}
	px.mu.Unlock()
	slot.mu_.Lock()
	defer slot.mu_.Unlock()
	if slot.Status != Decided {
		slot.Status = Decided
		slot.Value = args.V
	}
	return nil
}

func (px *Paxos) Forget(peer int, peerDone int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if peerDone > px.impl.Done[peer] {
		px.impl.Done[peer] = peerDone
	}
	min := px.impl.Done[0]
	for _, d := range px.impl.Done {
		if d < min {
			min = d
		}
	}
	offset := min - px.impl.Lowest_slot + 1
	if offset > 0 {
		px.impl.Lowest_slot += offset
	}
	for i := range px.impl.Slots {
		if i < px.impl.Lowest_slot {
			delete(px.impl.Slots, i)
		}
	}
}

//
// add RPC handlers for any RPCs you introduce.
//
