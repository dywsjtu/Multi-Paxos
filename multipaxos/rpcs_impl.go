package paxos

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

type PrepareArgs struct {
	View int64 // view number
	Id   int
}

type PrepareReply struct {
	View   int64 // view number
	Status string
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
}

func (px *Paxos) Tick(args *HeartBeatArgs, replys *HeartBeatReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	if px.impl.View > args.View {
		fmt.Printf("px %d view %d: got view %d from peer %d ... \n", px.me, px.impl.View, args.View, args.Id)
		return errors.New("your view is lower than mine")
	} else if px.me == args.Id {
		px.impl.Miss_count = 0
	}

	px.impl.Done = args.Done
	px.impl.Miss_count = 0

	for k, v := range args.Slots {
		slot := px.addSlots(k)
		slot.mu_.Lock()
		if slot.Status != Decided {
			slot.Status = Decided
			slot.Value = v.Value
		}
		slot.mu_.Unlock()
	}

	go px.Forget(args.Id, args.Done[args.Id])
	return nil
}

func (px *Paxos) ForwardLeader(args *ForwardLeaderArgs, reply *ForwardLeaderStartReply) error {
	px.Start(args.Seq, args.V)
	return nil
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	// fmt.Printf("px %d view %d: got prepare from peer %d with view %d... \n", px.me, px.impl.View, args.Id, args.View)
	if args.View < int64(px.impl.View) {
		reply.Status = Reject
		reply.View = int64(px.impl.View)
	} else {
		reply.Status = OK
		px.impl.View = int(args.View)
		reply.View = int64(px.impl.View)
	}
	// fmt.Printf("px %d view %d: accept prepare from peer %d with view %d... \n", px.me, px.impl.View, args.Id, args.View)
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
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
	reply.View = slot.Np
	if args.N >= slot.Np {
		slot.Np = args.N
		slot.Na = args.N
		slot.Va = args.V
		reply.Status = OK
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
