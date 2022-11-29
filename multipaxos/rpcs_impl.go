package paxos

import (
	"errors"
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
	N           int64
	V           interface{}
	Me          int
	LastestDone int
}

type AcceptReply struct {
	Status      string
	LastestDone int
	V           interface{}
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
		reply.Highest_N = slot.Np
	}
	if slot.Status == Decided {
		reply.V = slot.Value
	} else {
		reply.V = nil
	}
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
