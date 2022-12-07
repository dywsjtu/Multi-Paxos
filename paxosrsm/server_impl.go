package paxosrsm

import (
	paxos "cos518/proj/multipaxos"
	// "cos518/proj/paxos"
	"time"
)

//
// additions to PaxosRSM state
//
type PaxosRSMImpl struct {
	LastestDone int
}

//
// initialize rsm.impl.*
//
func (rsm *PaxosRSM) InitRSMImpl() {
	rsm.impl.LastestDone = rsm.px.Min() - 1
}

//
// application invokes AddOp to submit a new operation to the replicated log
// AddOp returns only once value v has been decided for some Paxos instance
//
func (rsm *PaxosRSM) AddOp(v interface{}) {
	for {
		// fmt.Printf("start slot %v for value %v\n", rsm.impl.LastestDone+1, v)
		to := 1 * time.Millisecond
		rsm.px.Start(rsm.impl.LastestDone+1, v)
		var val interface{}
		var status paxos.Fate
		for {
			status, val = rsm.px.Status(rsm.impl.LastestDone + 1)
			if status == paxos.Decided {
				break
			}
			time.Sleep(to)
			if to < 1*time.Second {
				to *= 2
			}
		}
		// fmt.Printf("slot %v is decided and op is %v, v is %v\n", rsm.impl.LastestDone+1, val, v)
		rsm.applyOp(val)
		// fmt.Printf("op %v has been applied\n", val)
		rsm.px.Done(rsm.impl.LastestDone + 1)
		rsm.impl.LastestDone += 1
		if v == val {
			// fmt.Printf("AddOp finished in slot %v and v = val = %v\n", rsm.impl.LastestDone, v)
			break
		}
	}
}
