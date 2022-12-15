package kvpaxos

import (
	"errors"
	"fmt"
)

//
// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters,
// otherwise RPC will break.
//
type Op struct {
	Type        string
	Key         string
	Value       string
	OperationID int64
	ClientID    string
}

//
// additions to KVPaxos state
//
type KVPaxosImpl struct {
	data           map[string]string
	MostRecentDone map[string](int64)
	// isFinished map[int64]bool
}

//
// initialize kv.impl.*
//
func (kv *KVPaxos) InitImpl() {
	kv.impl.data = make(map[string]string)
	kv.impl.MostRecentDone = make(map[string](int64))
}

//
// Handler for Get RPCs
//
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, exist := kv.impl.MostRecentDone[args.Impl.ClientID]; !exist {
		kv.impl.MostRecentDone[args.Impl.ClientID] = -1
	}
	MostRecentDone := kv.impl.MostRecentDone[args.Impl.ClientID]
	if args.Impl.OperationID < MostRecentDone {
		return errors.New("expired operation")
	} else if args.Impl.OperationID == MostRecentDone {
		value, ok := kv.impl.data[args.Key]
		if ok {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Value = ""
			reply.Err = ErrNoKey
		}
	} else {
		operation := Op{"Get", args.Key, "", args.Impl.OperationID, args.Impl.ClientID}
		kv.rsm.AddOp(operation)
		value, ok := kv.impl.data[args.Key]
		if ok {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Value = ""
			reply.Err = ErrNoKey
		}
		// kv.impl.MostRecentDone[args.Impl.ClientID] = args.Impl.OperationID
	}
	return nil
}

//
// Handler for Put and Append RPCs
//
func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exist := kv.impl.MostRecentDone[args.Impl.ClientID]; !exist {
		kv.impl.MostRecentDone[args.Impl.ClientID] = -1
	}
	MostRecentDone := kv.impl.MostRecentDone[args.Impl.ClientID]
	if args.Impl.OperationID < MostRecentDone {
		return errors.New("expired operation")
	} else if args.Impl.OperationID == MostRecentDone {
		reply.Err = OK
		return nil
	} else {
		operation := Op{args.Op, args.Key, args.Value, args.Impl.OperationID, args.Impl.ClientID}
		kv.rsm.AddOp(operation)
		reply.Err = OK
	}
	return nil
}

//
// Execute operation encoded in decided value v and update local state
//
func (kv *KVPaxos) ApplyOp(v interface{}) {
	fmt.Printf("ApplyOp %v\n", v)
	operation := v.(Op)
	if operation.Type == "Put" {
		kv.impl.data[operation.Key] = operation.Value
		// fmt.Println("map:", kv.impl.data)
	} else if operation.Type == "Append" {
		kv.impl.data[operation.Key] += operation.Value
		// fmt.Println("map:", kv.impl.data)
	}
	kv.impl.MostRecentDone[operation.ClientID] = operation.OperationID
}
