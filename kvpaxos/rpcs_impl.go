package kvpaxos

//
// additional state to include in arguments to PutAppend RPC.
// Field names must start with capital letters,
// otherwise RPC will break.
//
type PutAppendArgsImpl struct {
	ClientID    string
	OperationID int64
}

//
// additional state to include in arguments to Get RPC.
//
type GetArgsImpl struct {
	ClientID    string
	OperationID int64
}

//
// for new RPCs that you add, declare types for arguments and reply
//
