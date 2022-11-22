package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Impl  PutAppendArgsImpl
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Impl GetArgsImpl
}

type GetReply struct {
	Err   Err
	Value string
}
