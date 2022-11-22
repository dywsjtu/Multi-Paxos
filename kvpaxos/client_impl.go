package kvpaxos

import (
	"strconv"
	"time"

	"cos518/proj/common"
)

//
// any additions to Clerk state
//
type ClerkImpl struct {
	ClientID    string
	OperationID int64
}

//
// initialize ck.impl state
//
func (ck *Clerk) InitImpl() {
	ck.impl.ClientID = strconv.Itoa(int(time.Now().UnixNano())) + "-" + strconv.Itoa(int(common.Nrand()))
	ck.impl.OperationID = 0
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{key, GetArgsImpl{ck.impl.ClientID, ck.impl.OperationID}}
	reply := &GetReply{}
	index := 0
	for {
		if common.Call(ck.servers[index%len(ck.servers)], "KVPaxos.Get", &args, &reply) {
			ck.impl.OperationID += 1
			return reply.Value
		}
		index += 1
	}
}

//
// shared by Put and Append; op is either "Put" or "Append"
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{key, value, op, PutAppendArgsImpl{ck.impl.ClientID, ck.impl.OperationID}}
	reply := &PutAppendReply{}
	index := 0
	for {
		if common.Call(ck.servers[index%len(ck.servers)], "KVPaxos.PutAppend", &args, &reply) {
			if reply.Err == OK {
				ck.impl.OperationID += 1
				return
			}
		}
		index += 1
	}

}
