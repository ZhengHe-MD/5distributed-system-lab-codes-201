package pbservice

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	ErrServerInit  = "ErrServerInit"
	ErrNotReady    = "ErrNotReady"
	Primary 	   = "Primary"
	Backup 		   = "Backup"
	Put 		   = "Put"
	Append 		   = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op 	  string
	Id    int64
	No    uint
	Isfp  bool 		// is from primary, otherwise is targeted at primary
	Time time.Time
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.
type GetDBArgs struct {

}

type GetDBReply struct {
	Err	Err
	Time time.Time
	DBState DBState
}

type DBState struct {
	Data map[string]string
	History map[int64]struct{}
	Plastno uint
}

