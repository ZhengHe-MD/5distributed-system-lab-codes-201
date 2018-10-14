package pbservice

import (
	"log"
)
import "time"
import "viewservice"
import "net/rpc"
import "fmt"

import "crypto/rand"
import "math/big"


type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	psrv	string
	bsrv  	string
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.updateEndpoints()

	return ck
}

func (ck *Clerk) updateEndpoints() {
	view, ok := ck.vs.Get()

	if ok {
		ck.psrv = view.Primary
		ck.bsrv = view.Backup
	} else {
		//log.Printf("Clerk %v get service view failed, the whole service maybe down\n", ck)
	}
}

func (ck *Clerk) makeSureEndpointExist(endpoint string) {
	if endpoint == "" {
		ck.updateEndpoints()
	}
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// get srv string by endpoint name defined in pbservice/common.go
//
func (ck *Clerk) getSrv(endpoint string) string {
	switch endpoint {
	case Primary:
		ck.makeSureEndpointExist(ck.psrv)
		return ck.psrv
	case Backup:
		ck.makeSureEndpointExist(ck.bsrv)
		return ck.bsrv
	default:
		log.Fatalf("wrong endpoint name %v\n", endpoint)
		return ""
	}
}

func (ck *Clerk) GeneralGet(endpoint string, key string) string {
	args := GetArgs{
		Key: key,
	}

	for {
		reply := GetReply{}
		ok := call(ck.getSrv(endpoint), "PBServer.Get", args, &reply)

		if !ok {
			ck.updateEndpoints()
		} else {
			switch reply.Err {
			case ErrNoKey:
				return ""
			case ErrWrongView:
				ck.updateEndpoints()
			case "":
				return reply.Value
			}
		}

		time.Sleep(viewservice.PingInterval)
	}
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.
	return ck.GeneralGet(Primary, key)
}



//
// takes a server name, send a Put or Append RPC
//
func (ck *Clerk) GeneralPutAppend(endpoint string, key string, value string, op string) {
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Id:    nrand(),
		No:    0,  // doesn't matter
		Isfp:  false,
	}
	reply := PutAppendReply{}

	for {
		ok := call(ck.getSrv(endpoint), "PBServer.PutAppend", args, &reply)

		// TODO: 让 BackupPut 和 Append 使用单独的 GeneralPutAppend
		// 如果是 Backup PutAppend 请求，发现 Backup 发生变化则应该停止请求
		if !ok {
			ck.updateEndpoints()
		}

		if ok {
			break
		}

		time.Sleep(viewservice.PingInterval)
	}
}

//
// send a Put or Append RPC to primary server
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// Your code here.
	ck.GeneralPutAppend(Primary, key, value, op)
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) GeneralBackupPutAppend(endpoint string, key string, value string, op string, no uint, id int64) {
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Id:    id,
		No:    no,
		Isfp:  true,
	}
	reply := PutAppendReply{}

	for {
		ok := call(ck.getSrv(endpoint), "PBServer.PutAppend", args, &reply)

		// TODO: 让 BackupPut 和 Append 使用单独的 GeneralPutAppend
		// 如果是 Backup PutAppend 请求，发现 Backup 发生变化则应该停止请求
		if !ok {
			ck.updateEndpoints()
		}

		if ok {
			break
		}

		time.Sleep(viewservice.PingInterval)
	}
}

//
//
//
func (ck *Clerk) BackupPutAppend(key string, value string, op string, no uint, id int64) {
	ck.GeneralBackupPutAppend(Backup, key, value, op, no, id)
}

//
// tell the backup to update key's value
// must keep trying until it succeeds
//
func (ck *Clerk) BackupPut(key string, value string, no uint, id int64) {
	ck.BackupPutAppend(key, value, "Put", no, id)
}

//
// tell the backup to append to key's value
// must keep trying until it succeeds
//
func (ck *Clerk) BackupAppend(key string, value string, no uint, id int64) {
	ck.BackupPutAppend(key, value, "Append", no, id)
}

//
// get kvs data from primary
//
func (ck *Clerk) GetDB(args GetDBArgs, reply *GetDBReply) bool {
	return call(ck.getSrv(Primary), "PBServer.GetDB", args, reply)
}

func (ck *Clerk) BackupGet(args GetArgs, reply *GetReply, srv string) bool {
	return call(srv, "PBServer.Get", args, &reply)
}
