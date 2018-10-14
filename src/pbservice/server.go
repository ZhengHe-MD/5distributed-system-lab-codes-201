package pbservice

import (
	"net"
)
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	vshost 	   string
	view       viewservice.View
	kvs 	   map[string]string
	history    map[int64]struct{}
	// when me is primary, pbc
	isp  	   bool
	isb        bool
	// for primary
	backupck   *Clerk
	plastno    uint // record last backup write op number
	// for backup
	primaryck  *Clerk
	opscachemap map[uint]PutAppendArgs // cache unordered ops
	blastno    uint // record last backup write op number
	bready 	   bool
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.isp && !pb.isb {
		reply.Err = ErrIdle
		reply.Value = ""
		return nil
	}

	if pb.isb && !pb.bready {
		reply.Err = ErrBackupNotReady
		reply.Value = ""
		return nil
	}

	if pb.isp {
		// NOTE: if this pb server thought it should be the Primary
		// but not in reality, this pb server should reject the request
		if args.Isfp && args.Viewnum != pb.view.Viewnum {
			reply.Err = ErrWrongView
			reply.Value = ""
			return nil
		}

		if pb.view.Backup != "" {
			bargs := GetArgs{ Key: args.Key, Isfp: true, Viewnum: pb.view.Viewnum }
			breply := GetReply{}
			ok := pb.backupck.BackupGet(bargs, &breply, pb.view.Backup)

			if ok {
				if breply.Err == ErrWrongView {
					reply.Err = ErrWrongView
					reply.Value = ""
					return nil
				}
			} else {
				reply.Err = ErrLostBackup
				reply.Value = ""
				return nil
			}
		}
	}

	v, ok := pb.kvs[args.Key]

	if !ok {
		reply.Err = ErrNoKey
	}

	reply.Value = v
	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// backup is not initialized, discard all requests
	if pb.isb && !pb.bready {
		pb.opscachemap[args.No] = *args
		return nil
	}

	if pb.isp && args.Isfp {
		reply.Err = ErrWrongServer
		return nil
	}

	// Your code here.
	ak, av, op, id, no := args.Key, args.Value, args.Op, args.Id, args.No

	// check history
	_, hok := pb.history[id]

	if hok {
		return nil
	}

	v, ok := pb.kvs[ak]
	switch op {
	case "Put":
		// NOTE: backup write should be put before primary write
		// because if Primary is killed after it accepted a request
		// and before it forward it to Backup, that could cause
		// inconsistency
		if pb.isp {
			if pb.backupck != nil {
				pb.backupck.BackupPut(ak, av, pb.plastno + 1, id)
			}
			pb.kvs[ak] = "" + av
			pb.plastno += 1
		}

		if pb.isb {
			if pb.blastno + 1 != no {
				pb.opscachemap[no] = *args
			} else {
				pb.kvs[ak] = "" + av
				pb.blastno += 1
			}
		}
	case "Append":
		// NOTE: backup write should be put before primary write
		if pb.isp {
			if pb.backupck != nil {
				pb.backupck.BackupAppend(ak, av, pb.plastno + 1, id)
			}

			if ok {
				pb.kvs[ak] = v + av
			} else {
				pb.kvs[ak] = av
			}
			pb.plastno += 1
		}

		if pb.isb {
			if pb.blastno + 1 != no {
				pb.opscachemap[no] = *args
			} else {
				if ok {
					pb.kvs[ak] = v + av
				} else {
					pb.kvs[ak] = av
				}
				pb.blastno += 1
			}
		}
	}

	// log to history
	pb.history[id] = struct{}{}

	return nil
}

//
// NOTE:
// some newly started Backup call GetDB to transfer state
// it can be sure that when GetDB is called, this pb server
// must be the primary, no matter what its current view is
//
// Assumptions: only Primary will enter this code block
//
func (pb *PBServer) GetDB(args *GetDBArgs, reply *GetDBReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	reply.Time = time.Now()
	if pb.isp {
		// NOTE: should manual copy kvs and history map, otherwise
		// backup server will get the references, which can cause
		// unintentional concurrent read, write, iteration to map
		kvscp := make(map[string]string)

		for k, v := range pb.kvs {
			kvscp[k] = v
		}

		historycp := make(map[int64]struct{})

		for k, v := range pb.history {
			historycp[k] = v
		}

		reply.DBState = DBState{
			Data:    kvscp,
			History: historycp,
			Plastno: pb.plastno,
		}
		reply.Err = ""
	} else {
		reply.DBState = DBState{
			Data:    nil,
			History: nil,
			Plastno: 0,
		}
		reply.Err = ErrWrongServer
	}
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	// Your code here.
	view, err := pb.vs.Ping(pb.view.Viewnum)
	// NOTE: viewservice failed
	// we keep the previous view
	if err != nil {
		return
	}

	// view doesn't change, nothing to do
	if pb.view == view {
		return
	}

	// if view change
	pb.mu.Lock()
	defer pb.mu.Unlock()

	pview := pb.view
	pb.view = view

	// if pb is Primary in the new view
	pb.isp = pb.me == view.Primary
	pb.isb = pb.me == view.Backup

	changed := false
	if pb.isp && pview.Primary != pb.me {
		changed = true
	}

	if pb.isb && pview.Backup != pb.me {
		changed = true
	}

	// update clerk
	if pb.isp {
		pb.primaryck = nil
		if view.Backup != "" {
			pb.backupck = MakeClerk(pb.vshost, view.Backup)
		}
		pb.bready = false
		pb.opscachemap = make(map[uint]PutAppendArgs)
		if changed {
			pb.plastno = pb.blastno
		}
	}

	if pb.isb {
		pb.backupck = nil
		if view.Primary != "" {
			pb.primaryck = MakeClerk(pb.vshost, view.Primary)
		}
		pb.bready = false
		pb.opscachemap = make(map[uint]PutAppendArgs)
	}

	if pb.isb {
		go func() {
			// 1. getdb data
			var ok bool
			reply := GetDBReply{}
			for {
				// NOTE:
				// 在 {A B} [C] => {B C} [] 时
				// B, C 的 primaryck 有可能为 nil
				// 按 Lab 的设定，这里只可能是 C 为 nil

				// NOTE:
				// 使用 tmpreply 是因为可能出现第一次 GetDB 发送后，被执行但是服务器未正确返回 reply，
				// 重试之后，又发送一次，被执行且服务器正确返回，但是前者的写入时间在后者的写入时间之后，
				// 这时可能出现 break 以后 reply 中的 kvs 和 history 都是 nil
				tmpargs := GetDBArgs{}
				tmpreply := GetDBReply{}

				if pb.primaryck != nil {
					ok = pb.primaryck.GetDB(tmpargs, &tmpreply)

					if ok && tmpreply.Err == "" {
						reply = tmpreply
						break
					} else {
						pb.primaryck.updateEndpoints()
					}
				}

				time.Sleep(time.Millisecond * 20)
			}

			pb.mu.Lock()
			defer pb.mu.Unlock()
			// 2. update history, kvs and blastno
			pb.history = reply.DBState.History
			pb.kvs = reply.DBState.Data
			pb.blastno = reply.DBState.Plastno
			pb.bready = true
		}()

		// replay opscachemap
		if pb.bready {
			for {
				args, ok := pb.opscachemap[pb.blastno+1]

				if !ok {
					break
				} else {
					// check history
					ak, av, op, id := args.Key, args.Value, args.Op, args.Id

					_, hok := pb.history[id]

					if hok {
						continue
					}

					v, ok := pb.kvs[ak]
					switch op {
					case Put:
						pb.kvs[ak] = "" + av
					case Append:
						if ok {
							pb.kvs[ak] = v + av
						} else {
							pb.kvs[ak] = av
						}
					}
					pb.blastno += 1

					pb.history[id] = struct{}{}
				}
			}
		}
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.vshost = vshost
	pb.view = viewservice.View{
		Viewnum: 0,
		Primary: "",
		Backup:  "",
	}
	pb.kvs = make(map[string]string)
	pb.history = make(map[int64]struct{})
	pb.opscachemap = make(map[uint]PutAppendArgs)
	pb.backupck = nil
	pb.primaryck = nil
	pb.isb = false
	pb.isp = false
	pb.bready = false
	pb.plastno = 0
	pb.blastno = 0

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
