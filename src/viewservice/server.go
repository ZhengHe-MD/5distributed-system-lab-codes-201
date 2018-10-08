package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	pings    map[string]time.Time
	currView View
	nextView View
	lastAck  uint
	idles    []string
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	// debug
	// record ping
	vs.pings[args.Me] = time.Now()

	if vs.currView.Viewnum == 0 {
		vs.currView.Primary = args.Me
		vs.currView.Viewnum = 1
		vs.lastAck = 0
		// NOTE: assignment is a copy
		vs.nextView = vs.currView
		vs.logState()
	} else {
		switch args.Me {
		case vs.currView.Primary:
			if args.Viewnum == 0 {
				vs.currView = View{
					Viewnum: vs.currView.Viewnum + 1,
					Primary: vs.currView.Backup,
					Backup:  "",
				}

				vs.idles = append(vs.idles, args.Me)

				if len(vs.idles) > 0 {
					vs.currView.Backup, vs.idles = vs.idles[0], vs.idles[1:]
				}

				vs.nextView = vs.currView
				vs.logState()
			}

			if args.Viewnum == vs.currView.Viewnum {
				vs.lastAck = args.Viewnum
				if vs.currView != vs.nextView {
					if vs.nextView.Backup == "" && len(vs.idles) > 0 {
						vs.nextView.Backup, vs.idles = vs.idles[0], vs.idles[1:]
					}
					vs.currView = vs.nextView
				}
				vs.logState()
			}
		case vs.currView.Backup:
			// do nothing
		default:
			if vs.currView.Backup == "" && vs.currView.Primary != ""{
				if vs.lastAck == vs.currView.Viewnum {
					vs.currView.Backup = args.Me
					vs.currView.Viewnum += 1
					vs.nextView = vs.currView
					vs.logState()
				} else {
					vs.nextView.Backup = args.Me
					vs.nextView.Viewnum += 1
				}
			} else {
				vs.addIdle(args.Me)
				vs.logState()
			}
		}
	}

	reply.View = vs.currView

	return nil
}

//
// log viewserver state
//
func (vs *ViewServer) logState() {
	log.Printf("[viewservice]: %v %v %v\n", vs.lastAck, vs.currView, vs.idles)
}

//
// safe add idle server
//
func (vs *ViewServer) addIdle(name string) {
	if !vs.hasIdle(name) {
		vs.idles = append(vs.idles, name)
	}
}

//
// check the new server is in idle
//
func (vs *ViewServer) hasIdle(name string) bool {
	res := false
	for _, idle := range vs.idles {
		if idle == name {
			res = true
			break
		}
	}
	return res
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.currView

	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	// TODO: promote the backup if the viewservice has missed
	// DeadPings pings from the primary
	// Your code here.

	// check primary dies
	primaryLastPing, ok1 := vs.pings[vs.currView.Primary]
	backupLastPing, ok2 := vs.pings[vs.currView.Backup]

	if ok1 {
		if time.Now().Sub(primaryLastPing) > DeadPings * PingInterval {
			newView := View{
				Viewnum: vs.currView.Viewnum + 1,
				Primary: vs.currView.Backup,
				Backup:  "",
			}

			if vs.lastAck == vs.currView.Viewnum {
				vs.currView = newView
				if len(vs.idles) > 0 {
					vs.currView.Backup, vs.idles = vs.idles[0], vs.idles[1:]
				}
				vs.nextView = vs.currView
				vs.logState()
			} else {
				vs.nextView = newView
				// TODO: deal with idles
			}
		}
	}

	if ok2 {
		if time.Now().Sub(backupLastPing) > DeadPings * PingInterval {
			if len(vs.idles) > 0 {
				vs.currView.Backup, vs.idles = vs.idles[0], vs.idles[1:]
			} else {
				vs.currView.Backup = ""
			}
			vs.currView.Viewnum += 1
			vs.nextView = vs.currView
			vs.logState()
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.pings = make(map[string]time.Time)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
