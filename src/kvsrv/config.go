package kvsrv

import "6.5840/labrpc"
import "testing"
import "os"

//import "log"
import crand "crypto/rand"
import "math/big"
import "math/rand"
import "encoding/base64"
import "sync"
import "runtime"
import "fmt"
import "time"
import "sync/atomic"

const SERVERID = 0

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

type config struct {
	mu           sync.Mutex
	t            *testing.T
	net          *labrpc.Network
	kvserver     *KVServer
	endname      string // name of the server's sending ClientEnd
	clerks       map[*Clerk]string
	nextClientId int
	start        time.Time // time at which make_config() was called
	// begin()/end() statistics
	t0    time.Time // time at which test_test.go called cfg.begin()
	rpcs0 int       // rpcTotal() at start of test
	ops   int32     // number of clerk get/put/append method calls
}

func (cfg *config) checkTimeout() {
	// enforce a two minute real-time limit on each test
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}

func (cfg *config) cleanup() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.net.Cleanup()
	cfg.checkTimeout()
}

// Create a clerk with clerk specific server name.
func (cfg *config) makeClient() *Clerk {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// a fresh ClientEnds
	endname := randstring(20)
	end := cfg.net.MakeEnd(endname)
	cfg.net.Connect(endname, SERVERID)

	ck := MakeClerk(end)
	cfg.clerks[ck] = endname
	cfg.nextClientId++
	cfg.ConnectClientUnlocked(ck)
	return ck
}

func (cfg *config) deleteClient(ck *Clerk) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	v := cfg.clerks[ck]
	for i := 0; i < len(v); i++ {
		os.Remove(v)
	}
	cfg.net.DeleteEnd(v)
	delete(cfg.clerks, ck)
}

// caller should hold cfg.mu
func (cfg *config) ConnectClientUnlocked(ck *Clerk) {
	//log.Printf("ConnectClient %v\n", ck)
	endname := cfg.clerks[ck]
	cfg.net.Enable(endname, true)
}

func (cfg *config) ConnectClient(ck *Clerk) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.ConnectClientUnlocked(ck)
}

func (cfg *config) StartServer() {
	cfg.kvserver = StartKVServer()

	kvsvc := labrpc.MakeService(cfg.kvserver)
	srv := labrpc.MakeServer()
	srv.AddService(kvsvc)
	cfg.net.AddServer(0, srv)
}

var ncpu_once sync.Once

func make_config(t *testing.T, unreliable bool) *config {
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.clerks = make(map[*Clerk]string)
	cfg.nextClientId = SERVERID + 1
	cfg.start = time.Now()

	cfg.StartServer()

	cfg.net.Reliable(!unreliable)

	return cfg
}

func (cfg *config) rpcTotal() int {
	return cfg.net.GetTotalCount()
}

// start a Test.
// print the Test message.
// e.g. cfg.begin("Test (2B): RPC counts aren't too high")
func (cfg *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.rpcTotal()
	atomic.StoreInt32(&cfg.ops, 0)
}

func (cfg *config) op() {
	atomic.AddInt32(&cfg.ops, 1)
}

// end a Test -- the fact that we got here means there
// was no failure.
// print the Passed message,
// and some performance numbers.
func (cfg *config) end() {
	cfg.checkTimeout()
	if cfg.t.Failed() == false {
		t := time.Since(cfg.t0).Seconds()  // real time
		nrpc := cfg.rpcTotal() - cfg.rpcs0 // number of RPC sends
		ops := atomic.LoadInt32(&cfg.ops)  //  number of clerk get/put/append calls

		fmt.Printf("  ... Passed --")
		fmt.Printf(" t %4.1f nrpc %5d ops %4d\n", t, nrpc, ops)
	}
}
