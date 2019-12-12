package main

import (
	"flag"
	"fmt"
	pcgr "github.com/dgryski/go-pcgr"
	mct "github.com/dormando/mctester"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"
)

var cpuprofile = flag.String("cpuprofile", "", "dump cpu profile to file")
var memprofile = flag.String("memprofile", "", "dump memory profile")

func main() {
	fmt.Println("starting")
	flag.Parse()

	/*
		// example bit for testing zipf/random string code.
		prand := pcgr.New(time.Now().UnixNano(), 0)
		// s (> 1, generally 1.01-2) pulls the power curve toward 0
		// v (anything) puts the main part of the curve before this number,
		// biasing loads below it more.
		// imax is the highest number that will be seen.
		var src = rand.NewZipf(rand.New(&prand), 2, 5, 100)
		subRS := rand.NewSource(1)
		for i := 1; i < 100000; i++ {
			seed := src.Uint64()
			subRS.Seed(int64(seed))
			//fmt.Printf("%d: %s\n", seed, mct.RandString(subRS, 30))
			//fmt.Printf("%d\n", seed)
		}
		os.Exit(0)
	*/

	bl := &BasicLoader{
		servers:               []string{"127.0.0.1:11211"},
		desiredConnCount:      750,
		requestsPerSleep:      50,
		requestBundlesPerConn: 5,
		sleepPerBundle:        time.Millisecond * 50,
		keyMinLength:          30,
		keyMaxLength:          60,
		keyPrefix:             "foobarbazqux",
		keySpace:              100000,
		useZipf:               true,
		zipfS:                 1.5,
		zipfV:                 5,
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Println(err)
			return
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		// TODO: Use a real timer with channel.
		now := time.Now()
		bl.stopAfter = now.Add(time.Second * 10)
		fmt.Printf("time: %v\n", bl.stopAfter)
	}

	bl.Run()
}

// Basic persistent load test, using text protocol:
// - list of servers to connect to, pct of each.
// - zipf or uniform random
// - requests per connect (-1 for unlim)
// - gets per etc
// - multiget or not
// - set or add to replace
// - delete frequency
// - set size range
// - variances: peak/antipeak load
// - variances: how often to change item sizes
type BasicLoader struct {
	servers               []string
	stopAfter             time.Time
	desiredConnCount      int
	requestsPerSleep      int
	requestBundlesPerConn int
	sleepPerBundle        time.Duration
	setValueSizes         []int
	keyMinLength          int
	keyMaxLength          int
	keyPrefix             string
	keySpace              int
	useZipf               bool
	zipfS                 float64 // (> 1, generally 1.01-2) pulls the power curve toward 0)
	zipfV                 float64 // v (< keySpace) puts the main part of the curve before this number
}

func (l *BasicLoader) Run() {
	var runners int
	// TODO: should be method of surfacing errors.
	doneChan := make(chan int, 50)

	for {
		for runners < l.desiredConnCount {
			go l.Worker(doneChan)
			runners++
		}
		res := <-doneChan
		if res == 0 {
			//fmt.Println("That's a bingo!")
		}
		runners--
		if *cpuprofile != "" && time.Now().After(l.stopAfter) {
			return
		}
	}
}

// TODO: use sync.Pool for Item/etc?
// pool.Put() items back before sleep.
// may also be able to cache mc's bufio's this way.
func (l *BasicLoader) Worker(doneChan chan<- int) {
	// FIXME: selector.
	host := l.servers[0]
	mc := mct.NewClient(host)
	bundles := l.requestBundlesPerConn

	rs := pcgr.New(time.Now().UnixNano(), 0)
	var zipRS *rand.Zipf
	randR := rand.New(&rs) // main randomizer, so we can use the random interface.
	if l.useZipf {
		zipRS = rand.NewZipf(randR, l.zipfS, l.zipfV, uint64(l.keySpace))
		if zipRS == nil {
			fmt.Printf("bad arguments to zipf: S: %f V: %f\n", l.zipfS, l.zipfV)
			return
		}
	}

	subRS := pcgr.New(1, 0) // randomizer re-seeded for random strings.
	var res int
	defer func() { doneChan <- res }()

	for bundles == -1 || bundles > 0 {
		bundles--
		for i := l.requestsPerSleep; i > 0; i-- {
			// generate keys
			// TODO: prefix? sep function?
			// TODO: skip rand if min == max
			// TODO: How to get key length to match with key space?
			// use the initial random seed, first value for length, then etc?
			//keyLen := randR.Intn(l.keyMaxLength - l.keyMinLength) + l.keyMinLength
			keyLen := 30
			if l.useZipf {
				subRS.Seed(int64(zipRS.Uint64()))
			} else {
				subRS.Seed(int64(randR.Intn(l.keySpace)))
			}
			key := mct.RandString(&subRS, keyLen)
			// issue gets
			_, _, code, err := mc.Get(key)
			// validate responses
			if err != nil {
				fmt.Println(err)
				res = -1
				return
			}
			// set missing values
			if code == mct.McMISS {
				value := mct.RandBytes(&rs, 3000)
				// TODO: random sizing, TTL, flags
				mc.Set(key, 0, 180, value)
			}
		}
		time.Sleep(l.sleepPerBundle)
	}
}
