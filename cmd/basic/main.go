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

	connCount := flag.Int("conncount", 1, "number of client connections to establish")
	reqPerSleep := flag.Int("reqpersleep", 1, "number of requests to issue when client wakes up")
	reqBundlePerConn := flag.Int("reqbundles", 1, "number of times to wake up and send requests before disconnecting (-1 for unlimited)")
	sleepPerBundle := flag.Duration("sleepperbundle", time.Millisecond*1, "time to sleep between request bundles (accepts Ns, Nms, etc)")
	deletePercent := flag.Int("deletepercent", 0, "percentage of queries to issue as deletes instead of gets (0-1000)")
	keyPrefix := flag.String("keyprefix", "mctester:", "prefix to append to all generated keys")
	keySpace := flag.Int("keyspace", 1000, "number of unique keys to generate")
	keyLength := flag.Int("keylength", 10, "number of random characters to append to key")
	keyTTL := flag.Uint("ttl", 180, "TTL to set with new items")
	useZipf := flag.Bool("zipf", false, "use Zipf instead of uniform randomness (slow)")
	zipfS := flag.Float64("zipfS", 1.01, "zipf S value (general pull toward zero) must be > 1.0")
	zipfV := flag.Float64("zipfV", float64(*keySpace/2), "zipf V value (pull below this number")
	valueSize := flag.Uint("valuesize", 1000, "size of value (in bytes) to store on miss")
	clientFlags := flag.Uint("clietnflags", 0, "(32bit unsigned) client flag bits to set on miss")

	flag.Parse()

	/*
		// example for testing zipf/random string code.
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
		desiredConnCount:      *connCount,
		requestsPerSleep:      *reqPerSleep,
		requestBundlesPerConn: *reqBundlePerConn,
		sleepPerBundle:        *sleepPerBundle,
		deletePercent:		   *deletePercent,
		keyLength:             *keyLength,
		keyPrefix:             *keyPrefix,
		keySpace:              *keySpace,
		keyTTL:                *keyTTL,
		useZipf:               *useZipf,
		zipfS:                 *zipfS,
		zipfV:                 *zipfV,
		valueSize:             *valueSize,
		clientFlags:           *clientFlags,
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
	deletePercent         int
	keyLength             int
	keyPrefix             string
	keySpace              int
	keyTTL                uint
	useZipf               bool
	zipfS                 float64 // (> 1, generally 1.01-2) pulls the power curve toward 0)
	zipfV                 float64 // v (< keySpace) puts the main part of the curve before this number
	valueSize             uint
	clientFlags           uint
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

	subRS := pcgr.New(1, 0) // randomizer is re-seeded for random strings.
	var res int
	defer func() { doneChan <- res }()

	for bundles == -1 || bundles > 0 {
		bundles--
		for i := l.requestsPerSleep; i > 0; i-- {
			// generate keys
			// TODO: Allow min/max length for keys.
			// The random key needs to stick with the random length, or we end
			// up with keySpace * (max-min) number of unique keys.
			// Need to pull the randomizer exactly once (then just modulus for
			// a poor-but-probably-fine random value), then build the random
			// string from the rest.
			// Could also re-seed it twice, pull once Intn for length,
			// re-seed, then again for key space.

			keyLen := l.keyLength
			if l.useZipf {
				subRS.Seed(int64(zipRS.Uint64()))
			} else {
				subRS.Seed(int64(randR.Intn(l.keySpace)))
			}
			// TODO: might be nice to pass (by ref?) prefix in here to make
			// use of string.Builder.
			key := l.keyPrefix + mct.RandString(&subRS, keyLen)
			// chance we issue a delete instead.
			if l.deletePercent != 0 && randR.Intn(1000) < l.deletePercent {
				_, err := mc.Delete(key)
				if err != nil {
					fmt.Println(err)
					res = -1
					return
				}
			} else {
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
					// TODO: random sizing
					value := mct.RandBytes(&rs, int(l.valueSize))
					mc.Set(key, uint32(l.clientFlags), uint32(l.keyTTL), value)
				}
			}
		}
		time.Sleep(l.sleepPerBundle)
	}
}
