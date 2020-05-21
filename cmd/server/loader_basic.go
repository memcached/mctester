package main

import (
	"fmt"
	pcgr "github.com/dgryski/go-pcgr"
	mct "github.com/dormando/mctester"
	"math/rand"
	"time"
)

// Basic persistent load test, using text protocol:
type BasicLoader struct {
	Servers               []string
	stopAfter             time.Time
	DesiredConnCount      int
	RequestsPerSleep      int
	RequestBundlesPerConn int
	SleepPerBundle        time.Duration
	DeletePercent         int
	KeyLength             int
	KeyPrefix             string
	KeySpace              int
	KeyTTL                uint
	UseZipf               bool
	ZipfS                 float64 // (> 1, generally 1.01-2) pulls the power curve toward 0)
	ZipfV                 float64 // v (< KeySpace) puts the main part of the curve before this number
	ValueSize             uint
	ClientFlags           uint
}

func newBasicLoader() (*BasicLoader) {
	return &BasicLoader{
		Servers:               []string{"127.0.0.1:11211"},
		DesiredConnCount:      1,
		RequestsPerSleep:      1,
		RequestBundlesPerConn: 1,
		SleepPerBundle:        time.Millisecond*1,
		DeletePercent:		   0,
		KeyLength:             10,
		KeyPrefix:             "mctester:",
		KeySpace:              1000,
		KeyTTL:                180,
		UseZipf:               false,
		ZipfS:                 1.01,
		ZipfV:                 500,
		ValueSize:             1000,
		ClientFlags:           0,
	}
}

// Update receives *BasicLoader's from the server.
func runBasicLoader(Update <-chan interface{}, worker interface{}) {
	var l *BasicLoader = worker.(*BasicLoader)
	runners := 0
	nextId := 1
	workers := make(map[int]chan *BasicLoader)
	doneReceiver := make(chan int, 50)
	// need map of workers to update channel so we can broadcast updates...
	// worker channels should have 1 buffer, maybe? else it'll take forever to
	// update.

	for {
		keepGoing := true
		for runners < l.DesiredConnCount {
			// Give it a buffer of 1 to avoid race where worker is dying
			// rather than looking at its update channel.
			wc := make(chan *BasicLoader, 1)
			workers[nextId] = wc
			go basicWorker(nextId, doneReceiver, wc, l)
			nextId++
			runners++
		}

		select {
		case id := <-doneReceiver:
			runners--
			delete(workers, id)
		case update, ok := <-Update:
			if ok {
				fmt.Printf("received basic loader update\n")
				l = update.(*BasicLoader)
				// Blast out update to everyone.
				// Note they will pick up changes during the next sleep cycle.
				for _, wc := range workers {
					wc <- l
				}
				fmt.Printf("sent loader update to workers\n")
			} else {
				keepGoing = false
			}
		}

		if !keepGoing {
			// Let all the workers die off so they don't explode when writing
			// to doneChan's.
			for runners != 0 {
				id := <-doneReceiver
				delete(workers, id)
				runners--
			}
			return
		}
	}
}

// TODO: use sync.Pool for Item/etc?
// pool.Put() items back before sleep.
// may also be able to cache mc's bufio's this way.
func basicWorker(id int, doneChan chan<- int, updateChan <-chan *BasicLoader, l *BasicLoader) {
	// TODO: server selector.
	host := l.Servers[0]
	mc := mct.NewClient(host)
	bundles := l.RequestBundlesPerConn

	rs := pcgr.New(time.Now().UnixNano(), 0)
	var zipRS *rand.Zipf
	randR := rand.New(&rs) // main randomizer, so we can use the random interface.
	if l.UseZipf {
		zipRS = rand.NewZipf(randR, l.ZipfS, l.ZipfV, uint64(l.KeySpace))
		if zipRS == nil {
			fmt.Printf("bad arguments to zipf: S: %f V: %f\n", l.ZipfS, l.ZipfV)
			return
		}
	}

	subRS := pcgr.New(1, 0) // randomizer is re-seeded for random strings.
	var res int
	// TODO: struct with id, res, err?
	defer func() {
		doneChan <- id
		fmt.Printf("worker result: %d\n", res)
	}()

	for bundles == -1 || bundles > 0 {
		bundles--
		for i := l.RequestsPerSleep; i > 0; i-- {
			// generate keys
			// TODO: Allow min/max length for keys.
			// The random key needs to stick with the random length, or we end
			// up with KeySpace * (max-min) number of unique keys.
			// Need to pull the randomizer exactly once (then just modulus for
			// a poor-but-probably-fine random value), then build the random
			// string from the rest.
			// Could also re-seed it twice, pull once Intn for length,
			// re-seed, then again for key space.

			keyLen := l.KeyLength
			if l.UseZipf {
				subRS.Seed(int64(zipRS.Uint64()))
			} else {
				subRS.Seed(int64(randR.Intn(l.KeySpace)))
			}
			// TODO: might be nice to pass (by ref?) prefix in here to make
			// use of string.Builder.
			key := l.KeyPrefix + mct.RandString(&subRS, keyLen)
			// chance we issue a delete instead.
			if l.DeletePercent != 0 && randR.Intn(1000) < l.DeletePercent {
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
					value := mct.RandBytes(&rs, int(l.ValueSize))
					mc.Set(key, uint32(l.ClientFlags), uint32(l.KeyTTL), value)
				}
			}
		}
		select {
		case update := <-updateChan:
			// TODO: re-create client if server changed.
			l = update
		default:
			// nothing. just fast-check for updates.
		}
		time.Sleep(l.SleepPerBundle)
	}
}
