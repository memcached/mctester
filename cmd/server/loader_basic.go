package main

import (
	"fmt"
	"math/rand"
	"time"

	mct "mctester/internal"

	"github.com/dgryski/go-pcgr"
)

// Basic persistent load test, using text protocol:
type BasicLoader struct {
	Servers               []string      `json:"servers"`
	Socket                string        `json:"socket"`
	Pipelines             uint          `json:"pipelines"`
	StripKeyPrefix        bool          `json:"stripkeyprefix"`
	DesiredConnCount      int           `json:"conncount"`
	RequestsPerSleep      int           `json:"reqpersleep"`
	RequestBundlesPerConn int           `json:"reqbundlesperconn"`
	SleepPerBundle        time.Duration `json:"sleepperbundle"`
	DeletePercent         int           `json:"deletepercent"`
	KeyLength             int           `json:"keylength"`
	KeyPrefix             string        `json:"keyprefix"`
	KeySpace              int           `json:"keyspace"`
	KeyTTL                uint          `json:"keyttl"`
	UseZipf               bool          `json:"zipf"`
	ZipfS                 float64       `json:"zipfS"` // (> 1, generally 1.01-2) pulls the power curve toward 0)
	ZipfV                 float64       `json:"zipfV"` // v (< KeySpace) puts the main part of the curve before this number
	ValueSize             uint          `json:"valuesize"`
	ClientFlags           uint          `json:"clientflags"`
	stopAfter             time.Time
}

func newBasicLoader() *BasicLoader {
	return &BasicLoader{
		Servers:               []string{"127.0.0.1:11211"},
		Socket:                "",
		Pipelines:             1,
		StripKeyPrefix:        false,
		DesiredConnCount:      1,
		RequestsPerSleep:      1,
		RequestBundlesPerConn: 1,
		SleepPerBundle:        time.Millisecond * 1,
		DeletePercent:         0,
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
			// TODO: add a small backoff delay based on how fast we're
			// reaching here along with an error condition.
			// Need to add the error condition to doneReceiver first.
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
				// Close all the update channels so workers know to die off.
				for _, wc := range workers {
					close(wc)
				}
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
	mc := mct.NewClient(host, l.Socket, l.Pipelines, l.KeyPrefix, l.StripKeyPrefix)
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
	// TODO: struct with id, res, err?
	defer func() {
		doneChan <- id
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

			if l.UseZipf {
				subRS.Seed(int64(zipRS.Uint64()))
			} else {
				subRS.Seed(int64(randR.Intn(l.KeySpace)))
			}

			key := mct.RandString(&subRS, l.KeyLength, l.KeyPrefix)
			// chance we issue a delete instead.
			if l.DeletePercent != 0 && randR.Intn(1000) < l.DeletePercent {
				_, err := mc.Delete(key)
				if err != nil {
					fmt.Println(err)
					return
				}
			} else {
				// issue gets
				_, _, code, err := mc.Get(key)
				// validate responses
				if err != nil {
					fmt.Println(err)
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
		case update, ok := <-updateChan:
			// TODO: re-create client if server changed.
			if ok {
				l = update
			} else {
				// Told to die. Let the deferral handle updating doneChan.
				return
			}
		default:
			// nothing. just fast-check for updates.
		}
		time.Sleep(l.SleepPerBundle)
	}
}
