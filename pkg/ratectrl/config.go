package ratectrl

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"time"

	"github.com/dgryski/go-pcgr"
	"github.com/jamiealquiza/tachymeter"
	mct "github.com/memcached/mctester/internal"
	"github.com/memcached/mctester/pkg/client"
	"go.uber.org/ratelimit"
	"golang.org/x/sync/errgroup"
)

type Config struct {
	ClientFlags    uint
	ConnCount      int
	DelRatio       int
	Duration       time.Duration
	GetRatio       int
	KeyLength      int
	KeyPrefix      string
	KeySpace       int
	KeyTTL         uint
	MetaDelFlags   string
	MetaGetFlags   string
	MetaSetFlags   string
	Pipelines      uint
	RngSeed        int64
	RPS            int
	Servers        []string
	SetRatio       int
	Socket         string
	StripKeyPrefix bool
	UseMeta        bool
	UseZipf        bool
	ValidateGets   bool
	ValueSize      uint
	WarmPercent    int
	ZipfS          float64 // (> 1, generally 1.01-2) pulls the power curve toward 0)
	ZipfV          float64 // v (< keySpace) puts the main part of the curve before this number

	cacheEntries []CacheEntry
	rateLimiter  ratelimit.Limiter
	tachymeter   *tachymeter.Tachymeter
}

type CacheEntry struct {
	key   string
	value []byte
}

func (conf *Config) GenerateEntries() (entries []CacheEntry) {
	entries = make([]CacheEntry, conf.KeySpace)
	subRS := pcgr.New(1, 0)

	for i := 0; i < conf.KeySpace; i++ {
		subRS.Seed(conf.RngSeed + int64(i))
		key := mct.RandString(&subRS, conf.KeyLength, conf.KeyPrefix)

		valSeed := new(big.Int).SetBytes([]byte(key)).Int64()
		subRS.Seed(valSeed)
		value := mct.RandBytes(&subRS, int(conf.ValueSize))

		entries[i] = CacheEntry{key, value}
	}

	return
}

func (conf *Config) Run() (err error) {
	g, _ := errgroup.WithContext(context.Background())

	samples := conf.RPS * conf.ConnCount
	if samples < 1000 {
		samples = 1000
	}

	conf.cacheEntries = conf.GenerateEntries()

	if conf.WarmPercent > 0 {
		err = conf.WarmCache()
		if err != nil {
			return
		}
	}

	if conf.RPS > 0 {
		conf.rateLimiter = ratelimit.New(conf.RPS)
	} else {
		conf.rateLimiter = ratelimit.NewUnlimited()
	}

	threadStats := make(chan Stats, conf.ConnCount)
	conf.tachymeter = tachymeter.New(&tachymeter.Config{Size: samples})
	startTime := time.Now()

	for worker := 0; worker < conf.ConnCount; worker++ {
		index := worker
		g.Go(func() error {
			return conf.Worker(index, threadStats)
		})
	}

	err = g.Wait()
	endTime := time.Now()
	if err != nil {
		return
	}

	conf.tachymeter.SetWallTime(time.Since(startTime))
	close(threadStats)
	testStats := &Stats{}
	for stats := range threadStats {
		testStats.Add(&stats)
	}
	if !conf.ValidateGets {
		testStats.KeyCollisions = -1
	}

	report := &Report{
		StartTime: startTime,
		EndTime:   endTime,
		Config:    conf,
		Metrics:   conf.tachymeter.Calc(),
		Stats:     testStats,
	}
	err = report.PrettyPrint()

	return
}

func (conf *Config) WarmCache() error {
	mc := client.NewClient(conf.Servers[0], conf.Socket, conf.Pipelines, conf.KeyPrefix, conf.StripKeyPrefix)
	rs := pcgr.New(conf.RngSeed, 0)
	randR := rand.New(&rs)

	for keyIndex := 0; keyIndex < conf.KeySpace; keyIndex++ {
		if randR.Intn(100) < conf.WarmPercent {
			entry := conf.cacheEntries[keyIndex]
			key := entry.key
			value := entry.value

			if conf.UseMeta {
				err := mc.MetaSet(key, conf.MetaSetFlags, value)
				if err != nil {
					fmt.Printf("metaset error: %v\n", err)
					return err
				}

				_, _, c, err := mc.MetaReceive()
				if c != client.McHD {
					fmt.Printf("metaset not stored: %d\n", c)
				}
				if err != nil {
					fmt.Printf("metaset receive error: %v\n", err)
					return err
				}
			} else {
				_, err := mc.Set(key, uint32(conf.ClientFlags), uint32(conf.KeyTTL), value)
				if err != nil {
					fmt.Println(err)
					return err
				}
			}
		}
	}

	return nil
}

func (conf *Config) Worker(index int, results chan Stats) error {
	mc := client.NewClient(conf.Servers[0], conf.Socket, conf.Pipelines, conf.KeyPrefix, conf.StripKeyPrefix)
	stats := Stats{}
	rl := conf.rateLimiter

	workerSeed := conf.RngSeed + int64(index) + int64(conf.KeySpace)
	rs := pcgr.New(workerSeed, 0)
	randR := rand.New(&rs)

	var zipRS *rand.Zipf
	if conf.UseZipf {
		zipRS = rand.NewZipf(randR, conf.ZipfS, conf.ZipfV, uint64(conf.KeySpace))
		if zipRS == nil {
			fmt.Printf("bad arguments to zipf: S: %f V: %f\n", conf.ZipfS, conf.ZipfV)
			return nil
		}
	}

	for start := time.Now(); ; {
		iterStart := time.Now()
		if iterStart.Sub(start) > conf.Duration {
			break
		}

		var index int
		if conf.UseZipf {
			index = int(zipRS.Uint64())
		} else {
			index = randR.Intn(conf.KeySpace)
		}

		entry := conf.cacheEntries[index]
		key := entry.key

		rl.Take()
		switch rng := randR.Intn(conf.DelRatio + conf.SetRatio + conf.GetRatio); {
		case rng < conf.DelRatio:
			var code client.McCode
			var err error

			if conf.UseMeta {
				err = mc.MetaDelete(key, conf.MetaDelFlags)
				if err != nil {
					fmt.Printf("metadelete error: %v\n", err)
					return err
				}

				_, _, code, err = mc.MetaReceive()
				if code != client.McHD && code != client.McNF {
					fmt.Printf("metadelete not successful: %d\n", code)
				}
				if err != nil {
					fmt.Printf("metadelete receive error: %v\n", err)
					return err
				}
			} else {
				code, err = mc.Delete(key)
				if err != nil {
					fmt.Println(err)
					return err
				}
			}

			switch code {
			case client.McDELETED, client.McHD:
				stats.DeleteHits++
			case client.McNOT_FOUND, client.McNF:
				stats.DeleteMisses++
			}
		case rng < (conf.DelRatio + conf.SetRatio):
			value := entry.value

			if conf.UseMeta {
				err := mc.MetaSet(key, conf.MetaSetFlags, value)
				if err != nil {
					fmt.Printf("metaset error: %v\n", err)
					return err
				}

				_, _, code, err := mc.MetaReceive()
				if code != client.McHD {
					fmt.Printf("metaset not stored: %d\n", code)
				}
				if err != nil {
					fmt.Printf("metaset receive error: %v\n", err)
					return err
				}
			} else {
				_, err := mc.Set(key, uint32(conf.ClientFlags), uint32(conf.KeyTTL), value)
				if err != nil {
					fmt.Println(err)
					return err
				}
			}

			stats.SetsTotal++
		default:
			var code client.McCode
			var value []byte
			if conf.UseMeta {
				err := mc.MetaGet(key, conf.MetaGetFlags)
				if err != nil {
					fmt.Printf("metaget error: %v\n", err)
					return err
				}
				_, value, code, err = mc.MetaReceive()
				if err != nil {
					fmt.Printf("metaget receive error: %v\n", err)
					return err
				}
			} else {
				var err error
				_, value, code, err = mc.Get(key)
				if err != nil {
					fmt.Println(err, value)
					return err
				}
			}

			switch code {
			case client.McHIT, client.McVA:
				stats.GetHits++

				expectedValue := entry.value
				if conf.ValidateGets && !bytes.Equal(value, expectedValue) {
					stats.KeyCollisions++
					fmt.Printf("Unexpected value found for key `%s`\n\tExpected Value: %s\n\tActual Value: %s\n", key, expectedValue, value)
				}
			case client.McMISS, client.McEN:
				stats.GetMisses++
			}
		}

		conf.tachymeter.AddTime(time.Since(iterStart))
	}

	results <- stats
	return nil
}
