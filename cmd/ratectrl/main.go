package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"math/big"
	"math/rand"
	"time"

	mct "mctester/internal"

	"github.com/dgryski/go-pcgr"
	"github.com/jamiealquiza/tachymeter"

	"golang.org/x/sync/errgroup"

	"go.uber.org/ratelimit"
)

func main() {
	fmt.Println("starting")

	clientFlags := flag.Uint("clientflags", 0, "(32bit unsigned) client flag bits to set on miss")
	connCount := flag.Int("conncount", 1, "number of client connections to establish")
	duration := flag.Duration("duration", 0, "length of time that the test will run (0 for unlimited)")
	keyLength := flag.Int("keylength", 10, "number of random characters to append to key")
	keyPrefix := flag.String("keyprefix", "mctester:", "prefix to append to all generated keys")
	keySpace := flag.Int("keyspace", 1000, "number of unique keys to generate")
	pipelines := flag.Uint("pipelines", 1, "(32bit unsigned) number of GET requests to stack within the same syscall")
	delRatio := flag.Int("ratiodel", 0, "proportion of requests that should be sent as 'deletes'")
	getRatio := flag.Int("ratioget", 90, "proportion of requests that should be sent as 'gets'")
	setRatio := flag.Int("ratioset", 10, "proportion of requests that should be sent as 'sets'")
	rngSeed := flag.Int64("rngseed", time.Now().UnixNano(), "seed value used when initializing RNG")
	rps := flag.Int("rps", 0, "target number of requests per second (0 for unlimited)")
	server := flag.String("server", "127.0.0.1:11211", "`ip:port` for Memcached instance under test")
	socket := flag.String("socket", "", "domain socket used for connections")
	stripKeyPrefix := flag.Bool("stripkeyprefix", false, "remove key prefix before comparing with response")
	keyTTL := flag.Uint("ttl", 180, "TTL to set with new items")
	validateGets := flag.Bool("validate", false, "compare the value returned from a 'get' to what was initially 'set'")
	valueSize := flag.Uint("valuesize", 1000, "size of value (in bytes) to store on miss")
	warmPercent := flag.Int("warm", 90, "percent of keys to 'set' in Memcached before testing begins")
	useZipf := flag.Bool("zipf", false, "use Zipf instead of uniform randomness (slow)")
	zipfS := flag.Float64("zipfS", 1.01, "zipf S value (general pull toward zero) must be > 1.0")
	zipfV := flag.Float64("zipfV", float64(*keySpace/2), "zipf V value (pull below this number")

	flag.Parse()

	testConfig := &Config{
		ClientFlags:    *clientFlags,
		ConnCount:      *connCount,
		DelRatio:       *delRatio,
		Duration:       *duration,
		GetRatio:       *getRatio,
		KeyLength:      *keyLength,
		KeyPrefix:      *keyPrefix,
		KeySpace:       *keySpace,
		KeyTTL:         *keyTTL,
		Pipelines:      *pipelines,
		RngSeed:        *rngSeed,
		RPS:            *rps,
		Servers:        []string{*server},
		SetRatio:       *setRatio,
		Socket:         *socket,
		StripKeyPrefix: *stripKeyPrefix,
		UseZipf:        *useZipf,
		ValidateGets:   *validateGets,
		ValueSize:      *valueSize,
		WarmPercent:    *warmPercent,
		ZipfS:          *zipfS,
		ZipfV:          *zipfV,
	}

	testConfig.Run()
}

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
	Pipelines      uint
	RngSeed        int64
	RPS            int
	Servers        []string
	SetRatio       int
	Socket         string
	StripKeyPrefix bool
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
	mc := mct.NewClient(conf.Servers[0], conf.Socket, conf.Pipelines, conf.KeyPrefix, conf.StripKeyPrefix)
	rs := pcgr.New(conf.RngSeed, 0)
	randR := rand.New(&rs)

	for keyIndex := 0; keyIndex < conf.KeySpace; keyIndex++ {
		if randR.Intn(100) < conf.WarmPercent {
			entry := conf.cacheEntries[keyIndex]
			key := entry.key
			value := entry.value

			_, err := mc.Set(key, uint32(conf.ClientFlags), uint32(conf.KeyTTL), value)
			if err != nil {
				fmt.Println(err)
				return err
			}
		}
	}

	return nil
}

func (conf *Config) Worker(index int, results chan Stats) error {
	mc := mct.NewClient(conf.Servers[0], conf.Socket, conf.Pipelines, conf.KeyPrefix, conf.StripKeyPrefix)
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

		switch rng := randR.Intn(conf.DelRatio + conf.SetRatio + conf.GetRatio); {
		case rng < conf.DelRatio:
			rl.Take()
			code, err := mc.Delete(key)
			if err != nil {
				fmt.Println(err)
				return err
			}

			switch code {
			case mct.McDELETED:
				stats.DeleteHits++
			case mct.McNOT_FOUND:
				stats.DeleteMisses++
			}
		case rng < (conf.DelRatio + conf.SetRatio):
			value := entry.value
			rl.Take()
			_, err := mc.Set(key, uint32(conf.ClientFlags), uint32(conf.KeyTTL), value)
			if err != nil {
				fmt.Println(err)
				return err
			}

			stats.SetsTotal++
		default:
			rl.Take()
			_, value, code, err := mc.Get(key)
			if err != nil {
				fmt.Println(err, value)
				return err
			}

			switch code {
			case mct.McHIT:
				stats.GetHits++

				expectedValue := entry.value
				if conf.ValidateGets && !bytes.Equal(value, expectedValue) {
					stats.KeyCollisions++
					fmt.Printf("Unexpected value found for key `%s`\n\tExpected Value: %s\n\tActual Value: %s\n", key, expectedValue, value)
				}

			case mct.McMISS:
				stats.GetMisses++
			}
		}

		conf.tachymeter.AddTime(time.Since(iterStart))
	}

	results <- stats
	return nil
}
