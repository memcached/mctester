package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/memcached/mctester/pkg/ratectrl"
)

func main() {
	fmt.Println("starting")

	clientFlags := flag.Uint("clientflags", 0, "(32bit unsigned) client flag bits to set on miss")
	connCount := flag.Int("conncount", 1, "number of client connections to establish")
	duration := flag.Duration("duration", 0, "length of time that the test will run (0 for unlimited)")
	keyLength := flag.Int("keylength", 10, "number of random characters to append to key")
	keyPrefix := flag.String("keyprefix", "mctester:", "prefix to append to all generated keys")
	keySpace := flag.Int("keyspace", 1000, "number of unique keys to generate")
	metaDelFlags := flag.String("mdflags", "", "flags sent alongside 'Meta Delete' commands")
	useMeta := flag.Bool("meta", false, "if true, communicate with Memcached using meta commands")
	metaGetFlags := flag.String("mgflags", "f v", "flags sent alongside 'Meta Get' commands")
	metaSetFlags := flag.String("msflags", "", "flags sent alongside 'Meta Set' commands")
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

	testConfig := &ratectrl.Config{
		ClientFlags:    *clientFlags,
		ConnCount:      *connCount,
		DelRatio:       *delRatio,
		Duration:       *duration,
		GetRatio:       *getRatio,
		KeyLength:      *keyLength,
		KeyPrefix:      *keyPrefix,
		KeySpace:       *keySpace,
		KeyTTL:         *keyTTL,
		MetaDelFlags:   *metaDelFlags,
		MetaGetFlags:   *metaGetFlags,
		MetaSetFlags:   *metaSetFlags,
		Pipelines:      *pipelines,
		RngSeed:        *rngSeed,
		RPS:            *rps,
		Servers:        []string{*server},
		SetRatio:       *setRatio,
		Socket:         *socket,
		StripKeyPrefix: *stripKeyPrefix,
		UseMeta:        *useMeta,
		UseZipf:        *useZipf,
		ValidateGets:   *validateGets,
		ValueSize:      *valueSize,
		WarmPercent:    *warmPercent,
		ZipfS:          *zipfS,
		ZipfV:          *zipfV,
	}

	testConfig.Run()
}
