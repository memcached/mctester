package ratectrl

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jamiealquiza/tachymeter"
)

type Report struct {
	StartTime time.Time
	EndTime   time.Time
	Config    *Config
	Metrics   *tachymeter.Metrics
	Stats     *Stats
}

func (report *Report) PrettyPrint() (err error) {
	jsonReport, err := json.MarshalIndent(report, "", "\t")
	if err == nil {
		fmt.Println(string(jsonReport))
	}
	return
}

type Stats struct {
	DeleteHits    int
	DeleteMisses  int
	GetHits       int
	GetMisses     int
	KeyCollisions int
	SetsTotal     int
}

func (stats *Stats) Add(other *Stats) {
	stats.DeleteHits += other.DeleteHits
	stats.DeleteMisses += other.DeleteMisses
	stats.GetHits += other.GetHits
	stats.GetMisses += other.GetMisses
	stats.KeyCollisions += other.KeyCollisions
	stats.SetsTotal += other.SetsTotal
}
