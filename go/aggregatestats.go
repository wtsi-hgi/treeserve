package treeserve

import (
	"fmt"

	log "github.com/Sirupsen/logrus"
)

type AggregateStats struct {
	StatMappings *StatMappings
	Size         *Bigint
	Count        *Bigint
	CreateCost   *Bigint
	ModifyCost   *Bigint
	AccessCost   *Bigint
}

func (stats *AggregateStats) Add(addend *AggregateStats) (err error) {
	//	allKeys := stats.StatMappingKeys.Union(*addend.StatMappingKeys)
	//	stats.StatMappingKeys = &allKeys
	for _, k := range addend.StatMappings.Keys() {
		_, have := stats.StatMappings.Get(k)
		if !have {
			newStatMapping, ok := addend.StatMappings.Get(k)
			if !ok {
				log.WithFields(log.Fields{
					"stats": stats,
					"k":     k,
				}).Fatal("AggregateStats.Add addend did not have key k")
			}
			stats.StatMappings.Add(k, newStatMapping)
		}
	}
	stats.Size.Add(stats.Size, addend.Size)
	stats.Count.Add(stats.Count, addend.Count)
	stats.CreateCost.Add(stats.CreateCost, addend.CreateCost)
	stats.ModifyCost.Add(stats.ModifyCost, addend.ModifyCost)
	stats.AccessCost.Add(stats.AccessCost, addend.AccessCost)
	return
}

func (stats *AggregateStats) String() (s string) {
	// TODO
	s = fmt.Sprintf("size: %v", stats.Size.Text(10))
	return
}
