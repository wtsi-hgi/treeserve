package treeserve

import (
	"errors"
	"fmt"

	log "github.com/Sirupsen/logrus"
)

// Aggregate stats contains the rolled up values for a node, with the associated mapping to group/user/tag
// the mapping may have more than one set of Goup/User/Tag for which the numbers are the same.
type AggregateStats struct {
	StatMappings *StatMappings
	Size         *Bigint
	Count        *Bigint
	CreateCost   *Bigint
	ModifyCost   *Bigint
	AccessCost   *Bigint
}

// Add adds aggregate values where a set of group/user/tag is in the StatMappings
// of both. If not found return error
func (stats *AggregateStats) Add(addend *AggregateStats) (err error) {
	// check it's there
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
	s = fmt.Sprintf("size: %v ", stats.Size.Text(10))
	s += fmt.Sprintf(" count: %v ", stats.Count.Text(10))
	s += fmt.Sprintf(" acost: %v ", stats.AccessCost.Text(10))
	s += fmt.Sprintf(" mcost: %v ", stats.ModifyCost.Text(10))
	s += fmt.Sprintf(" ccost: %v ", stats.CreateCost.Text(10))
	r := stats.StatMappings.Values()
	for j := range r {
		s += fmt.Sprintf(" mappings: %v, %v, %v ", r[j].Group, r[j].User, r[j].Tag)
	}

	return
}

// combineAggregateStats takes an array of AggregateStats that could have repeated StatMappings and combines the entries
func combineAggregateStats(input []*AggregateStats) (combined []*AggregateStats, err error) {

	flattened := make(map[Md5Key]AggregateStats)

	for i := range input {
		//fmt.Println(i, input[i])
		if input[i] == nil {
			continue
		}
		keys := input[i].StatMappings.Keys()
		for k := range keys {
			val, ok := input[i].StatMappings.Get(keys[k])
			if !ok {
				log.Error("value not found for key in statmappings")
				break // not found
			}

			a := AggregateStats{}
			//a.StatMappings = &StatMappings{}
			s := NewStatMappings()

			s.Add(keys[k], val)

			a.StatMappings = s
			a.AccessCost = input[i].AccessCost
			a.ModifyCost = input[i].ModifyCost
			a.CreateCost = input[i].CreateCost
			a.Size = input[i].Size
			a.Count = input[i].Count

			got, OK := flattened[keys[k]] // does this combination already exist?
			if !OK {
				flattened[keys[k]] = a

			} else {

				b1 := NewBigint()

				b1.Add(a.AccessCost, got.AccessCost)
				a.AccessCost = b1

				b2 := NewBigint()
				b2.Add(a.ModifyCost, got.ModifyCost)
				a.ModifyCost = b2

				b3 := NewBigint()
				b3.Add(a.CreateCost, got.CreateCost)
				a.CreateCost = b3

				b4 := NewBigint()
				b4.Add(a.Size, got.Size)
				a.Size = b4

				b5 := NewBigint()
				b5.Add(a.Count, got.Count)
				a.Count = b5

				flattened[keys[k]] = a
			}

		}

	}

	// now we have a set of aggregates with one tag set per number set. Regroup
	// well for now just regenerate (could regroup)
	c2 := []AggregateStats{}
	for _, v := range flattened {
		c2 = append(c2, v)
	}

	for i := range c2 {
		combined = append(combined, &c2[i])

	}
	return
}

// saveAggregateStats saves a set of stats to the databases.
func (ts *TreeServe) saveAggregateStats(node *Md5Key, aggregateStats []*AggregateStats) (err error) {
	//log.Info("SAVING AGGREGATE STATS")

	for i := range aggregateStats {

		// for each set of aggregate stats, add the stats and the mapping to the database
		for k, v := range aggregateStats[i].StatMappings.m {

			// get and add the key to link the aggregate stats to a set of Group/User/Tag mappings and aggregate stats
			if aggregateStats[i].Count.isZero() {
				err = errors.New("Node with zero count stat")
				return
			}

			k1, _, _ := ts.GenerateAggregateKeys(node, &k)
			err = ts.StatMappingsDB.AddKeyToKeySet(node, k1)
			if err != nil {
				logError(err)
				return
			}

			err = ts.StatMappingDB.Add(k1, v, true)
			if err != nil {
				logError(err)
				return
			}

			err = ts.AggregateAccessCostDB.Add(k1, aggregateStats[i].AccessCost, true)
			if err != nil {
				logError(err)
				return
			}
			//err = ts.AggregateAccessCostDB.Add(localKey, aggregateStats.AccessCost, true)

			err = ts.AggregateModifyCostDB.Add(k1, aggregateStats[i].ModifyCost, true)
			if err != nil {
				logError(err)
				return
			}
			//err = ts.AggregateModifyCostDB.Add(localKey, aggregateStats.ModifyCost, true)

			err = ts.AggregateCreateCostDB.Add(k1, aggregateStats[i].CreateCost, true)
			if err != nil {
				logError(err)
				return
			}
			//err = ts.AggregateCreateCostDB.Add(localKey, aggregateStats.CreateCost, true)

			err = ts.AggregateSizeDB.Add(k1, aggregateStats[i].Size, true)
			if err != nil {
				logError(err)
				return
			}

			err = ts.AggregateCountDB.Add(k1, aggregateStats[i].Count, true)
			if err != nil {
				logError(err)
				return
			}
			//err = ts.AggregateSizeDB.Add(localKey, aggregateStats.Size, true)

			//err = ts.AggregateCountDB.Add(localKey, aggregateStats.Count, true)
		}

	}

	return
}
