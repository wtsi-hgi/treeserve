package treeserve

import (
	"errors"
	"fmt"
	"math/big"

	log "github.com/Sirupsen/logrus"
)

// Aggregate stats contains the rolled up values for a node, with the associated mapping to group/user/tag
// the mapping may have more than one set of Goup/User/Tag for which the numbers are the same.
type AggregateStats struct {
	StatMappings *StatMappings
	Size         *big.Int
	Count        *big.Int
	CreateCost   *big.Int
	ModifyCost   *big.Int
	AccessCost   *big.Int
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
				log.Fatal(fmt.Sprintf("Key not found"))
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

				b1 := big.NewInt(0)
				b1.Add(a.AccessCost, got.AccessCost)
				a.AccessCost = b1

				b2 := big.NewInt(0)
				b2.Add(a.ModifyCost, got.ModifyCost)
				a.ModifyCost = b2

				b3 := big.NewInt(0)
				b3.Add(a.CreateCost, got.CreateCost)
				a.CreateCost = b3

				b4 := big.NewInt(0)
				b4.Add(a.Size, got.Size)
				a.Size = b4

				b5 := big.NewInt(0)
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

// saveAggregateStats saves a set of stats to the database
func (ts *TreeServe) saveAggregateStats(node *Md5Key, aggregateStats []*AggregateStats) (err error) {

	for i := range aggregateStats {

		n := GetAggregateNums(aggregateStats[i])

		// for each set of aggregate stats, add the stats and the mapping to the database
		for k, v := range aggregateStats[i].StatMappings.m {

			if aggregateStats[i].Count.Cmp(big.NewInt(0)) == 0 {
				err = errors.New("Node with zero count stat")
				return
			}

			k1, _, _ := ts.GenerateAggregateKeys(node, &k)
			err = ts.StatMappingsDB.AddKeyToKeySet(node, k1)
			if err != nil {
				LogError(err)
				return
			}

			err = ts.StatMappingDB.Add(k1, v, true)
			if err != nil {
				LogError(err)
				return err
			}

			err = ts.AggregateStatsDB.Add(k1, n, true)
			if err != nil {
				LogError(err)
				return
			}

		}

	}

	return
}
