package treeserve

import (
	"encoding/json"
	"math/big"
)

// AggregateNums is the 5 numbers rolled up for each node
type AggregateNums struct {
	Size       *big.Int
	Count      *big.Int
	CreateCost *big.Int
	ModifyCost *big.Int
	AccessCost *big.Int
}

// GetAggregateNums returns the numbers from an Aggregate stats struct
func GetAggregateNums(s *AggregateStats) (nums *AggregateNums) {

	n := AggregateNums{}
	n.AccessCost = s.AccessCost
	n.Count = s.Count
	n.CreateCost = s.CreateCost
	n.Size = s.Size
	n.ModifyCost = s.ModifyCost

	nums = &n
	return
}

// MarshalBinary returns binary encoding of the AggregateNums struct. Needed for saving to LMBD as bytes.
func (stats *AggregateNums) MarshalBinary() (data []byte, err error) {
	// also tried gob, no faster

	data, err = json.Marshal(stats)
	LogError(err)

	return
}

// UnmarshalBinary decodes binary encoding of the AggregateNums struct. Needed for retrieving from LMDB
func (stats *AggregateNums) UnmarshalBinary(data []byte) (err error) {

	err = json.Unmarshal(data, stats)

	LogError(err)

	return
}

// NewAggregateNums returns an empty struct
func NewAggregateNums() *AggregateNums {
	return &AggregateNums{}
}
