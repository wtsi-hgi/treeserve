package treeserve

import (
	"fmt"
	"testing"
)

func TestUpdateMap(t *testing.T) {
}

func TestGetQueryParameters(t *testing.T) {
}

func TestAddChild(t *testing.T) {

}

func TestAddAggregates(t *testing.T) {

	b1 := NewBigint()
	b1.SetInt64(100)
	b2 := NewBigint()
	b2.SetInt64(10000)
	a := Aggregates{Group: "xx", User: "yy", Tag: "zz", Count: b1, Size: b1, AccessCost: b1, ModifyCost: b1, CreationCost: b1}
	b := Aggregates{Group: "xx", User: "yy", Tag: "zz", Count: b2, Size: b2, AccessCost: b2, ModifyCost: b1, CreationCost: b1}

	fmt.Println(addAggregates(a, b))

}
