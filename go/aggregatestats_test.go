package treeserve

import (
	"fmt"
	"math/big"
	"testing"
)

func TestMarshalBinary(t *testing.T) {

	x := big.NewInt(30)

	a := AggregateNums{Count: x}
	b, err := a.MarshalBinary()
	fmt.Println(string(b), err)

	c := AggregateNums{}
	err = c.UnmarshalBinary(b)

	fmt.Println(*c.Count, err)

}

func TestCombineAggregateStats(t *testing.T) {
	testdata := []*AggregateStats{}
	categories1 := StatMapping{"u1", "g2", "t3"}
	categories2 := StatMapping{"u1", "g2", "t4"}

	cat1Key := categories1.GetKey()
	cat2Key := categories2.GetKey()

	stat1 := NewStatMappings()
	stat1.Add(cat1Key, &categories1)
	stat1.Add(cat2Key, &categories2)

	stat2 := NewStatMappings()
	stat2.Add(cat1Key, &categories1)

	b1 := big.NewInt(1)

	b2 := big.NewInt(2)
	b3 := big.NewInt(3)
	b4 := big.NewInt(4)
	b5 := big.NewInt(5)

	testdata = append(testdata, &AggregateStats{stat1, b1, b2, b3, b4, b5})
	//testdata = append(testdata, &AggregateStats{stat1, b1, b1, b1, b1, b1})
	testdata = append(testdata, &AggregateStats{stat2, b1, b1, b1, b1, b1})

	a, b := combineAggregateStats(testdata)

	if b != nil {
		t.Errorf(b.Error())
	}
	if len(a) != 2 {
		t.Errorf("Expected %d, got %d ", 2, len(a))
	}

	for i := range a {
		if a[i].StatMappings.Values()[0].Tag == "t4" {
			if a[i].Size.Text(10) != "1" {
				t.Errorf("Expected %s, got %s ", "1", a[i].Size.Text(10))
			}
		}
		if a[i].StatMappings.Values()[0].Tag == "t3" {
			if a[i].Size.Text(10) != "2" {
				t.Errorf("Expected %s, got %s ", "2", a[i].Size.Text(10))
			}
		}
		fmt.Println(a[i])
	}

}
