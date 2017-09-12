package treeserve

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestBuildTree(t *testing.T) {}

func TestUpdateMap(t *testing.T) {
}

func TestGetQueryParameters(t *testing.T) {
}

func TestAddChild(t *testing.T) {

}

func TestSubtractAggregateMap(t *testing.T) {

	b1 := NewBigint()
	b1.SetInt64(100)
	b2 := NewBigint()
	b2.SetInt64(10000)

	a := Aggregates{Group: "xx", User: "yy", Tag: "zz", Count: b1, Size: b1, AccessCost: b1, ModifyCost: b1, CreationCost: b1}
	b := Aggregates{Group: "xx", User: "yy", Tag: "zz", Count: b2, Size: b2, AccessCost: b2, ModifyCost: b1, CreationCost: b1}
	c := Aggregates{Group: "xx", User: "yy", Tag: "aa", Count: b2, Size: b2, AccessCost: b2, ModifyCost: b1, CreationCost: b1}
	d := Aggregates{Group: "xx", User: "yy", Tag: "zz", Count: b1, Size: b1, AccessCost: b1, ModifyCost: b1, CreationCost: b1}

	parent := make(map[string]Aggregates)
	parent["1"] = a
	parent["2"] = b
	parent["3"] = c

	child := make(map[string]Aggregates)
	child["1"] = d

	result, err := subtractAggregateMap(parent, child)
	if err != nil {
		t.Errorf(err.Error())
	}

	//fmt.Printf("%T   %+v", result, result)

	if len(result) != 2 {
		t.Errorf("have %d, wanted %d", len(result), 2)
	}

}

func TestOrganiseAggregates(t *testing.T) {
	b1 := NewBigint()
	b1.SetInt64(100)
	b2 := NewBigint()
	b2.SetInt64(10000)
	a := Aggregates{Group: "xx", User: "yy", Tag: "zz", Count: b1, Size: b1, AccessCost: b1, ModifyCost: b1, CreationCost: b1}
	b := Aggregates{Group: "xx", User: "yy", Tag: "zz", Count: b2, Size: b2, AccessCost: b2, ModifyCost: b1, CreationCost: b1}
	f := Aggregates{Group: "xx", User: "yy", Tag: "aa", Count: b2, Size: b2, AccessCost: b2, ModifyCost: b1, CreationCost: b1}

	m, err := organiseAggregates([]Aggregates{a, b, f})
	if err != nil {
		t.Errorf(err.Error())
	}

	j, err := json.Marshal(m)
	if err != nil {
		t.Errorf(err.Error())
	}

	fmt.Println(string(j))

}

func TestAddAggregates(t *testing.T) {

	b1 := NewBigint()
	b1.SetInt64(100)
	b2 := NewBigint()
	b2.SetInt64(10000)
	a := Aggregates{Group: "xx", User: "yy", Tag: "zz", Count: b1, Size: b1, AccessCost: b1, ModifyCost: b1, CreationCost: b1}
	b := Aggregates{Group: "xx", User: "yy", Tag: "zz", Count: b2, Size: b2, AccessCost: b2, ModifyCost: b1, CreationCost: b1}

	c, err := addAggregates(a, b)
	if err != nil {
		t.Errorf(err.Error())
	}
	fmt.Println(c)

	c1 := NewBigint()
	c1.SetInt64(10100)
	if !c.Count.Equals(c1) {
		t.Errorf("wanted %+v, got %+v", c1, c.Count)
	}

}

func TestBuildMap(t *testing.T) {

	//write the file
	filename := "/tmp/temp"
	testdata := []string{"sjc:x:1000:", "postfix:x:133:", "postdrop:x:134:", "docker:x:999:"}

	fo, err := os.Create(filename)
	if err != nil {
		t.Errorf(err.Error())
	}
	defer fo.Close()

	for i := range testdata {
		s := testdata[i] + "\n"
		_, err = io.WriteString(fo, s)
		if err != nil {
			t.Errorf(err.Error())
		}
	}

	fo.Close()

	// make the map ... what we are testing
	m := buildMap(filename, ":", 2, 0)

	for k, v := range m {
		fmt.Println(k, v)
	}

	// check shoudl exist
	if val, ok := m["133"]; ok {
		if val != "postfix" {
			t.Errorf("Got %s, wanted %s", val, "postfix")
		}
	} else {
		t.Errorf("Got no entry, wanted %s", "postfix")
	}
	// check shoudl not exist
	if val, ok := m["10"]; ok {

		t.Errorf("Got %s, wanted no value", val)

	}

}

func TestLookUpUID(t *testing.T) {
	userMap = buildMap("/home/sjc/testdata/p", ":", 2, 0)
	groupMap = buildMap("/home/sjc/testdata/g", ":", 2, 0)
	fmt.Println(lookupUID("*"))
	fmt.Println(lookupUID("0"))
}

func TestLookUpGID(t *testing.T) {
	userMap = buildMap("/home/sjc/testdata/p", ":", 2, 0)
	groupMap = buildMap("/home/sjc/testdata/g", ":", 2, 0)
	fmt.Println(lookupGID("*"))
	fmt.Println(lookupGID("0"))
}

func TestMapFromAggregateArray(t *testing.T) {

	b1 := NewBigint()
	b1.SetInt64(100)
	b2 := NewBigint()
	b2.SetInt64(10000)

	a := Aggregates{Group: "xx", User: "yy", Tag: "zz", Count: b1, Size: b1, AccessCost: b1, ModifyCost: b1, CreationCost: b1}
	b := Aggregates{Group: "xx", User: "yy", Tag: "zz", Count: b2, Size: b2, AccessCost: b2, ModifyCost: b1, CreationCost: b1}
	c := Aggregates{Group: "xx", User: "yy", Tag: "aa", Count: b2, Size: b2, AccessCost: b2, ModifyCost: b1, CreationCost: b1}
	d := Aggregates{Group: "xx", User: "yy", Tag: "zz", Count: b1, Size: b1, AccessCost: b1, ModifyCost: b1, CreationCost: b1}

	array := []Aggregates{a, b, c, d}

	m := mapFromAggregateArray(array)

	fmt.Println(m, len(m))

	for k, v := range m {
		fmt.Println(k, v.Count.Text(10))
	}
}
