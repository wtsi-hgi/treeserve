package treeserve

import (
	"fmt"
	"io"
	"os"
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
