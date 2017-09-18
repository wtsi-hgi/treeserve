// Copyright (c) 2016 Genome Research Ltd.
// Author: Joshua C. Randall <jcrandall@alum.mit.edu>
//
// This program is free software: you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the Free Software
// Foundation; either version 3 of the License, or (at your option) any later
// version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
// details.
//
// You should have received a copy of the GNU General Public License along with
// this program. If not, see <http://www.gnu.org/licenses/>.
//

package treeserve

import (
	"fmt"
	"testing"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

/*
func TestTreeNodeDB(t *testing.T) {
	lmdbPath, err := ioutil.TempDir("", "genericdb_test_treenodedb")
	if err != nil {
		t.Fatalf("failed to create temporary directory for LMDB: %v", err)
	}
	defer syscall.Rmdir(lmdbPath)

	ts := NewTreeServe(lmdbPath+"/lmdb", 1024*1024, 1, 1, 1, false)
	err = ts.OpenLMDB()
	if err != nil {
		t.Fatalf("failed to open LMDB: %v", err)
	}
	defer ts.CloseLMDB()

	testNode1 := &TreeNode{Name: "testNode1-1"}
	testKey1 := Md5Key(md5.Sum([]byte("testKey1")))
	err = ts.TreeNodeDB.Update(&testKey1, func(_ BinaryMarshalUnmarshaler) (node BinaryMarshalUnmarshaler, err error) {
		node = testNode1
		return
	})
	if err != nil {
		t.Errorf("failed to add treenode to database: %v", err)
	}

	treeNodeData, err := ts.TreeNodeDB.Get(&testKey1)
	if err != nil {
		t.Errorf("failed to retrieved treenode from database: %v", err)
	}

	checkTestNode1 := treeNodeData.(*TreeNode)
	if *checkTestNode1 != *testNode1 {
		t.Errorf("retrieved treenode did not match: %v != %v", *checkTestNode1, *testNode1)
	}
}*/

func TestOutputAll(t *testing.T) {
	lmdbPath := "/tmp/treeserve_lmdb"

	LMDBEnv, err := lmdb.NewEnv()
	if err != nil {
		fmt.Println(err)
	}
	err = LMDBEnv.SetMapSize(200 * 1024 * 1024 * 1024)
	if err != nil {
		fmt.Println(err)
	}
	err = LMDBEnv.SetMaxDBs(10)
	if err != nil {
		fmt.Println(err)
	}

	err = LMDBEnv.Open(lmdbPath, (lmdb.NoSubdir), 0600)
	if err != nil {
		fmt.Println(err)
	}

	err = LMDBEnv.View(func(txn *lmdb.Txn) (err error) {
		d, _ := txn.OpenDBI("treenode", 0)
		cur, err := txn.OpenCursor(d)
		if err != nil {
			fmt.Println(err)
		}
		defer cur.Close()

		x, err := cur.Count()
		fmt.Println(x)

		for {
			k, v, err := cur.Get(nil, nil, lmdb.Next)

			if err != nil {
				fmt.Println(err)
			}

			var t TreeNode
			_, err = t.Unmarshal(v)

			if err != nil {
				fmt.Println(err)
			}

			fmt.Printf("%s %+v\n", k, t)
		}

	})

}
