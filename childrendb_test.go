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
	"crypto/md5"
	"io/ioutil"
	"syscall"
	"testing"
)

func TestChildrenDB(t *testing.T) {
	lmdbPath, err := ioutil.TempDir("", "genericdb_test_keysetdb")
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

	testKey := Md5Key(md5.Sum([]byte("testKey")))
	testChildKey1 := Md5Key(md5.Sum([]byte("testChildKey1")))
	err = ts.ChildrenDB.AddKeyToKeySet(&testKey, &testChildKey1)
	if err != nil {
		t.Errorf("failed to add key/child pair to database: %v", err)
	}

	keyset, err := ts.ChildrenDB.GetKeySet(&testKey)
	if err != nil {
		t.Errorf("failed to retrieved keyset from database: %v", err)
	}

	checkChildKey1 := keyset[0].(*Md5Key)
	if testChildKey1 != *checkChildKey1 {
		t.Errorf("retrieved keyset did not match: %v != %v", &testChildKey1, checkChildKey1)
	}
}
