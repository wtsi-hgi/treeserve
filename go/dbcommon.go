package treeserve

import (
	"encoding"

	log "github.com/Sirupsen/logrus"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

type DBCommon struct {
	TS   *TreeServe
	Name string
	DBI  lmdb.DBI
}

// Reset the database to its initial state.
func (db *DBCommon) Reset() (err error) {
	ts := db.TS
	err = ts.LMDBEnv.Update(func(txn *lmdb.Txn) (err error) {
		err = txn.Drop(db.DBI, false)
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
				"db":  db,
			}).Error("failed to reset database")
		}
		return
	})
	if err != nil {
		return
	}
	dbiStat, err := db.Stat()
	log.WithFields(log.Fields{
		"dbiStat": dbiStat,
	}).Infof("reset database %v", db.Name)
	return
}

func (db *DBCommon) Stat() (dbiStat *lmdb.Stat, err error) {
	ts := db.TS
	err = ts.LMDBEnv.View(func(txn *lmdb.Txn) (err error) {
		dbiStat, err = txn.Stat(db.DBI)
		return
	})
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"db":  db,
		}).Fatal("failed to get stats for database")
	}
	return
}

func (db *DBCommon) HasKey(key encoding.BinaryMarshaler) (present bool, err error) {
	ts := db.TS
	keyBytes, err := key.MarshalBinary()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("could not marshal key")
	}
	present = false
	err = ts.LMDBEnv.View(func(txn *lmdb.Txn) (err error) {
		_, err = txn.Get(db.DBI, keyBytes)
		return
	})
	if err == nil {
		if ts.Debug {
			log.WithFields(log.Fields{
				"key": key,
			}).Debug("key already exists in database")
		}
		present = true
	} else if lmdb.IsNotFound(err) {
		err = nil
	} else {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("failed to check if database has key")
	}
	return
}
