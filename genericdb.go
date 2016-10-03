package treeserve

import (
	"encoding"

	log "github.com/Sirupsen/logrus"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

type GenericDB struct {
	DBCommon
	NewData NewData
}

func (gdb *GenericDB) Add(key encoding.BinaryMarshaler, data BinaryMarshalUnmarshaler, overwrite bool) (err error) {
	ts := gdb.TS
	keyBytes, err := key.MarshalBinary()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("could not marshal key")
	}
	dataBytes, err := data.MarshalBinary()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("could not marshal data")
	}
	err = ts.LMDBEnv.Update(func(txn *lmdb.Txn) (err error) {
		if !overwrite {
			// check if node already exists
			_, err = txn.Get(gdb.DBI, keyBytes)
			if err == nil {
				if ts.Debug {
					log.WithFields(log.Fields{
						"keyBytes": keyBytes,
					}).Debug("key already exists in database")
				}
				return
			}
			return
		}
		err = txn.Put(gdb.DBI, keyBytes, dataBytes, 0)
		if err != nil {
			log.WithFields(log.Fields{
				"gdb":      gdb,
				"keyBytes": keyBytes,
				"err":      err,
			}).Error("failed to add entry to database")
			return
		}
		if ts.Debug {
			log.WithFields(log.Fields{
				"keyBytes": keyBytes,
				"data":     data,
			}).Debug("added database entry")
		}
		return
	})
	return
}

func (gdb *GenericDB) Update(key encoding.BinaryMarshaler, update UpdateData) (err error) {
	ts := gdb.TS
	keyBytes, err := key.MarshalBinary()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("could not marshal key")
		return
	}
	var existing BinaryMarshalUnmarshaler
	var updated BinaryMarshalUnmarshaler
	err = ts.LMDBEnv.Update(func(txn *lmdb.Txn) (err error) {
		existingBytes, err := txn.Get(gdb.DBI, keyBytes)
		if lmdb.IsNotFound(err) {
			if ts.Debug {
				log.WithFields(log.Fields{
					"keyBytes": keyBytes,
					"update":   update,
				}).Debug("key not in database, calling update(nil)")
			}
			updated, err = update(nil)
			if err != nil {
				log.WithFields(log.Fields{
					"err":    err,
					"update": update,
				}).Error("update failed to process nil")
				return
			}
		} else if err == nil {
			existing = gdb.NewData()
			err = existing.UnmarshalBinary(existingBytes)
			if err != nil {
				log.WithFields(log.Fields{
					"existingBytes": existingBytes,
					"err":           err,
				}).Error("failed to unmarshall existing data")
				return
			}
			if ts.Debug {
				log.WithFields(log.Fields{
					"keyBytes": keyBytes,
					"existing": existing,
					"update":   update,
				}).Debug("key already exists in database, calling update(existing)")
			}
			updated, err = update(existing)
			if err != nil {
				log.WithFields(log.Fields{
					"existing": existing,
					"err":      err,
					"update":   update,
				}).Error("update failed to process existing data")
				return
			}
		} else if err != nil {
			log.WithFields(log.Fields{
				"err":      err,
				"gdb":      gdb,
				"keyBytes": keyBytes,
			}).Error("failed to get key from database")
			return
		}

		updatedBytes, err := updated.MarshalBinary()
		if err != nil {
			log.WithFields(log.Fields{
				"err":     err,
				"updated": updated,
			}).Error("could not marshal updated data")
			return
		}
		err = txn.Put(gdb.DBI, keyBytes, updatedBytes, 0)
		if err != nil {
			log.WithFields(log.Fields{
				"keyBytes":     keyBytes,
				"updatedBytes": updatedBytes,
				"err":          err,
			}).Error("failed to update database")
			return
		}
		if ts.Debug {
			log.WithFields(log.Fields{
				"keyBytes":     keyBytes,
				"updated":      updated,
				"updatedBytes": updatedBytes,
			}).Debug("updated data")
		}
		return
	})
	return
}

func (gdb *GenericDB) Get(key encoding.BinaryMarshaler) (data BinaryMarshalUnmarshaler, err error) {
	ts := gdb.TS
	keyBytes, err := key.MarshalBinary()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("could not marshal key")
	}
	var dataBytes []byte
	data = gdb.NewData()
	err = ts.LMDBEnv.View(func(txn *lmdb.Txn) (err error) {
		dataBytes, err = txn.Get(gdb.DBI, keyBytes)
		return
	})
	if lmdb.IsNotFound(err) {
		log.WithFields(log.Fields{
			"err": err,
			"gdb": gdb,
		}).Warning("key not found")
		return
	} else if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"gdb": gdb,
		}).Fatal("failed to get node from database")
	}
	err = data.UnmarshalBinary(dataBytes)
	if err != nil {
		log.WithFields(log.Fields{
			"err":       err,
			"dataBytes": dataBytes,
		}).Fatal("failed to unmarshal data")
	}
	return
}
