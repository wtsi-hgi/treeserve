package treeserve

import (
	"bytes"
	"encoding"

	log "github.com/Sirupsen/logrus"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

type KeySetDB struct {
	DBCommon
}

func (ksdb *KeySetDB) AddKeyToKeySet(key encoding.BinaryMarshaler, setKey encoding.BinaryMarshaler) (err error) {
	ts := ksdb.TS
	keyBytes, err := key.MarshalBinary()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("could not marshal key")
		return
	}
	setKeyBytes, err := setKey.MarshalBinary()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("could not marshal setKey")
		return
	}

	err = ts.LMDBEnv.Update(func(txn *lmdb.Txn) (err error) {
		err = txn.Put(ksdb.DBI, keyBytes, setKeyBytes, lmdb.NoDupData)
		return
	})
	if lmdb.IsErrno(err, lmdb.KeyExist) {
		if ts.Debug {
			log.WithFields(log.Fields{
				"key":    key,
				"setKey": setKey,
			}).Debug("setKey is already in the key set for key")
		}
		err = nil
	} else if err != nil {
		log.WithFields(log.Fields{
			"key":    key,
			"setKey": setKey,
			"err":    err,
		}).Error("failed to add setKey to key set for key")
		return
	}
	return
}

func (ksdb *KeySetDB) GetKeySet(key encoding.BinaryMarshaler) (keySetKeys []encoding.BinaryMarshaler, err error) {
	ts := ksdb.TS
	if ts.Debug {
		log.WithFields(log.Fields{
			"key": key,
		}).Debug("about to start read transaction")
	}
	keyBytes, err := key.MarshalBinary()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("could not marshal key")
	}
	err = ts.LMDBEnv.View(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(ksdb.DBI)
		if err != nil {
			log.WithFields(log.Fields{
				"err":  err,
				"ksdb": ksdb,
				"ts":   ts,
			}).Error("failed to open DBI cursor")
			return
		}
		defer cur.Close()

		if ts.Debug {
			log.WithFields(log.Fields{
				"key": key,
			}).Debug("moving cursor to start of set")
		}
		_, firstKeySetKey, err := cur.Get(keyBytes, nil, lmdb.Set)
		if lmdb.IsNotFound(err) {
			if ts.Debug {
				log.WithFields(log.Fields{
					"err": err,
					"key": key,
				}).Debug("no keys in keyset")
			}
			err = nil
			return
		}
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
				"key": key,
			}).Error("failed to get key set")
		}
		stride := len(firstKeySetKey)
		if ts.Debug {
			log.WithFields(log.Fields{
				"key":            key,
				"stride":         stride,
				"firstKeySetKey": firstKeySetKey,
			}).Debug("have stride, getting keyset")
		}
		k, v, err := cur.Get(nil, nil, lmdb.NextMultiple)
		if lmdb.IsNotFound(err) {
			log.Debug("nextmultiple not found, this key only has one key in the keyset")
			keySetKey := Md5Key{}
			copy(keySetKey[:], firstKeySetKey)
			keySetKeys = append(keySetKeys, &keySetKey)
			err = nil
			return
		}
		keySetCount := 0
		keySetPageCount := 0
		for {
			if lmdb.IsNotFound(err) {
				if ts.Debug {
					log.WithFields(log.Fields{
						"k":               k,
						"v":               v,
						"err":             err,
						"keySetCount":     keySetCount,
						"keySetPageCount": keySetPageCount,
					}).Debug("no more sets of key sets")
				}
				err = nil
				break
			}
			if err != nil {
				log.WithFields(log.Fields{
					"err": err,
					"key": key,
					"k":   k,
				}).Fatal("failed to iterate over key sets")
			}
			if !bytes.Equal(k, keyBytes) {
				if ts.Debug {
					log.WithFields(log.Fields{
						"key":             key,
						"k":               k,
						"keySetPageCount": keySetPageCount,
						"keySetCount":     keySetCount,
					}).Debug("got unexpected key")
				}
				break
			}
			multi := lmdb.WrapMulti(v, stride)
			if ts.Debug {
				log.WithFields(log.Fields{
					"multi":       multi,
					"multi.Len()": multi.Len(),
					"v":           v,
					"stride":      stride,
				}).Debug("have wrapped multi")
			}
			for i := 0; i < multi.Len(); i++ {
				keySetCount++
				keySetKey := Md5Key{}
				copy(keySetKey[:], multi.Val(i))
				if ts.Debug {
					log.WithFields(log.Fields{
						"keySetKey":   keySetKey,
						"i":           i,
						"k":           k,
						"keySetCount": keySetCount,
					}).Debug("got keySet, appending")
				}
				keySetKeys = append(keySetKeys, &keySetKey)
			}
			k, v, err = cur.Get(nil, nil, lmdb.NextMultiple)
			keySetPageCount++
		}
		return
	})
	return
}
