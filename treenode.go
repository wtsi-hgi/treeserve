package treeserve

import (
	log "github.com/Sirupsen/logrus"
)

// TreeNode is defined in gencode schema

func (tn *TreeNode) MarshalBinary() (data []byte, err error) {
	data, err = tn.Marshal(nil)
	if err != nil {
		log.WithFields(log.Fields{
			"tn":  tn,
			"err": err,
		}).Error("failed to marshall treenode")
		return
	}
	return
}

func (tn *TreeNode) UnmarshalBinary(data []byte) (err error) {
	_, err = tn.Unmarshal(data)
	if err != nil {
		log.WithFields(log.Fields{
			"data": data,
			"err":  err,
		}).Error("failed to unmarshall data into treenode")
		return
	}
	return
}
