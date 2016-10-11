package treeserve

import (
	"strings"

	log "github.com/Sirupsen/logrus"
)

// type StatMapping defined in gencode schema

func NewStatMapping() *StatMapping {
	return &StatMapping{}
}

func (sm *StatMapping) GetKey() (statMappingKey Md5Key) {
	statMappingKey = Md5Key{}
	statMappingKey.Sum([]byte(strings.Join([]string{sm.User, sm.Group, sm.Tag}, "|")))
	return
}

func (sm *StatMapping) MarshalBinary() (data []byte, err error) {
	data, err = sm.Marshal(nil)
	if err != nil {
		log.WithFields(log.Fields{
			"sm":  sm,
			"err": err,
		}).Error("failed to marshall statmapping")
		return
	}
	return
}

func (sm *StatMapping) UnmarshalBinary(data []byte) (err error) {
	_, err = sm.Unmarshal(data)
	if err != nil {
		log.WithFields(log.Fields{
			"data": data,
			"err":  err,
		}).Error("failed to unmarshall data into statmapping")
		return
	}
	return
}
