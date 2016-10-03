package treeserve

import (
	"crypto/md5"
	"strings"
)

// type StatMapping defined in gencode schema

func (statMapping *StatMapping) GetKey() (statMappingKey Md5Key) {
	statMappingKey = Md5Key(md5.Sum([]byte(strings.Join([]string{statMapping.User, statMapping.Group, statMapping.Tag}, "|"))))
	return
}
