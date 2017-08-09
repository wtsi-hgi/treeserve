package treeserve

import (
	"crypto/md5"
)

// +gen set
type Md5Key struct {
	k [16]byte
}

func (m *Md5Key) MarshalBinary() (data []byte, err error) {
	data = m.k[:]
	return
}

func (m *Md5Key) UnmarshalBinary(data []byte) (err error) {
	m.SetBytes(data)
	return
}

func (m *Md5Key) String() string {
	return string(m.k[:])
}

func (m *Md5Key) Sum(data []byte) {
	m.k = md5.Sum(data)
}

func (m *Md5Key) SetBytes(data []byte) {
	k := Md5Key{}
	copy(m.k[:], data)
	m = &k
	return
}

func (m *Md5Key) GetBytes() (data []byte) {
	data = m.k[:]
	return
}

func (m *Md5Key) GetFixedBytes() (data [16]byte) {
	data = m.k
	return
}
