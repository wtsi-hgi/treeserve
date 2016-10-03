package treeserve

type Md5Key [16]byte

func (k *Md5Key) MarshalBinary() (data []byte, err error) {
	data = k[:]
	return
}

func (k *Md5Key) String() string {
	return string(k[:])
}
