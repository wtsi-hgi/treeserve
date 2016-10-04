package treeserve

type Md5Key [16]byte

func (k *Md5Key) MarshalBinary() (data []byte, err error) {
	data = k[:]
	return
}

func (k *Md5Key) UnmarshalBinary(data []byte) (err error) {
	md5Key := Md5Key{}
	copy(md5Key[:], data)
	k = &md5Key
	return
}

func (k *Md5Key) String() string {
	return string(k[:])
}
