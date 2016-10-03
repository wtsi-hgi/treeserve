package treeserve

import "math/big"

type Bigint struct {
	*big.Int
}

func (bi *Bigint) MarshalBinary() (data []byte, err error) {
	data = bi.Bytes()
	return
}

func (bi *Bigint) UnmarshalBinary(data []byte) (err error) {
	bi.SetBytes(data)
	return
}
