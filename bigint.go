package treeserve

import "math/big"

type Bigint struct {
	i *big.Int
}

func NewBigint() *Bigint {
	bi := Bigint{}
	bi.i = &big.Int{}
	return &bi
}

func (bi *Bigint) MarshalBinary() (data []byte, err error) {
	data = bi.i.Bytes()
	return
}

func (bi *Bigint) UnmarshalBinary(data []byte) (err error) {
	bi.i.SetBytes(data)
	return
}

func (bi *Bigint) SetUint64(x uint64) {
	bi.i.SetUint64(x)
}

func (bi *Bigint) SetInt64(x int64) {
	bi.i.SetInt64(x)
}

func (bi *Bigint) Mul(x, y *Bigint) {
	bi.i.Mul(x.i, y.i)
}

func (bi *Bigint) Add(x, y *Bigint) {
	bi.i.Add(x.i, y.i)
}

func (bi *Bigint) Text(base int) (s string) {
	s = bi.i.Text(base)
	return
}
