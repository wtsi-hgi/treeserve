package treeserve

/*
type Bigint struct {
	I *big.Int
}

// NewBigint returns a pointer to a Bigint initialised to zero
func NewBigint() *Bigint {
	bi := Bigint{}
	bi.I = &big.Int{}
	return &bi
}

// MarshalBinary returns a Bigint converted to bytes for LMDB storage
func (bi *Bigint) MarshalBinary() (data []byte, err error) {
	data = bi.I.Bytes()
	return
}

func (bi *Bigint) UnmarshalBinary(data []byte) (err error) {
	bi.I.SetBytes(data)
	return
}

func (bi *Bigint) SetUint64(x uint64) {
	bi.I.SetUint64(x)
}

func (bi *Bigint) SetInt64(x int64) {
	bi.I.SetInt64(x)
}
func (bi *Bigint) SetString(x string) {
	bi.I.SetString(x, 10)
}

func (bi *Bigint) Mul(x, y *Bigint) {
	bi.I.Mul(x.I, y.I)
}

func (bi *Bigint) Add(x, y *Bigint) {
	bi.I.Add(x.I, y.I)
}

func (bi *Bigint) Subtract(x, y *Bigint) {
	bi.I.Sub(x.I, y.I)
}

func (bi *Bigint) Text(base int) (s string) {
	s = bi.I.Text(base)
	return
}
func Divide(x, y *Bigint) (f string) {
	f1 := new(big.Float).SetInt(x.I)
	f2 := new(big.Float).SetInt(y.I)
	f3 := new(big.Float).Quo(f1, f2)
	f = f3.Text('e', 16)
	return

}

func (bi *Bigint) isZero() bool {
	x := NewBigint()
	return (bi.I.Cmp(x.I) == 0)
}

func (bi *Bigint) isNegative() bool {
	x := NewBigint()
	return (bi.I.Cmp(x.I) <= 0)
}

// Equals returne true if two Bigints are equal
func (bi *Bigint) Equals(x *Bigint) (ans bool) {
	ans = (bi.I.Cmp(x.I) == 0)
	return
}

*/
