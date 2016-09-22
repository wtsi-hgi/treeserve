package treeserve

import (
	"io"
	"time"
	"unsafe"
)

var (
	_ = unsafe.Sizeof(0)
	_ = io.ReadFull
	_ = time.Now()
)

type TreeNode struct {
	Name         string
	ParentKey    [16]byte
	ChildrenKeys [][16]byte
}

func (d *TreeNode) Size() (s uint64) {

	{
		l := uint64(len(d.Name))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		s += 16
	}
	{
		l := uint64(len(d.ChildrenKeys))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}

		for _ = range d.ChildrenKeys {

			{
				s += 16
			}

		}

	}
	return
}
func (d *TreeNode) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{
		l := uint64(len(d.Name))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.Name)
		i += l
	}
	{
		copy(buf[i+0:], d.ParentKey[:])
		i += 16
	}
	{
		l := uint64(len(d.ChildrenKeys))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		for k := range d.ChildrenKeys {

			{
				copy(buf[i+0:], d.ChildrenKeys[k][:])
				i += 16
			}

		}
	}
	return buf[:i+0], nil
}

func (d *TreeNode) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.Name = string(buf[i+0 : i+0+l])
		i += l
	}
	{
		copy(d.ParentKey[:], buf[i+0:])
		i += 16
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.ChildrenKeys)) >= l {
			d.ChildrenKeys = d.ChildrenKeys[:l]
		} else {
			d.ChildrenKeys = make([][16]byte, l)
		}
		for k := range d.ChildrenKeys {

			{
				copy(d.ChildrenKeys[k][:], buf[i+0:])
				i += 16
			}

		}
	}
	return i + 0, nil
}
