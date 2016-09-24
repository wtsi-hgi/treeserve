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
	Name      string
	ParentKey [16]byte
	Stats     NodeStats
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
		s += d.Stats.Size()
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
		nbuf, err := d.Stats.Marshal(buf[i+0:])
		if err != nil {
			return nil, err
		}
		i += uint64(len(nbuf))
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
		ni, err := d.Stats.Unmarshal(buf[i+0:])
		if err != nil {
			return 0, err
		}
		i += ni
	}
	return i + 0, nil
}

type NodeStats struct {
	size             uint64
	uid              uint64
	gid              uint64
	accessTime       int64
	modificationTime int64
	creationTime     int64
	fileType         byte
}

func (d *NodeStats) Size() (s uint64) {

	s += 49
	return
}
func (d *NodeStats) Marshal(buf []byte) ([]byte, error) {
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

		buf[0+0] = byte(d.size >> 0)

		buf[1+0] = byte(d.size >> 8)

		buf[2+0] = byte(d.size >> 16)

		buf[3+0] = byte(d.size >> 24)

		buf[4+0] = byte(d.size >> 32)

		buf[5+0] = byte(d.size >> 40)

		buf[6+0] = byte(d.size >> 48)

		buf[7+0] = byte(d.size >> 56)

	}
	{

		buf[0+8] = byte(d.uid >> 0)

		buf[1+8] = byte(d.uid >> 8)

		buf[2+8] = byte(d.uid >> 16)

		buf[3+8] = byte(d.uid >> 24)

		buf[4+8] = byte(d.uid >> 32)

		buf[5+8] = byte(d.uid >> 40)

		buf[6+8] = byte(d.uid >> 48)

		buf[7+8] = byte(d.uid >> 56)

	}
	{

		buf[0+16] = byte(d.gid >> 0)

		buf[1+16] = byte(d.gid >> 8)

		buf[2+16] = byte(d.gid >> 16)

		buf[3+16] = byte(d.gid >> 24)

		buf[4+16] = byte(d.gid >> 32)

		buf[5+16] = byte(d.gid >> 40)

		buf[6+16] = byte(d.gid >> 48)

		buf[7+16] = byte(d.gid >> 56)

	}
	{

		buf[0+24] = byte(d.accessTime >> 0)

		buf[1+24] = byte(d.accessTime >> 8)

		buf[2+24] = byte(d.accessTime >> 16)

		buf[3+24] = byte(d.accessTime >> 24)

		buf[4+24] = byte(d.accessTime >> 32)

		buf[5+24] = byte(d.accessTime >> 40)

		buf[6+24] = byte(d.accessTime >> 48)

		buf[7+24] = byte(d.accessTime >> 56)

	}
	{

		buf[0+32] = byte(d.modificationTime >> 0)

		buf[1+32] = byte(d.modificationTime >> 8)

		buf[2+32] = byte(d.modificationTime >> 16)

		buf[3+32] = byte(d.modificationTime >> 24)

		buf[4+32] = byte(d.modificationTime >> 32)

		buf[5+32] = byte(d.modificationTime >> 40)

		buf[6+32] = byte(d.modificationTime >> 48)

		buf[7+32] = byte(d.modificationTime >> 56)

	}
	{

		buf[0+40] = byte(d.creationTime >> 0)

		buf[1+40] = byte(d.creationTime >> 8)

		buf[2+40] = byte(d.creationTime >> 16)

		buf[3+40] = byte(d.creationTime >> 24)

		buf[4+40] = byte(d.creationTime >> 32)

		buf[5+40] = byte(d.creationTime >> 40)

		buf[6+40] = byte(d.creationTime >> 48)

		buf[7+40] = byte(d.creationTime >> 56)

	}
	{
		buf[48] = d.fileType
	}
	return buf[:i+49], nil
}

func (d *NodeStats) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{

		d.size = 0 | (uint64(buf[0+0]) << 0) | (uint64(buf[1+0]) << 8) | (uint64(buf[2+0]) << 16) | (uint64(buf[3+0]) << 24) | (uint64(buf[4+0]) << 32) | (uint64(buf[5+0]) << 40) | (uint64(buf[6+0]) << 48) | (uint64(buf[7+0]) << 56)

	}
	{

		d.uid = 0 | (uint64(buf[0+8]) << 0) | (uint64(buf[1+8]) << 8) | (uint64(buf[2+8]) << 16) | (uint64(buf[3+8]) << 24) | (uint64(buf[4+8]) << 32) | (uint64(buf[5+8]) << 40) | (uint64(buf[6+8]) << 48) | (uint64(buf[7+8]) << 56)

	}
	{

		d.gid = 0 | (uint64(buf[0+16]) << 0) | (uint64(buf[1+16]) << 8) | (uint64(buf[2+16]) << 16) | (uint64(buf[3+16]) << 24) | (uint64(buf[4+16]) << 32) | (uint64(buf[5+16]) << 40) | (uint64(buf[6+16]) << 48) | (uint64(buf[7+16]) << 56)

	}
	{

		d.accessTime = 0 | (int64(buf[0+24]) << 0) | (int64(buf[1+24]) << 8) | (int64(buf[2+24]) << 16) | (int64(buf[3+24]) << 24) | (int64(buf[4+24]) << 32) | (int64(buf[5+24]) << 40) | (int64(buf[6+24]) << 48) | (int64(buf[7+24]) << 56)

	}
	{

		d.modificationTime = 0 | (int64(buf[0+32]) << 0) | (int64(buf[1+32]) << 8) | (int64(buf[2+32]) << 16) | (int64(buf[3+32]) << 24) | (int64(buf[4+32]) << 32) | (int64(buf[5+32]) << 40) | (int64(buf[6+32]) << 48) | (int64(buf[7+32]) << 56)

	}
	{

		d.creationTime = 0 | (int64(buf[0+40]) << 0) | (int64(buf[1+40]) << 8) | (int64(buf[2+40]) << 16) | (int64(buf[3+40]) << 24) | (int64(buf[4+40]) << 32) | (int64(buf[5+40]) << 40) | (int64(buf[6+40]) << 48) | (int64(buf[7+40]) << 56)

	}
	{
		d.fileType = buf[48]
	}
	return i + 49, nil
}
