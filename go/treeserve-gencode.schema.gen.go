package treeserve

import (
	"errors"
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
	if len(buf) == 0 {
		return 0, errors.New("Nothing to unmarshall")
	}
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
	FileSize         uint64
	Uid              uint64
	Gid              uint64
	AccessTime       int64
	ModificationTime int64
	CreationTime     int64
	FileType         byte
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

		buf[0+0] = byte(d.FileSize >> 0)

		buf[1+0] = byte(d.FileSize >> 8)

		buf[2+0] = byte(d.FileSize >> 16)

		buf[3+0] = byte(d.FileSize >> 24)

		buf[4+0] = byte(d.FileSize >> 32)

		buf[5+0] = byte(d.FileSize >> 40)

		buf[6+0] = byte(d.FileSize >> 48)

		buf[7+0] = byte(d.FileSize >> 56)

	}
	{

		buf[0+8] = byte(d.Uid >> 0)

		buf[1+8] = byte(d.Uid >> 8)

		buf[2+8] = byte(d.Uid >> 16)

		buf[3+8] = byte(d.Uid >> 24)

		buf[4+8] = byte(d.Uid >> 32)

		buf[5+8] = byte(d.Uid >> 40)

		buf[6+8] = byte(d.Uid >> 48)

		buf[7+8] = byte(d.Uid >> 56)

	}
	{

		buf[0+16] = byte(d.Gid >> 0)

		buf[1+16] = byte(d.Gid >> 8)

		buf[2+16] = byte(d.Gid >> 16)

		buf[3+16] = byte(d.Gid >> 24)

		buf[4+16] = byte(d.Gid >> 32)

		buf[5+16] = byte(d.Gid >> 40)

		buf[6+16] = byte(d.Gid >> 48)

		buf[7+16] = byte(d.Gid >> 56)

	}
	{

		buf[0+24] = byte(d.AccessTime >> 0)

		buf[1+24] = byte(d.AccessTime >> 8)

		buf[2+24] = byte(d.AccessTime >> 16)

		buf[3+24] = byte(d.AccessTime >> 24)

		buf[4+24] = byte(d.AccessTime >> 32)

		buf[5+24] = byte(d.AccessTime >> 40)

		buf[6+24] = byte(d.AccessTime >> 48)

		buf[7+24] = byte(d.AccessTime >> 56)

	}
	{

		buf[0+32] = byte(d.ModificationTime >> 0)

		buf[1+32] = byte(d.ModificationTime >> 8)

		buf[2+32] = byte(d.ModificationTime >> 16)

		buf[3+32] = byte(d.ModificationTime >> 24)

		buf[4+32] = byte(d.ModificationTime >> 32)

		buf[5+32] = byte(d.ModificationTime >> 40)

		buf[6+32] = byte(d.ModificationTime >> 48)

		buf[7+32] = byte(d.ModificationTime >> 56)

	}
	{

		buf[0+40] = byte(d.CreationTime >> 0)

		buf[1+40] = byte(d.CreationTime >> 8)

		buf[2+40] = byte(d.CreationTime >> 16)

		buf[3+40] = byte(d.CreationTime >> 24)

		buf[4+40] = byte(d.CreationTime >> 32)

		buf[5+40] = byte(d.CreationTime >> 40)

		buf[6+40] = byte(d.CreationTime >> 48)

		buf[7+40] = byte(d.CreationTime >> 56)

	}
	{
		buf[48] = d.FileType
	}
	return buf[:i+49], nil
}

func (d *NodeStats) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{

		d.FileSize = 0 | (uint64(buf[0+0]) << 0) | (uint64(buf[1+0]) << 8) | (uint64(buf[2+0]) << 16) | (uint64(buf[3+0]) << 24) | (uint64(buf[4+0]) << 32) | (uint64(buf[5+0]) << 40) | (uint64(buf[6+0]) << 48) | (uint64(buf[7+0]) << 56)

	}
	{

		d.Uid = 0 | (uint64(buf[0+8]) << 0) | (uint64(buf[1+8]) << 8) | (uint64(buf[2+8]) << 16) | (uint64(buf[3+8]) << 24) | (uint64(buf[4+8]) << 32) | (uint64(buf[5+8]) << 40) | (uint64(buf[6+8]) << 48) | (uint64(buf[7+8]) << 56)

	}
	{

		d.Gid = 0 | (uint64(buf[0+16]) << 0) | (uint64(buf[1+16]) << 8) | (uint64(buf[2+16]) << 16) | (uint64(buf[3+16]) << 24) | (uint64(buf[4+16]) << 32) | (uint64(buf[5+16]) << 40) | (uint64(buf[6+16]) << 48) | (uint64(buf[7+16]) << 56)

	}
	{

		d.AccessTime = 0 | (int64(buf[0+24]) << 0) | (int64(buf[1+24]) << 8) | (int64(buf[2+24]) << 16) | (int64(buf[3+24]) << 24) | (int64(buf[4+24]) << 32) | (int64(buf[5+24]) << 40) | (int64(buf[6+24]) << 48) | (int64(buf[7+24]) << 56)

	}
	{

		d.ModificationTime = 0 | (int64(buf[0+32]) << 0) | (int64(buf[1+32]) << 8) | (int64(buf[2+32]) << 16) | (int64(buf[3+32]) << 24) | (int64(buf[4+32]) << 32) | (int64(buf[5+32]) << 40) | (int64(buf[6+32]) << 48) | (int64(buf[7+32]) << 56)

	}
	{

		d.CreationTime = 0 | (int64(buf[0+40]) << 0) | (int64(buf[1+40]) << 8) | (int64(buf[2+40]) << 16) | (int64(buf[3+40]) << 24) | (int64(buf[4+40]) << 32) | (int64(buf[5+40]) << 40) | (int64(buf[6+40]) << 48) | (int64(buf[7+40]) << 56)

	}
	{
		d.FileType = buf[48]
	}
	return i + 49, nil
}

type StatMapping struct {
	User  string
	Group string
	Tag   string
}

func (d *StatMapping) Size() (s uint64) {

	{
		l := uint64(len(d.User))

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
		l := uint64(len(d.Group))

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
		l := uint64(len(d.Tag))

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
	return
}
func (d *StatMapping) Marshal(buf []byte) ([]byte, error) {
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
		l := uint64(len(d.User))

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
		copy(buf[i+0:], d.User)
		i += l
	}
	{
		l := uint64(len(d.Group))

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
		copy(buf[i+0:], d.Group)
		i += l
	}
	{
		l := uint64(len(d.Tag))

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
		copy(buf[i+0:], d.Tag)
		i += l
	}
	return buf[:i+0], nil
}

func (d *StatMapping) Unmarshal(buf []byte) (uint64, error) {
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
		d.User = string(buf[i+0 : i+0+l])
		i += l
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
		d.Group = string(buf[i+0 : i+0+l])
		i += l
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
		d.Tag = string(buf[i+0 : i+0+l])
		i += l
	}
	return i + 0, nil
}
