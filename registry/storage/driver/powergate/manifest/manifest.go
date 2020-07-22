package powManifest

import (
	"errors"
	"time"
)

type PowInode struct {
	Name     string
	Hash     string
	JobID    string
	Dir      bool
	Sz       int64
	Children []string
	Created  int64
	Updated  int64
}

func (p *PowInode) Path() string {
	return p.Name
}
func (p *PowInode) Size() int64 {
	return p.Sz
}
func (p *PowInode) ModTime() time.Time {
	return time.Unix(p.Updated, 0)
}
func (p *PowInode) IsDir() bool {
	return p.Dir
}

var ErrNotFound error = errors.New("Key not found")

type PowManifest interface {
	Read(*PowInode) error
	Create(*PowInode) error
	Update(*PowInode) error
	Delete(*PowInode) error
}
