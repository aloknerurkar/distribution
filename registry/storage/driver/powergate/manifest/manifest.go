package powManifest

import (
	"encoding/json"
	"errors"
	"fmt"
	dsstore "github.com/StreamSpace/ss-ds-store"
	"github.com/StreamSpace/ss-store"
	badger "github.com/ipfs/go-ds-badger"
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

func (p *PowInode) GetId() string {
	return p.Name
}

func (p *PowInode) GetNamespace() string {
	return "powInode"
}

func (p *PowInode) SetCreated(t int64) {
	p.Created = t
}

func (p *PowInode) SetUpdated(t int64) {
	p.Updated = t
}

func (p *PowInode) Marshal() ([]byte, error) {
	return json.Marshal(p)
}

func (p *PowInode) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, p)
}

var ErrNotFound error = errors.New("Key not found")

type PowManifest interface {
	store.Store
}

func Init(root string) (PowManifest, error) {
	ds, err := badger.NewDatastore(root, &badger.DefaultOptions)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Created new store at %s", root)
	st, _ := dsstore.NewDataStore(&dsstore.DSConfig{
		DS: ds,
	})
	return st, nil
}
