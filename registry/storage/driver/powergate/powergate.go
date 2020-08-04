package powergate

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/factory"
	manifest "github.com/docker/distribution/registry/storage/driver/powergate/manifest"
	"github.com/ipfs/go-cid"
	pow "github.com/textileio/powergate/api/client"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"strings"
	"sync"
	"time"
)

const (
	driverName = "pow"
	rootPath   = "/"
)

func init() {
	factory.Register(driverName, &powFactory{})
}

type powFactory struct{}

func (p *powFactory) Create(params map[string]interface{}) (storagedriver.StorageDriver, error) {
	return New(params)
}

var _ storagedriver.StorageDriver = &powergateDriver{}

type powergateDriver struct {
	pm    manifest.PowManifest
	api   *pow.Client
	token string
}

func New(params map[string]interface{}) (storagedriver.StorageDriver, error) {
	api := params["powaddress"]
	if api == nil {
		return nil, fmt.Errorf("Pow instance address required")
	}
	c, err := pow.NewClient(api.(string), grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Failed to create new POW client Err:%s", err.Error())
	}
	token := params["token"]
	if token == nil {
		_, token, err = c.FFS.Create(context.Background())
		if err != nil {
			return nil, fmt.Errorf("Failed to create new POW instance Err:%s", err.Error())
		}
	}
	fmt.Printf("TOKEN %v\n", token)
	root := params["powpath"]
	if root == nil {
		usr, err := user.Current()
		if err != nil {
			return nil, fmt.Errorf("Failed getting user Err:%s", err.Error())
		}
		root = usr.HomeDir + string(os.PathSeparator) + ".pow_driver"
	}
	m, err := manifest.Init(root.(string))
	if err != nil {
		return nil, fmt.Errorf("Failed creating manifest Err:%s", err.Error())
	}
	// bkp := params["manifest"]
	// if bkp != nil {
	// 	bkpCid, err := cid.Decode(bkp.(string))
	// 	if err != nil {

	// 	}
	// 	rdr, err := c.FFS.Get(context.Background(), bkpCid)
	// 	if err != nil {

	// 	}
	// }
	return &powergateDriver{
		api:   c,
		pm:    m,
		token: token.(string),
	}, nil
}

func (p *powergateDriver) reqCtx(parent context.Context) context.Context {
	fmt.Printf("reqCtx %s\n", p.token)
	return context.WithValue(parent, pow.AuthKey, p.token)
}

func (p *powergateDriver) Name() string {
	return driverName
}

func (p *powergateDriver) GetContent(
	ctx context.Context,
	path string,
) ([]byte, error) {
	fmt.Println("GetContent")
	rdr, err := p.Reader(p.reqCtx(ctx), path, 0)
	if err != nil {
		return nil, fmt.Errorf("pow: Failed to get content Err:%s", err)
	}
	return ioutil.ReadAll(rdr)
}

func (p *powergateDriver) Reader(
	ctx context.Context,
	path string,
	offset int64,
) (io.ReadCloser, error) {
	fmt.Println("Reader")
	ino := &manifest.PowInode{
		Name: fullPath(path),
	}
	err := p.pm.Read(ino)
	if err != nil {
		return nil, fmt.Errorf("pow: Failed reading node Err:%s", err.Error())
	}
	ctCid, err := cid.Decode(ino.Hash)
	if err != nil {
		return nil, fmt.Errorf("pow: Failed decoding CID from node Err:%s", err.Error())
	}
	rdr, err := p.api.FFS.Get(p.reqCtx(ctx), ctCid)
	if err != nil {
		return nil, fmt.Errorf("pow: Failed getting data from FFS Err:%s", err.Error())
	}
	if offset > 0 {
		newRdr := bufio.NewReader(rdr)
		_, err = newRdr.Discard(int(offset) - 1)
		if err != nil {
			return nil, fmt.Errorf("pow: Failed to go to offset Err:%s", err.Error())
		}
		return ioutil.NopCloser(newRdr), nil
	}
	return ioutil.NopCloser(rdr), nil
}

func (p *powergateDriver) PutContent(
	ctx context.Context,
	path string,
	content []byte,
) error {
	fmt.Println("PutContent")
	// Read parent inode. Create if it doesn't exist
	parentIno := &manifest.PowInode{
		Name: getParentPath(fullPath(path)),
	}
	err := p.readOrCreate(parentIno)
	if err != nil {
		return err
	}
	ino := &manifest.PowInode{
		Name: fullPath(path),
	}
	err = p.pm.Read(ino)
	if err != nil {
		err = p.pm.Create(ino)
		if err != nil {
			return fmt.Errorf("pow: Failed creating new node Err:%s", err.Error())
		}
	}
	err = p.addOrReplaceNode(p.reqCtx(ctx), ino, bytes.NewBuffer(content))
	if err != nil {
		return err
	}
	err = p.updateParent(parentIno, ino.Path())
	if err != nil {
		return err
	}
	return nil
}

func (p *powergateDriver) Writer(
	ctx context.Context,
	path string,
	app bool,
) (storagedriver.FileWriter, error) {
	fmt.Println("Writer", path)
	// Read parent inode. Create if it doesn't exist
	parentIno := &manifest.PowInode{
		Name: getParentPath(fullPath(path)),
	}
	err := p.readOrCreate(parentIno)
	if err != nil {
		return nil, err
	}
	fIno := &manifest.PowInode{
		Name: fullPath(path),
	}
	if app {
		err = p.pm.Read(fIno)
		if err != nil {
			return nil, fmt.Errorf("pow: Failed to read node Err:%s", err.Error())
		}
	} else {
		err = p.pm.Create(fIno)
		if err != nil {
			return nil, fmt.Errorf("pow: Failed to create node Err:%s", err.Error())
		}
	}
	pf := &powFile{
		driver: p,
		nd:     fIno,
		buf:    new(bytes.Buffer),
	}
	r := &customReader{buf: pf.buf, done: make(chan bool, 1)}
	if app {
		if len(fIno.Hash) > 0 {
			// Read old contents back to reader to replay. As content will be same
			// this replay operation should serve the purpose of linking the CIDs to
			// the new item DAG
			ctCid, err := cid.Decode(fIno.Hash)
			if err != nil {
				return nil, fmt.Errorf("pow: Failed decoding previous CID Err:%s", err.Error())
			}
			rdr, err := p.api.FFS.Get(p.reqCtx(ctx), ctCid)
			if err != nil {
				return nil, fmt.Errorf("pow: Failed getting previous CID Err:%s", err.Error())
			}
			b := make([]byte, 32768)
			for {
				n, err := rdr.Read(b)
				if err == nil {
					_, err = pf.buf.Write(b)
				}
				if err != nil {
					return nil, fmt.Errorf(
						"pow: Failed replaying previous CID content Err:%s", err.Error())
				}
				if n < 32768 {
					break
				}
			}
		}
	}
	pf.startWorker(p.reqCtx(ctx), r)
	return pf, nil
}

func (p *powergateDriver) Stat(
	ctx context.Context,
	path string,
) (storagedriver.FileInfo, error) {
	fmt.Println("Stat")
	ino := &manifest.PowInode{
		Name: fullPath(path),
	}
	err := p.pm.Read(ino)
	if err != nil {
		return nil, fmt.Errorf("pow: Failed to read node Err:%s", err.Error())
	}
	return ino, nil
}

func (p *powergateDriver) List(ctx context.Context, path string) ([]string, error) {
	fmt.Println("List")
	ino := &manifest.PowInode{
		Name: fullPath(path),
	}
	err := p.pm.Read(ino)
	if err != nil {
		return nil, fmt.Errorf("pow: Failed to read node Err:%s", err.Error())
	}
	return ino.Children, nil
}

func (p *powergateDriver) Move(
	ctx context.Context,
	sourcePath string,
	destPath string,
) error {
	fmt.Println("Move")
	err := p.cleanupParent(fullPath(sourcePath))
	if err != nil {
		return err
	}
	newParent := &manifest.PowInode{
		Name: getParentPath(fullPath(destPath)),
	}
	err = p.readOrCreate(newParent)
	if err != nil {
		return err
	}
	err = p.updateParent(newParent, fullPath(destPath))
	if err != nil {
		return err
	}
	return nil
}

func (p *powergateDriver) Delete(ctx context.Context, path string) error {
	fmt.Println("Delete")
	ino := &manifest.PowInode{
		Name: fullPath(path),
	}
	err := p.pm.Read(ino)
	if err != nil {
		return fmt.Errorf("pow: Failed reading node Err:%s", err.Error())
	}
	if ino.IsDir() {
		fmt.Println("Directory delete")
		return nil
	}
	defer fmt.Println("Delete done")
	ctCid, err := cid.Decode(ino.Hash)
	if err != nil {
		return fmt.Errorf("pow: Failed decoding node CID Err:%s", err.Error())
	}
	err = p.api.FFS.Remove(p.reqCtx(ctx), ctCid)
	if err != nil {
		return fmt.Errorf("pow: Failed deleting from pow Err:%s", err.Error())
	}
	err = p.pm.Delete(ino)
	if err != nil {
		return fmt.Errorf("pow: Failed deleting node Err:%s", err.Error())
	}
	err = p.cleanupParent(ino.Path())
	if err != nil {
		return err
	}
	return nil
}

func (p *powergateDriver) readOrCreate(ino *manifest.PowInode) error {
	err := p.pm.Read(ino)
	if err != nil {
		ino, err = p.mkdirAll(ino.Path())
		if err != nil {
			return fmt.Errorf("pow: Failed creating parent node Err:%s", err.Error())
		}
	}
	return nil
}

func (p *powergateDriver) cleanupParent(fp string) error {
	parentIno := &manifest.PowInode{
		Name: getParentPath(fp),
	}
	err := p.pm.Read(parentIno)
	if err != nil {
		return fmt.Errorf("pow: Failed reading parent node Err:%s", err.Error())
	}
	found := false
	for i, v := range parentIno.Children {
		if v == fp {
			parentIno.Children = append(parentIno.Children[:i], parentIno.Children[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("pow: Cannot find node in parent")
	}
	err = p.pm.Update(parentIno)
	if err != nil {
		return fmt.Errorf("pow: Failed updating parent node Err:%s", err.Error())
	}
	return nil
}

func (p *powergateDriver) addOrReplaceNode(
	ctx context.Context,
	fIno *manifest.PowInode,
	rdr io.Reader,
) error {
	c, err := p.api.FFS.Stage(ctx, rdr)
	if err != nil {
		return fmt.Errorf("pow: Failed adding to Hot storage Err:%s", err.Error())
	}
	fmt.Println("Current hash", fIno.Hash)
	if len(fIno.Hash) > 0 {
		c1, err := cid.Decode(fIno.Hash)
		if err != nil {
			return fmt.Errorf("pow: Failed to decode existing hash Err:%s", err.Error())
		}
		jb, err := p.api.FFS.Replace(ctx, c1, *c)
		if err != nil {
			return fmt.Errorf("pow: Failed to replace existing hash Err:%s", err.Error())
		}
		fIno.JobID = string(jb)
	} else {
		jb, err := p.api.FFS.PushStorageConfig(ctx, *c)
		if err != nil {
			return fmt.Errorf("pow: Failed to replace existing hash Err:%s", err.Error())
		}
		fIno.JobID = string(jb)
	}
	fIno.Hash = c.String()
	fmt.Println("Adding IPFS Hash", fIno.Hash)
	return p.pm.Update(fIno)
}

func (p *powergateDriver) updateParent(parentIno *manifest.PowInode, child string) error {
	if parentIno.Children == nil {
		parentIno.Children = []string{}
	}
	found := false
	for _, v := range parentIno.Children {
		if v == child {
			found = true
		}
	}
	if !found {
		parentIno.Children = append(parentIno.Children, child)
		err := p.pm.Update(parentIno)
		if err != nil {
			return fmt.Errorf("pow: Failed updating parent node Err:%s", err.Error())
		}
	}
	return nil
}

func (p *powergateDriver) URLFor(
	ctx context.Context,
	path string,
	options map[string]interface{},
) (string, error) {
	return "", nil
}

type queryQueue struct {
	mtx   sync.Mutex
	items []string
}

func (q *queryQueue) Push(i string) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	if q.items == nil {
		q.items = make([]string, 0)
	}
	q.items = append(q.items, i)
}

func (q *queryQueue) Pop() string {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	if q.items == nil || len(q.items) == 0 {
		return ""
	}
	itemToReturn := q.items[0]
	q.items = q.items[1:]
	return itemToReturn
}

func (p *powergateDriver) Walk(
	ctx context.Context,
	path string,
	f storagedriver.WalkFn,
) error {
	q := &queryQueue{}
	q.Push(rootPath)
	for item := q.Pop(); len(item) != 0; item = q.Pop() {
		ino := &manifest.PowInode{
			Name: item,
		}
		err := p.pm.Read(ino)
		if err != nil {
			return fmt.Errorf("pow: Failed to read node Err:%s", err.Error())
		}
		err = f(ino)
		if err != nil && err != storagedriver.ErrSkipDir {
			return fmt.Errorf("pow: Failed in WalkFn Err:%s", err.Error())
		}
		if ino.IsDir() {
			for _, v := range ino.Children {
				q.Push(v)
			}
		}
	}
	return nil
}

// Helper routines
func (p *powergateDriver) mkdirAll(parent string) (*manifest.PowInode, error) {
	if parent != rootPath {
		gp, err := p.mkdirAll(path.Dir(parent))
		if err == nil {
			err = p.updateParent(gp, parent)
		}
		if err != nil {
			return nil, err
		}
	}
	pIno := &manifest.PowInode{
		Name: parent,
	}
	err := p.pm.Read(pIno)
	if err != nil {
		err = p.pm.Create(pIno)
	}
	return pIno, err
}

// Helpers
func fullPath(name string) string {
	return path.Join(rootPath, strings.TrimLeft(name, string(os.PathSeparator)))
}

func getParentPath(name string) string {
	return path.Dir(name)
}

type customReader struct {
	quit bool
	done chan bool
	buf  *bytes.Buffer
}

func (c *customReader) Read(p []byte) (int, error) {
	if c.quit && c.buf.Len() == 0 {
		return 0, io.EOF
	}
	for {
		select {
		case <-c.done:
			c.quit = true
		default:
			if c.buf.Len() >= len(p) {
				return c.buf.Read(p)
			}
			if c.quit && c.buf.Len() > 0 {
				return c.buf.Read(p)
			}
		}
	}
}

func (c *customReader) Close() error {
	c.done <- true
	return nil
}

type powFile struct {
	driver    *powergateDriver
	nd        *manifest.PowInode
	size      int64
	buf       *bytes.Buffer
	err       error
	closed    bool
	cancelled bool
	committed bool
	done      func()
	cancel    func()
}

func (g *powFile) startWorker(pCtx context.Context, rdr io.ReadCloser) {
	ctx, cancel := context.WithTimeout(pCtx, time.Minute*15)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := g.driver.addOrReplaceNode(ctx, g.nd, rdr)
		if err != nil && err != context.Canceled {
			g.err = err
			return
		}
	}()
	g.done = func() {
		rdr.Close()
		wg.Wait()
	}
	g.cancel = func() {
		cancel()
		wg.Wait()
	}
}

func (g *powFile) Write(inBuf []byte) (int, error) {
	n, err := g.buf.Write(inBuf)
	if err != nil {
		return 0, fmt.Errorf("powFile: Failed to write to buf Err:%s", err.Error())
	}
	g.size += int64(n)
	return n, nil
}

func (g *powFile) Size() int64 {
	return g.size
}

func (g *powFile) Close() error {
	if g.closed {
		return fmt.Errorf("powFile: already closed")
	}
	if !g.committed {
		g.done()
	}
	g.closed = true
	return g.err
}

func (g *powFile) Cancel() error {
	if g.closed {
		return fmt.Errorf("powFile: already closed")
	} else if g.committed {
		return fmt.Errorf("powFile: already committed")
	}
	g.cancel()
	g.cancelled = true
	return g.err
}

func (g *powFile) Commit() error {
	if g.closed {
		return fmt.Errorf("powFile: already closed")
	} else if g.committed {
		return fmt.Errorf("powFile: already committed")
	} else if g.cancelled {
		return fmt.Errorf("powFile: already cancelled")
	}
	g.done()
	g.committed = true
	return g.err
}
