package powergate

import (
	"io/ioutil"
	"os"
	"testing"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	"gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

func init() {
	root, err := ioutil.TempDir("", "powdriver-")
	if err != nil {
		panic(err)
	}
	defer os.Remove(root)

	driver, err := New(map[string]interface{}{
		"powaddress": "http://localhost:4001",
		"powpath":    root,
	})
	if err != nil {
		panic(err)
	}

	testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return driver, nil
	}, testsuites.NeverSkip)
}
