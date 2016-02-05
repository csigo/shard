/* Package shard allows mapping of resources, e.g, services, to be
dynamically scaled using consistent hashing algorithm using etcd.
*/

package shard

// ConsistentHashRes.go allows mapping of resources, e.g, services, to be
// dynamically scaled using consistent hashing algorithm using shard.

// Assume we have a service, a type of resources, and we want client to
// consistently query the service based on a string token. It's very similar to
// sharding of services where requests identified by the same token always
// go to the same service provider. However, consistent hashing of a service
// has the benefit over sharding in the sense we don't have to predetermine
// how many shards we need. Instead, as there are more and more request
// loads, we just keep adding service providers. Because of consistent hashing,
// addition of new service provider only redistribute a small portion of token
// to service provider mapping. And if both client and service provide both
// uses the same consistent hashing table, then we can ensure that:
// 1, when clients and service providers have the same consistent hash mapping
// then one token will always map to the same provider, and
// 2, when clients and service providers have different mapping, the mismatch
// will be detected.
//
// The following is an example of using ConsistentHashRes to map request hashed
// with a string token always map to a service provider
//
// On the server side
//	import shard "htc.com/csi/base/shard"
//  import       "github.com/csigo/ephemeral"
//
//	var emConn ephemeral.Ephemeral
//	var emRoot string
//	//setup shard client connection
//	//...
//
//	service := NewService(ipport)
//	conhash := shard.NewConsistentHashResServer(emConn, emRoot, "my_service_id",
//			   shard.ConsistentHashMapReplicaNum, time.Second)
//
// One the client side, we have
//
//	conhash := shard.NewConsistentHashResClient(emConn, emRoot,
//		       shard.ConsistentHashMapReplicaNum,time.Second)
//	service_id := conhash.Get("token1")
//	connect_to_server(service_id)

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/csigo/ephemeral"
	"github.com/facebookgo/stats"
	"github.com/golang/groupcache/consistenthash"
	"github.com/spaolacci/murmur3"
	"golang.org/x/net/context"
)

const (
	//ConsistentHashMapReplicaNum default number of replicas for
	//each consistent map entries
	ConsistentHashMapReplicaNum = 50

	maxHosts = 1000
)

var (
	//ErrConnTimedOut timeout
	ErrConnTimedOut = fmt.Errorf("[shard] init timeout")
)

// ResFinder is the interface to lookup information of resources
type ResFinder interface {
	//Get return server ipport given the key
	Get(key string) (string, bool)
	// IsMyKey checks whether the given key is handled by current shard
	IsMyKey(key string) bool
	// All returns all ipports in current resource pool
	All() map[string]struct{}
}

//ConsistentHashRes represents a consistently hashed resource.
type ConsistentHashRes struct {
	sync.Mutex

	ephemeral ephemeral.Ephemeral // ephemeral tracks online service instances.
	root      string              //znode root path for consistent hashed connections
	cmap      *consistenthash.Map //consistent hash map
	ipport    string              //ipport for server, "" for client
	done      chan struct{}       //done channel
	peers     map[string]struct{} //set for maintaining all peers
	ctr       stats.Client
}

const (
	// timeWait is the waiting time for getting data from zookeepr again
	timeWait = 200 * time.Millisecond
)

//NewConsistentHashResClient client is an entity that doesn't provide a consistently
//hashed resource, but uses one.
func NewConsistentHashResClient(
	em ephemeral.Ephemeral,
	root string,
	replica int,
	timeout time.Duration,
	counter stats.Client,
) (*ConsistentHashRes, error) {
	return newConsistentHashRes(em, root, "", replica, timeout, counter)
}

//NewConsistentHashResServer is a provider of a consistently hashed resource.
func NewConsistentHashResServer(
	em ephemeral.Ephemeral,
	root string,
	ipport string,
	replica int,
	timeout time.Duration,
	counter stats.Client,
) (*ConsistentHashRes, error) {
	return newConsistentHashRes(em, root, ipport, replica, timeout, counter)
}

//GetResources return a slice of all distinct resources
func (c *ConsistentHashRes) GetResources() (ret []string) {
	for k := range c.peers {
		ret = append(ret, k)
	}

	return
}

func (c *ConsistentHashRes) setCmap(other *consistenthash.Map,
	resMap map[string]struct{}) {
	c.Lock()
	c.cmap = other
	c.peers = resMap
	c.Unlock()
}

// createNode creates an ephemeral node under zookeeper root node
func createNode(em ephemeral.Ephemeral, path string) {
	err := em.AddKey(context.Background(), path, "")
	// We don't skip if err == ErrPathExists. If path exists, other server is occupying the
	// same znode. Should error / fatal to notify.
	if err != nil {
		// NOTE: we are going to fatal if creating znode failed
		log.Fatalf("[shard] fail to create %v %v", path, err)
	}
}

func newConsistentHashRes(
	em ephemeral.Ephemeral,
	root string,
	ipport string,
	replica int,
	timeout time.Duration,
	counter stats.Client,
) (*ConsistentHashRes, error) {
	c := &ConsistentHashRes{
		ephemeral: em,
		root:      root,
		cmap:      consistenthash.New(replica, murmur3.Sum32),
		done:      make(chan struct{}),
		ipport:    ipport,
		ctr:       counter,
	}

	// ensure root path
	if err := ephemeral.EnsurePath(em, root); err != nil {
		return nil, err
	}

	//if I am a server then register
	if ipport != "" {
		if _, _, err := net.SplitHostPort(ipport); err != nil {
			log.Errorf("incoming hostport %s isn't in host:port format, err %v", ipport, err)
			return nil, err
		}
		node := makeNode(root, ipport)
		createNode(em, node)
	}

	ready := make(chan struct{})
	var retErr error

	//listen to server events
	go func() {
		readySent := false
		receiver := c.ephemeral.List(context.Background(), c.root, true)

	zkloop:
		// TODO: add maxRetries
		for {
			var resp *ephemeral.ListResponse
			select {
			case <-c.done: // signal done received
				c.ctr.BumpSum("loop.done", 1)
				break zkloop
			case resp = <-receiver:
				if resp.Err == ephemeral.ErrPathNotFound {
					log.Fatalf("[shard] root directory<%s> not found", c.root)
				}
				if resp.Err != nil {
					// TODO: handle conn.close
					// when conn is closed, we will get an ErrSessionExired
					// vendor/src/github.com/samuel/go-zookeeper/shard/zk_test.go
					// line 370
					c.ctr.BumpSum("loop.setwatch.err", 1)
					log.Errorf("[shard] fail to watch %v err: %v", c.root, resp.Err)
					retErr = resp.Err
					time.Sleep(timeWait)
					// Re-assign receiver.
					receiver = c.ephemeral.List(context.Background(), c.root, true)
					continue
				}
				c.ctr.BumpSum("loop.zkchange", 1)
			}

			log.Infof("[shard] in consistentres, root:%s, children: %v", c.root, resp.Children)

			cmap := consistenthash.New(replica, murmur3.Sum32)
			keys := make(map[string]struct{})

			// The list will be reallocated by append() if size is not enough
			hosts := make([]string, 0, maxHosts)
			for _, child := range resp.Children {
				ipport, err := ExtractIPPort(child)
				if err != nil {
					c.ctr.BumpSum("loop.parse.err", 1)
					log.Errorf("[shard] parse error root %v, node %v err %v",
						c.root, child, err)
					continue
				}
				if _, ok := keys[ipport]; ok {
					c.ctr.BumpSum("loop.dupkey.warn", 1)
					log.Infof("[shard] duplicated shard info %v %v", c.root, ipport)
					continue
				}
				keys[ipport] = struct{}{}
				hosts = append(hosts, ipport)
			}
			cmap.Add(hosts...)

			//replace the old cmap
			c.setCmap(cmap, keys)

			//signal ready
			if !readySent {
				// if ready, clear previous err
				retErr = nil
				c.ctr.BumpSum("loop.ready", 1)
				ready <- struct{}{}
				readySent = true
			}
		}
		close(ready)
	}()

	// wait till ready
	select {
	case <-ready:
		if retErr != nil {
			c.ctr.BumpSum("newhash.init.err", 1)
			return nil, retErr
		}
		c.ctr.BumpSum("newhash.ready", 1)
	case <-time.After(timeout):
		c.ctr.BumpSum("newhash.timeout.err", 1)
		log.Errorf("[shard] consistent hash init timeout %v", c.root)
		return nil, ErrConnTimedOut
	}

	return c, nil
}

// TODO: retire separator
const seperator = "_"

// makeNode generated path of a ZNode
func makeNode(zkRoot string, ipport string) string {
	return fmt.Sprintf("%s/%s", zkRoot, ipport)
}

// ExtractIPPort parses the input name and returns host IP port which is the
// first token
func ExtractIPPort(name string) (string, error) {
	// TODO: Remove separator. Separator and spliting is needed for backward compatibility.
	// Should remove separator checking after all servers don't use separator anymore.
	tokens := strings.Split(name, seperator)
	/*
		if len(tokens) <= 1 {
			return "", fmt.Errorf("[shard] illegal znode name %s", name)
		}
	*/
	return tokens[0], nil
}

//Close close the client
func (c *ConsistentHashRes) Close() {
	c.done <- struct{}{}
}

//HostPort returns the ipport of this connection. If ipport is empty then it's a
//client
func (c *ConsistentHashRes) HostPort() string {
	return c.ipport
}

//Get return server ipport given the key
func (c *ConsistentHashRes) Get(key string) (string, bool) {
	defer c.ctr.BumpTime("conhash.get").End()
	c.Lock()
	defer c.Unlock()

	if c.cmap.IsEmpty() {
		return "", false
	}

	return c.cmap.Get(key), true
}

// IsMyKey checks whether the given key is handled by current shard
func (c *ConsistentHashRes) IsMyKey(key string) bool {
	return c.ipport != "" && c.cmap.Get(key) == c.ipport
}

// All returns all ipports in current resource pool
func (c *ConsistentHashRes) All() map[string]struct{} {
	c.Lock()
	defer c.Unlock()

	return c.peers
}
