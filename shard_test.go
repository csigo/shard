package shard

import (
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/csigo/ephemeral"
	"github.com/csigo/portforward"
	"github.com/csigo/test"
	"github.com/golang/groupcache/consistenthash"
	"github.com/satori/go.uuid"
	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	svr1       = "127.0.0.1:8080"
	svr2       = "192.168.0.1:81"
	svr3       = "10.0.0.1:4000"
	testEmRoot = "/htc.com/csi/groupcache"
)

var (
	etcdCli      *etcd.Client
	etcdForwdCli *etcd.Client
)

type ConnHashTestSuite struct {
	suite.Suite
	etcdOriPort int
	etcdPort    int
	stopForward chan struct{}
	forwarder   func() (chan struct{}, error)
}

func portForwarder(from, to string) func() (chan struct{}, error) {
	return func() (chan struct{}, error) {
		return portforward.PortForward(
			from, to,
		)
	}
}

func TestConnHashTestSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping zk shard test in short mode.")
	}
	//launch zk
	sl := test.NewServiceLauncher()
	etcdOriPort, stopEtcd, err := sl.Start(test.Etcd)
	assert.NoError(t, err)
	s := new(ConnHashTestSuite)
	s.etcdOriPort = etcdOriPort
	s.etcdPort = 3333 // used for port forward
	s.forwarder = portForwarder(
		fmt.Sprintf(":%d", s.etcdPort), fmt.Sprintf(":%d", s.etcdOriPort))

	s.stopForward, err = s.forwarder()
	assert.NoError(t, err, "no error")

	// non-forward etcdCli
	etcdCli = etcd.NewClient([]string{fmt.Sprintf("http://localhost:%d", etcdOriPort)})
	// forwardable etcdCli
	etcdForwdCli = etcd.NewClient([]string{fmt.Sprintf("http://localhost:%d", s.etcdPort)})
	suite.Run(t, s)

	// clean up the forwarding
	s.stopForward <- struct{}{}
	etcdCli.Close()
	etcdForwdCli.Close()
	assert.NoError(s.T(), stopEtcd())
}

func (c *ConnHashTestSuite) TestNewConnHashTimeout() {
	// overwrite log.Exiter as do noting, otherwise,
	// we will go to fatal since conn is closed and we attemp to create a znode on a closed conn
	em, err := ephemeral.NewEtcdEphemeral(etcdCli)
	assert.NoError(c.T(), err, "should get the connection")

	conn, err := NewConsistentHashResServer(em, testEmRoot, svr3,
		ConsistentHashMapReplicaNum, time.Nanosecond, dummy{})
	fmt.Printf("result of NewConsistentHashResServer %v, %v\n", conn, err)

	assert.Error(c.T(), err, "should hit timeout error")
	assert.Equal(c.T(), err, ErrConnTimedOut)
	assert.Nil(c.T(), conn, "should be nil")
	em.Close()
	fmt.Println("done")
}

func (c *ConnHashTestSuite) TestConsistentHashRes() {
	//creates two servers 127.0.0.1:8080 and 192.168.0.1:81
	//testing key uid[1-10]. Compare the result against
	//direct hash calculation from consistenthash.Map

	//first connect server 1 and server 2

	//consistent server 1
	t := c.T()
	em1, err := ephemeral.NewEtcdEphemeral(etcdForwdCli)

	if err != nil {
		t.Fatalf("Connect to zk error for server1: %s", err)
	}
	conn1, err := NewConsistentHashResServer(em1, testEmRoot, svr1,
		ConsistentHashMapReplicaNum, time.Second, dummy{})
	if err != nil {
		t.Fatalf("consistent server 1 %s create failed:%s", svr1, err)
	}
	assert.Equal(t, conn1.HostPort(), svr1)

	// wait zk change to stablize
	time.Sleep(1 * time.Second)
	assert.True(t, conn1.IsMyKey("any keys"), "should always be true since only one server only")

	//consistent server 2
	em2, err := ephemeral.NewEtcdEphemeral(etcdForwdCli)
	if err != nil {
		t.Fatalf("Connect to zk error for server2: %s", err)
	}
	conn2, err := NewConsistentHashResServer(em2, testEmRoot, svr2,
		ConsistentHashMapReplicaNum, time.Second, dummy{})
	if err != nil {
		t.Fatalf("consistent server 2 %s create failed:%s", svr2, err)
	}
	assert.Equal(t, conn2.HostPort(), svr2)

	//client
	emClient, err := ephemeral.NewEtcdEphemeral(etcdForwdCli)
	assert.NoError(t, err)
	client, err := NewConsistentHashResClient(emClient, testEmRoot,
		ConsistentHashMapReplicaNum, time.Second, dummy{})
	if err != nil {
		t.Fatalf("consistent client create failed:%s", err)
	}
	assert.Equal(t, client.HostPort(), "")

	//add server 1 and 2
	cmap := consistenthash.New(ConsistentHashMapReplicaNum, murmur3.Sum32)
	cmap.Add(svr1, svr2)

	//verify hashes are the same across all instances
	verifyAnswer(t, cmap, conn1, conn2, client)
	//verify peers
	verifyPeers(t, client.GetResources(), []string{svr1, svr2})
	// verify shard assignment distribution
	verifyShardDist(t, client, 2, 1000)

	//add another server
	em3, err := ephemeral.NewEtcdEphemeral(etcdForwdCli)
	if err != nil {
		t.Fatalf("Connect to zk error for server3: %s", err)
	}
	conn3, err := NewConsistentHashResServer(em3, testEmRoot, svr3,
		ConsistentHashMapReplicaNum, time.Second, dummy{})
	if err != nil {
		t.Fatalf("consistent server 3 %s create failed:%s", svr3, err)
	}
	assert.Equal(t, conn3.HostPort(), svr3)

	cmap.Add(svr3)

	//verify hashes are the same across all instances
	verifyAnswer(t, cmap, conn1, conn3, client, conn1)
	//verify peers
	verifyPeers(t, client.GetResources(), []string{svr1, svr2, svr3})
	// verify shard assignment distribution
	verifyShardDist(t, client, 3, 1000)

	// when zk are unreachable for like 20 seconds
	// all znodes are expired due to clent session is expired by zk

	// when the zkconn is back again, we still can do sharding
	c.stopForward <- struct{}{}
	time.Sleep(10 * time.Second)

	// make conn alive
	c.stopForward, _ = c.forwarder()
	time.Sleep(time.Second) // wait one second for ready

	//verify peers
	verifyPeers(t, client.GetResources(), []string{svr1, svr2, svr3})
	// verify shard assignment distribution
	verifyShardDist(t, client, 3, 1000)

	conn1.Close()
	conn2.Close()
	conn3.Close()
	client.Close()
	emClient.Close()
	em1.Close()
	em2.Close()
	em3.Close()
}

func verifyPeers(t *testing.T, recv []string, expect []string) {
	if len(recv) != len(expect) {
		t.Errorf("peers are different. Expecting %v, received %v", expect, recv)
	}

	sort.Strings(recv)
	sort.Strings(expect)
	for i := 0; i < len(recv); i++ {
		if recv[i] != expect[i] {
			t.Errorf("peers are different. Expecting %v, received %v", expect, recv)
			break
		}
	}
}

func verifyAnswer(t *testing.T, cmap *consistenthash.Map, servers ...*ConsistentHashRes) {
	//give zk sometime to stabalize
	time.Sleep(time.Second)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("uid%d", i)

		answer := cmap.Get(key)

		for _, s := range servers {
			shash, _ := s.Get(key)

			if answer != shash {
				t.Errorf("for %s expected %s, %s responded %s",
					key, answer, s.HostPort(), shash)
			}
		}
	}
}

func (c *ConnHashTestSuite) TestShardDistribution() {
	checkShardDispatch(c.T(), 5, 1000)
	checkShardDispatch(c.T(), 10, 10000)
	checkShardDispatch(c.T(), 80, 100000)
}

func checkShardDispatch(t *testing.T, nServer int, iteration int) {
	replica := ConsistentHashMapReplicaNum
	em, err := ephemeral.NewEtcdEphemeral(etcdForwdCli)
	if err != nil {
		t.Fatalf("Connect to zk error for server1: %s", err)
	}
	defer em.Close()
	fmt.Println("start creating shard host")
	for i := 0; i < nServer; i++ {
		name := fmt.Sprintf("192.168.0.1:%d", i)
		conn, err := NewConsistentHashResServer(em, testEmRoot, name,
			replica, 5*time.Second, dummy{})
		if err != nil {
			t.Fatalf("consistent server %s create failed:%s", name, err)
		}
		fmt.Printf("%d\t", i)

		defer conn.Close()
		assert.Equal(t, conn.HostPort(), name)
	}
	fmt.Println("finish creating shard host")

	// take a snap to get ready
	time.Sleep(time.Second)

	emClient, err := ephemeral.NewEtcdEphemeral(etcdForwdCli)
	assert.NoError(t, err)
	defer emClient.Close()
	client, err := NewConsistentHashResClient(emClient, testEmRoot,
		replica, 5*time.Second, dummy{})
	if err != nil {
		t.Fatalf("consistent client create failed:%s", err)
	}
	defer client.Close()
	assert.Equal(t, client.HostPort(), "")

	verifyShardDist(t, client, nServer, iteration)
}

func verifyShardDist(t *testing.T, client *ConsistentHashRes, nShard int, n int) {
	sdMax := float64(6)
	result := make(map[string]int)

	fmt.Println("========================")
	for i := 0; i < n; i++ {
		id := murmur3.Sum64(uuid.NewV4().Bytes())
		info, ok := client.Get(fmt.Sprintf("%d", id))
		assert.True(t, ok, "should get shard info")
		if _, b := result[info]; b == true {
			result[info]++
		} else {
			result[info] = 1
		}
	}

	avg := float64(100.0) / float64(nShard)
	var sum float64
	for key, val := range result {
		fmt.Printf("%s, count = %d\n", key, val)
		sum += math.Pow(((100.0 * float64(val) / float64(n)) - avg), 2)
	}

	sd := math.Sqrt(sum / float64(nShard))

	fmt.Printf("average: %.3f%% standard deviation: %.3f%%\n", avg, sd)
	if sd > sdMax {
		assert.Fail(t, fmt.Sprintf("standard deviation is too high %v", sd))
	}
}

func TestExtractIPort(t *testing.T) {
	ipport := "192.180.1.1:1234"

	v, err := ExtractIPPort(ipport + seperator + "xxxxx")
	assert.NoError(t, err)
	assert.Equal(t, ipport, v)

	v, err = ExtractIPPort(ipport + seperator)
	assert.NoError(t, err)
	assert.Equal(t, ipport, v)

	v, err = ExtractIPPort(ipport)
	assert.NoError(t, err)
	assert.Equal(t, ipport, v)

	v, err = ExtractIPPort("xxxxxyyyyyy")
	assert.NoError(t, err)
	assert.Equal(t, "xxxxxyyyyyy", v)

	v, err = ExtractIPPort("")
	assert.NoError(t, err)
	assert.Equal(t, "", v)
}

type dummy struct{}

func (d dummy) BumpAvg(key string, val float64) {}

func (d dummy) BumpSum(key string, val float64) {}

func (d dummy) BumpHistogram(key string, val float64) {}

func (d dummy) End() {}

func (d dummy) BumpTime(key string) interface {
	End()
} {
	return dummy{}
}
