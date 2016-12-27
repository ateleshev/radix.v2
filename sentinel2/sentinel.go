// Package sentinel provides a convenient interface with a redis sentinel which
// will automatically handle pooling connections and failover.
//
// Here's an example of creating a sentinel client and then using it to perform
// some commands
//
//	func example() error {
//		// If there exists sentinel masters "bucket0" and "bucket1", and we want
//		// out client to create pools for both:
//		client, err := sentinel.NewClient("tcp", "localhost:6379", 100, "bucket0", "bucket1")
//		if err != nil {
//			return err
//		}
//
//		if err := exampleCmd(client); err != nil {
//			return err
//		}
//
//		return nil
//	}
//
//	func exampleCmd(client *sentinel.Client) error {
//		conn, err := client.GetMaster("bucket0")
//		if err != nil {
//			return redisErr
//		}
//		defer client.PutMaster("bucket0", conn)
//
//		i, err := conn.Cmd("GET", "foo").Int()
//		if err != nil {
//			return err
//		}
//
//		if err := conn.Cmd("SET", "foo", i+1); err != nil {
//			return err
//		}
//
//		return nil
//	}
//
// This package only guarantees that when Do is called the used connection will
// be a connection to the master as of the moment that method is called. It is
// still possible that there is a failover in the middle of an Action.
package sentinel

import (
	"errors"
	"sync"

	radix "github.com/mediocregopher/radix.v2"
)

type sentinelClient struct {
	// we read lock when calling methods on p, and normal lock when swapping the
	// value of p, pAddr, or modifying addrs
	sync.RWMutex
	p     radix.Pool
	pAddr string
	addrs []string // the known sentinel addresses

	name string
	dfn  radix.DialFunc // the function used to dial sentinel instances
	pfn  radix.PoolFunc
}

func (sc *sentinelClient) Do(a radix.Action) error {
	sc.RLock()
	defer sc.RUnlock()
	return sc.p.Do(a)
}

func (sc *sentinelClient) Close() error {
	sc.RLock()
	defer sc.RUnlock()
	// TODO probably need to stop the sentinel conn
	return sc.p.Close()
}

func (sc *sentinelClient) Get() (radix.PoolConn, error) {
	sc.RLock()
	defer sc.RUnlock()
	return sc.p.Get()
}

// given a connection to a sentinel, ensures that the pool currently being held
// agrees with what the sentinel thinks it should be
func (sc *sentinelClient) ensureMaster(conn radix.Conn) error {
	sc.Lock()
	lastAddr := sc.pAddr
	sc.Unlock()

	var m map[string]string
	err := radix.CmdNoKey("SENTINEL", "MASTER", sc.name).Into(&m).Run(conn)
	if err != nil {
		return err
	} else if m["ip"] == "" || m["port"] == "" {
		return errors.New("malformed SENTINEL MASTER response")
	}
	newAddr := m["ip"] + ":" + m["port"]
	if newAddr == lastAddr {
		return nil
	}

	newPool, err := sc.pfn("tcp", newAddr)
	if err != nil {
		return err
	}

	sc.Lock()
	if sc.p != nil {
		sc.p.Close()
	}
	sc.p = newPool
	sc.pAddr = newAddr
	sc.Unlock()

	return nil
}

// annoyingly the SENTINEL SENTINELS <name> command doesn't return _this_
// sentinel instance, only the others it knows about for that master
func (sc *sentinelClient) ensureSentinelAddrs(conn radix.Conn) error {
	addrs := []string{conn.RemoteAddr().String()}
	var mm []map[string]string
	err := radix.CmdNoKey("SENTINEL", "SENTINELS", sc.name).Into(&mm).Run(conn)
	if err != nil {
		return err
	}

	for _, m := range mm {
		addrs = append(addrs, m["ip"]+":"+m["port"])
	}

	sc.Lock()
	sc.addrs = addrs
	sc.Unlock()
	return nil
}
