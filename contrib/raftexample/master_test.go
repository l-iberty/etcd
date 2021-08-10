package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

func call(rpcname string, port int, arg interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":"+strconv.Itoa(port))
	if err != nil {
		panic(err)
	}
	defer c.Close()

	err = c.Call(rpcname, arg, reply)
	if err == nil {
		return true
	}

	fmt.Println("call error:", err)
	return false
}

func TestSlaveHeartbeatNoTimeout(t *testing.T) {
	stopC := make(chan struct{})
	notifyC := make(chan *string)
	var mu sync.Mutex
	timeout_ids := make(map[int]struct{})
	newMaster(1234, notifyC, stopC, func(id int) {
		mu.Lock()
		defer mu.Unlock()
		timeout_ids[id] = struct{}{}
	})

	pkgs := []heartbeatPackage{
		{1, "10.0.0.1", nil},
		{2, "10.0.0.2", nil},
		{3, "10.0.0.3", nil},
	}

	start := time.Now()
	log.Printf("*** start sending heartbeats to master")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			select {
			case <-time.After(HeartbeatTimeout / 2):
				if time.Since(start) > time.Minute {
					wg.Done()
					return
				}
				for _, p := range pkgs {
					v := p.mustMarshal()
					notifyC <- &v
				}
			}
		}
	}()
	wg.Wait()
	log.Printf("*** stop sending heartbeats to master")

	if len(timeout_ids) > 0 {
		t.Errorf("no time-out slaves should be seen")
	}
}

func TestSlaveHeartbeatTimeout(t *testing.T) {
	stopC := make(chan struct{})
	notifyC := make(chan *string)
	var mu sync.Mutex
	timeout_ids := make(map[int]struct{})
	newMaster(1234, notifyC, stopC, func(id int) {
		mu.Lock()
		defer mu.Unlock()
		timeout_ids[id] = struct{}{}
	})

	pkgs := []heartbeatPackage{
		{1, "10.0.0.1", nil},
		{2, "10.0.0.2", nil},
		{3, "10.0.0.3", nil},
	}

	start := time.Now()
	log.Printf("*** start sending heartbeats to master")

	var timeout_id int
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			select {
			case <-time.After(HeartbeatTimeout / 2):
				if time.Since(start) > time.Minute {
					wg.Done()
					return
				}
				if d := time.Since(start); d > 20*time.Second && timeout_id == 0 {
					timeout_id = rand.Intn(3) + 1
					log.Printf("*** timeout id: %d", timeout_id)
				}
				for _, p := range pkgs {
					if p.SlaveId == timeout_id {
						continue
					}
					v := p.mustMarshal()
					notifyC <- &v
				}
			}
		}
	}()
	wg.Wait()
	log.Printf("*** stop sending heartbeats to master")
	stopC <- struct{}{}

	if len(timeout_ids) == 0 {
		t.Fatalf("no time-out slaves found")
	}
	if len(timeout_ids) != 1 {
		t.Errorf("there should be only one time-out slave, got %d", len(timeout_ids))
	}
	if _, ok := timeout_ids[timeout_id]; !ok {
		t.Errorf("time-out slave %d not found", timeout_id)
	}
}

func TestSlaveDiscovery(t *testing.T) {
	stopC := make(chan struct{})
	notifyC := make(chan *string)
	m := newMaster(1234, notifyC, stopC, nil)

	pkgs := []heartbeatPackage{
		{
			SlaveId:   1,
			SlaveHost: "10.0.0.1:1234",
			FileStores: []FileStore{
				{
					GroupId:  1,
					PeerId:   101,
					Filename: "/test/file1",
					Md5sum:   "5D41402ABC4B2A76B9719D911017C592",
				},
				{
					GroupId:  2,
					PeerId:   201,
					Filename: "/test/file2",
					Md5sum:   "C404F7B26383437911584E7BF70367C2",
				},
			},
		},
		{
			SlaveId:   2,
			SlaveHost: "10.0.0.2:1234",
			FileStores: []FileStore{
				{
					GroupId:  1,
					PeerId:   101,
					Filename: "/test/file1",
					Md5sum:   "5D41402ABC4B2A76B9719D911017C592",
				},
				{
					GroupId:  2,
					PeerId:   201,
					Filename: "/test/file2",
					Md5sum:   "C404F7B26383437911584E7BF70367C2",
				},
				{
					GroupId:  3,
					PeerId:   301,
					Filename: "/test/file3",
					Md5sum:   "D3EF2051AA1686175F911A42DE9BA428",
				},
			},
		},
		{
			SlaveId:   3,
			SlaveHost: "10.0.0.3:1234",
			FileStores: []FileStore{
				{
					GroupId:  1,
					PeerId:   101,
					Filename: "/test/file1",
					Md5sum:   "5D41402ABC4B2A76B9719D911017C592",
				},
				{
					GroupId:  3,
					PeerId:   301,
					Filename: "/test/file3",
					Md5sum:   "D3EF2051AA1686175F911A42DE9BA428",
				},
			},
		},
		{
			SlaveId:   4,
			SlaveHost: "10.0.0.4:1234",
			FileStores: []FileStore{
				{
					GroupId:  2,
					PeerId:   201,
					Filename: "/test/file2",
					Md5sum:   "C404F7B26383437911584E7BF70367C2",
				},
				{
					GroupId:  3,
					PeerId:   301,
					Filename: "/test/file3",
					Md5sum:   "D3EF2051AA1686175F911A42DE9BA428",
				},
			},
		},
	}

	var wg sync.WaitGroup
	for _, p := range pkgs {
		wg.Add(1)
		go func(p heartbeatPackage) {
			defer wg.Done()
			time.Sleep(100 * time.Microsecond)
			v := p.mustMarshal()
			notifyC <- &v
		}(p)
	}
	wg.Wait()
	stopC <- struct{}{}

	if exp, got := len(m.slaveHost), 4; exp != got {
		t.Errorf("len(slaveHost) exp %v, got %v", exp, got)
	}

	for _, id := range []int{1, 2, 3, 4} {
		if exp, got := m.slaveHost[id], fmt.Sprintf("10.0.0.%d:1234", id); exp != got {
			t.Errorf("host of slave-%d: exp %v, got %v", id, exp, got)
		}
		if !reflect.DeepEqual(m.slaveFileStore[id], pkgs[id-1].FileStores) {
			t.Errorf("slaveFileStore on slave-%d is cracked", id)
		}
	}

	tests := []struct {
		file string
		ids  []int
	}{
		{"/test/file1", []int{1, 2, 3}},
		{"/test/file2", []int{1, 2, 4}},
		{"/test/file3", []int{2, 3, 4}},
	}
	for i := range tests {
		idmap := m.fileReplicaAddr[tests[i].file]
		var ids []int
		for id := range idmap {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		if exp, got := tests[i].ids, ids; !reflect.DeepEqual(exp, got) {
			t.Errorf("exp %v, got %v", exp, got)
		}
	}
}

func TestMasterBasicRPC(t *testing.T) {
	rpcname := "Master.AssignSlave"
	rpcport := 1234
	stopC := make(chan struct{})
	m := newMaster(rpcport, nil, stopC, func(int) {})

	slave_ids := []int{1, 2, 3, 4}
	for _, id := range slave_ids {
		m.slaveHost[id] = fmt.Sprintf("slave-%d", id)
		m.slaveFileStore[id] = []FileStore{}
	}

	args := []AssignSlaveArg{
		// file1 的3个副本
		{"file1_1_101.tsm"},
		{"file1_1_102.tsm"},
		{"file1_1_103.tsm"},
		// file2 的3个副本
		{"file2_2_101.tsm"},
		{"file2_2_102.tsm"},
		{"file2_2_103.tsm"},
		// file3 的3个副本
		{"file3_3_101.tsm"},
		{"file3_3_102.tsm"},
		{"file3_3_103.tsm"},
	}
	var reply AssignSlaveReply

	results := make(map[int]map[string]struct{}) // slave id -> file tags
	for _, id := range slave_ids {
		results[id] = make(map[string]struct{})
	}
	for _, arg := range args {
		ok := call(rpcname, rpcport, &arg, &reply)
		if !ok {
			t.Errorf("RPC %s failed", rpcname)
		}
		if exp, got := fmt.Sprintf("slave-%d", reply.SlaveId), reply.SlaveAddr; exp != got {
			t.Errorf("reply.SlaveAddr: expected %s, got %s", exp, got)
		}
		tag := getFileTag(arg.Filename)
		if _, ok := results[reply.SlaveId][tag]; ok {
			t.Errorf("same replica shouldn't be assigned to the same slave")
		}
		results[reply.SlaveId][tag] = struct{}{}
	}

	var used int
	for id := range results {
		if len(results[id]) > 0 {
			used++
		}
	}
	if used != len(slave_ids) {
		t.Errorf("some slaves are ignored in the assignment")
	}
	stopC <- struct{}{}
}
