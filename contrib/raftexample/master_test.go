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

func toSortedInts(m map[int]struct{}) []int {
	a := []int{}
	for x := range m {
		a = append(a, x)
	}
	sort.Ints(a)
	return a
}

func toSortedInts2(m map[int]int) []int {
	a := []int{}
	for x := range m {
		a = append(a, x)
	}
	sort.Ints(a)
	return a
}

func intersectedKeys1(a map[int]int, b []int) []int {
	var res []int
	for k := range a {
		for i := range b {
			if k == b[i] {
				res = append(res, k)
			}
		}
	}
	return res
}

func intersectedKeys2(a map[int]struct{}, b []int) []int {
	var res []int
	for k := range a {
		for i := range b {
			if k == b[i] {
				res = append(res, k)
			}
		}
	}
	return res
}

func TestSlaveHeartbeatNoTimeout(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)
	notifyC := make(chan *string)
	var mu sync.Mutex
	timeout_ids := make(map[int]struct{})
	m := NewMaster(nil, 1234, notifyC, stopC, func(id int) {
		mu.Lock()
		defer mu.Unlock()
		timeout_ids[id] = struct{}{}
	})
	m.Run()

	pkgs := []heartbeatPackage{
		{1, "10.0.0.1", 10, 20, nil},
		{2, "10.0.0.2", 10, 20, nil},
		{3, "10.0.0.3", 10, 20, nil},
	}

	start := time.Now()
	log.Printf("*** start sending heartbeats to master")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			select {
			case <-time.After(HeartbeatTimeout / 2):
				if time.Since(start) > 10*time.Second {
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

	if len(m.activeSlaves) != 0 {
		t.Error("no AssignSlave RPC invoked, so no active slaves should be found")
	}

	backup_ids := []int{}
	for id := range m.backupSlaves {
		backup_ids = append(backup_ids, id)
	}
	sort.Ints(backup_ids)
	if exp := []int{1, 2, 3}; !reflect.DeepEqual(exp, backup_ids) {
		t.Errorf("backup_ids: exp %v, got %v", exp, backup_ids)
	}
}

func TestSlaveHeartbeatTimeout(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)
	notifyC := make(chan *string)
	var mu sync.Mutex
	timeout_ids := make(map[int]struct{})
	m := NewMaster(nil, 1234, notifyC, stopC, func(id int) {
		mu.Lock()
		defer mu.Unlock()
		timeout_ids[id] = struct{}{}
	})
	m.Run()

	pkgs := []heartbeatPackage{
		{1, "10.0.0.1", 10, 20, nil},
		{2, "10.0.0.2", 10, 20, nil},
		{3, "10.0.0.3", 10, 20, nil},
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
				if time.Since(start) > 30*time.Second {
					wg.Done()
					return
				}
				if d := time.Since(start); d > 20*time.Second && timeout_id == 0 {
					timeout_id = rand.Intn(3) + 1
					log.Printf("*** timeout id: %d", timeout_id)
				}
				for _, p := range pkgs {
					if p.SlaveId == timeout_id {
						// skip to make it timeout
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

	if len(timeout_ids) == 0 {
		t.Fatalf("no time-out slaves found")
	}
	if len(timeout_ids) != 1 {
		t.Errorf("there should be only one time-out slave, got %d", len(timeout_ids))
	}
	if _, ok := timeout_ids[timeout_id]; !ok {
		t.Errorf("time-out slave %d not found", timeout_id)
	}

	if len(m.activeSlaves) != 0 {
		t.Error("no AssignSlave RPC invoked, so no active slaves should be found")
	}

	// time-out slave should be kicked out
	if _, ok := m.slaveHost[timeout_id]; ok {
		t.Errorf("time-out slave %d should not be found in slaveHost", timeout_id)
	}
	if _, ok := m.slaveLastHeartbeat[timeout_id]; ok {
		t.Errorf("time-out slave %d should not be found in slaveLastHearbeat", timeout_id)
	}
	if _, ok := m.slaveFileStore[timeout_id]; ok {
		t.Errorf("time-out slave %d should not be found in slaveFileStore", timeout_id)
	}
	if _, ok := m.activeSlaves[timeout_id]; ok {
		t.Errorf("time-out slave %d should not be found in activeSlaves", timeout_id)
	}
	if _, ok := m.backupSlaves[timeout_id]; ok {
		t.Errorf("time-out slave %d should not be found in backupSlaves", timeout_id)
	}
}

func TestSlaveDiscovery(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)
	notifyC := make(chan *string)
	m := NewMaster(nil, 1234, notifyC, stopC, nil)
	m.Run()

	pkgs := []heartbeatPackage{
		{
			SlaveId:   1,
			SlaveHost: "10.0.0.1:1234",
			FileStores: []FileStore{
				{
					GroupId:  1,
					PeerId:   100,
					Filename: "file1.tsm",
					Md5sum:   "xxx",
				},
				{
					GroupId:  1,
					PeerId:   100,
					Filename: "file2.tsm",
					Md5sum:   "yyy",
				},
				{
					GroupId:  1,
					PeerId:   100,
					Filename: "file3.tsm",
					Md5sum:   "zzz",
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
					Filename: "file1.tsm",
					Md5sum:   "xxx",
				},
				{
					GroupId:  1,
					PeerId:   101,
					Filename: "file2.tsm",
					Md5sum:   "yyy",
				},
				{
					GroupId:  1,
					PeerId:   101,
					Filename: "file3.tsm",
					Md5sum:   "zzz",
				},
			},
		},
		{
			SlaveId:   3,
			SlaveHost: "10.0.0.3:1234",
			FileStores: []FileStore{
				{
					GroupId:  1,
					PeerId:   102,
					Filename: "file1.tsm",
					Md5sum:   "xxx",
				},
				{
					GroupId:  1,
					PeerId:   102,
					Filename: "file2.tsm",
					Md5sum:   "yyy",
				},
				{
					GroupId:  1,
					PeerId:   102,
					Filename: "file3.tsm",
					Md5sum:   "zzz",
				},
			},
		},
		{
			SlaveId:    4,
			SlaveHost:  "10.0.0.4:1234",
			FileStores: []FileStore{},
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

	time.Sleep(HeartbeatTimeout)

	if exp, got := 4, len(m.slaveHost); exp != got {
		t.Errorf("len(slaveHost) exp %v, got %v", exp, got)
	}

	for _, id := range []int{1, 2, 3, 4} {
		if exp, got := fmt.Sprintf("10.0.0.%d:1234", id), m.slaveHost[id]; exp != got {
			t.Errorf("host of slave-%d: exp %v, got %v", id, exp, got)
		}
		if len(m.slaveFileStore[id]) != len(pkgs[id-1].FileStores) {
			t.Errorf("len(m.slaveFileStore[%d])!=len(pkgs[%d-1].FileStores)", id, id)
		}
		for _, f := range pkgs[id-1].FileStores {
			if !reflect.DeepEqual(m.slaveFileStore[id][f.Filename], f) {
				t.Errorf("slaveFileStore on slave-%d is cracked", id)
			}
		}
	}

	tests := []struct {
		file string
		ids  []int
	}{
		{"file1.tsm", []int{1, 2, 3}},
		{"file2.tsm", []int{1, 2, 3}},
		{"file3.tsm", []int{1, 2, 3}},
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

func TestIntegrationTest_NoSlaveTimeout(t *testing.T) {
	rpcname := "Master.AssignSlave"
	rpcport := 1234
	stopC := make(chan struct{})
	defer close(stopC)
	notifyC := make(chan *string)
	var mu1 sync.RWMutex
	var mu2 sync.RWMutex
	timeout_ids := make(map[int]struct{})
	m := NewMaster(nil, rpcport, notifyC, stopC, func(id int) {
		mu1.Lock()
		defer mu1.Unlock()
		timeout_ids[id] = struct{}{}
	})
	m.Run()

	pkgs := []heartbeatPackage{
		{1, "10.0.0.1", 10, 20, []FileStore{}},
		{2, "10.0.0.2", 10, 20, []FileStore{}},
		{3, "10.0.0.3", 10, 20, []FileStore{}},
		{4, "10.0.0.4", 10, 20, []FileStore{}},
		{5, "10.0.0.5", 10, 20, []FileStore{}},
	}

	start := time.Now()
	log.Printf("*** start sending heartbeats to master")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			select {
			case <-time.After(HeartbeatTimeout / 2):
				if time.Since(start) > 30*time.Second {
					wg.Done()
					return
				}
				mu2.RLock()
				for _, p := range pkgs {
					v := p.mustMarshal()
					notifyC <- &v
				}
				mu2.RUnlock()
			}
		}
	}()

	// wait for a little while and begin sending AssignSlave RPC
	time.Sleep(5 * time.Second)

	args := []AssignSlaveArg{
		// file1 的3个副本
		{"file1_1_101.tsm"},
		{"file1_1_102.tsm"},
		{"file1_1_103.tsm"},
		// file2 的3个副本
		{"file2_1_101.tsm"},
		{"file2_1_102.tsm"},
		{"file2_1_103.tsm"},
		// file3 的3个副本
		{"file3_1_101.tsm"},
		{"file3_1_102.tsm"},
		{"file3_1_103.tsm"},
	}
	var reply AssignSlaveReply

	assignedSlaves := make(map[int]struct{})

	for _, arg := range args {
		// 模拟 influxdb data node 向 master 发送 AssignSlave RPC 询问它应该
		// 将 TSM 发送到哪一个 slave.
		ok := call(rpcname, rpcport, &arg, &reply)
		if !ok {
			t.Errorf("RPC %s failed", rpcname)
		}
		// 检查 RPC reply 的 SlaveId 和 SlaveAddr 是否匹配.
		if exp, got := fmt.Sprintf("10.0.0.%d:10", reply.SlaveId), reply.SlaveAddr; exp != got {
			t.Errorf("reply.SlaveAddr: expected %s, got %s", exp, got)
		}

		_, basename, groupId, peerId := parseFilename(arg.Filename)
		tag := fmt.Sprintf("%d_%d", groupId, peerId) // tag 唯一标识一个 influxdb data node
		// master 在响应 influxdb data node 的 AssignSlave RPC 后应该在 node2slave 里.
		// 记录下它将哪个 slave 分配给了这个 influxdb data node
		assignedSlaves[m.node2slave[tag]] = struct{}{}

		// influxdb data node 在收到 master 的 RPC reply 之后就会把 TSM 发送到
		// 指定的 slave, slave 成功接收完 TSM 后会在发送给 master 的心跳包里携带上
		// 当前已存储的 TSM 文件信息. 这里通过直接修改 pkgs 进行模拟.
		mu2.Lock()
		for i := range pkgs {
			// pkgs[i].SlaveId 表示这个 heartbeat 来自哪一个 slave
			if pkgs[i].SlaveId == reply.SlaveId {
				pkgs[i].FileStores = append(pkgs[i].FileStores, FileStore{
					GroupId:  groupId,
					PeerId:   peerId,
					Filename: basename,
					Md5sum:   "", // empty for this test case
				})
			}
		}
		mu2.Unlock()
	}

	wg.Wait()
	log.Printf("*** stop sending heartbeats to master")

	if len(timeout_ids) > 0 {
		t.Errorf("no time-out slaves should be seen")
	}

	// 我们模拟了3个 influxdb data node: 1-101, 1-102, 1-103
	// master 应该分配出3个 slave, 并将其记录在 activeSlaves; 后备节点还剩2个
	assignedSlaveIds := toSortedInts(assignedSlaves)
	if len(assignedSlaveIds) != 3 {
		t.Error("len(assignedSlaveIds) != 3")
	}
	activeSlaveIds := toSortedInts2(m.activeSlaves)
	if !reflect.DeepEqual(activeSlaveIds, assignedSlaveIds) {
		t.Errorf("activeSlaveIds %v != assignedSlaveIds %v", activeSlaveIds, assignedSlaveIds)
	}
	if len(m.backupSlaves) != 2 {
		t.Errorf("len(m.backupSlaves) != 2")
	}

	// 根据发送的心跳包 pkgs 来看, 每个文件都有3个副本, 并且应该存储在3个相同的 slave 上面
	// ——这些 slave 就是 master 分配出去的那3个.
	for _, file := range []string{"file1.tsm", "file2.tsm", "file3.tsm"} {
		ids := []int{}
		for id := range m.fileReplicaAddr[file] {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		if !reflect.DeepEqual(ids, assignedSlaveIds) {
			t.Errorf("fileReplicaAddr[%s] %v != %v", file, m.fileReplicaAddr[file], assignedSlaveIds)
		}
	}
}

func TestIntegrationTest_SlaveTimeout(t *testing.T) {
	rpcname := "Master.AssignSlave"
	rpcport := 1234
	stopC := make(chan struct{})
	defer close(stopC)
	notifyC := make(chan *string)
	var mu1 sync.RWMutex
	var mu2 sync.RWMutex
	timeout_ids := make(map[int]struct{})
	m := NewMaster(nil, rpcport, notifyC, stopC, func(id int) {
		mu1.Lock()
		defer mu1.Unlock()
		timeout_ids[id] = struct{}{}
	})
	m.Run()

	pkgs := []heartbeatPackage{
		{1, "10.0.0.1", 10, 20, []FileStore{}},
		{2, "10.0.0.2", 10, 20, []FileStore{}},
		{3, "10.0.0.3", 10, 20, []FileStore{}},
		{4, "10.0.0.4", 10, 20, []FileStore{}},
		{5, "10.0.0.5", 10, 20, []FileStore{}},
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
				if time.Since(start) > 30*time.Second {
					wg.Done()
					return
				}
				if d := time.Since(start); d > 15*time.Second && len(m.activeSlaves) == 3 && timeout_id == 0 {
					timeout_id = pickKey2(m.activeSlaves)
					if timeout_id != 0 {
						log.Printf("*** timeout id: %d", timeout_id)
					}
				}
				mu2.RLock()
				for _, p := range pkgs {
					if p.SlaveId == timeout_id {
						// skip to make it timeout
						continue
					}
					v := p.mustMarshal()
					notifyC <- &v
				}
				mu2.RUnlock()
			}
		}
	}()

	// wait for a little while and begin sending AssignSlave RPC
	time.Sleep(5 * time.Second)

	args := []AssignSlaveArg{
		// file1 的3个副本
		{"file1_1_101.tsm"},
		{"file1_1_102.tsm"},
		{"file1_1_103.tsm"},
		// file2 的3个副本
		{"file2_1_101.tsm"},
		{"file2_1_102.tsm"},
		{"file2_1_103.tsm"},
		// file3 的3个副本
		{"file3_1_101.tsm"},
		{"file3_1_102.tsm"},
		{"file3_1_103.tsm"},
	}
	var reply AssignSlaveReply

	assignedSlaves := make(map[int]struct{})

	for _, arg := range args {
		// 模拟 influxdb data node 向 master 发送 AssignSlave RPC 询问它应该
		// 将 TSM 发送到哪一个 slave.
		ok := call(rpcname, rpcport, &arg, &reply)
		if !ok {
			t.Errorf("RPC %s failed", rpcname)
		}
		// 检查 RPC reply 的 SlaveId 和 SlaveAddr 是否匹配.
		if exp, got := fmt.Sprintf("10.0.0.%d:10", reply.SlaveId), reply.SlaveAddr; exp != got {
			t.Errorf("reply.SlaveAddr: expected %s, got %s", exp, got)
		}

		_, basename, groupId, peerId := parseFilename(arg.Filename)
		tag := fmt.Sprintf("%d_%d", groupId, peerId) // tag 唯一标识一个 influxdb data node
		// master 在响应 influxdb data node 的 AssignSlave RPC 后应该在 node2slave 里.
		// 记录下它将哪个 slave 分配给了这个 influxdb data node
		assignedSlaves[m.node2slave[tag]] = struct{}{}

		// influxdb data node 在收到 master 的 RPC reply 之后就会把 TSM 发送到
		// 指定的 slave, slave 成功接收完 TSM 后会在发送给 master 的心跳包里携带上
		// 当前已存储的 TSM 文件信息. 这里通过直接修改 pkgs 进行模拟.
		mu2.Lock()
		for i := range pkgs {
			// pkgs[i].SlaveId 表示这个 heartbeat 来自哪一个 slave
			if pkgs[i].SlaveId == reply.SlaveId {
				pkgs[i].FileStores = append(pkgs[i].FileStores, FileStore{
					GroupId:  groupId,
					PeerId:   peerId,
					Filename: basename,
					Md5sum:   "", // empty for this test case
				})
			}
		}
		mu2.Unlock()
	}

	wg.Wait()
	log.Printf("*** stop sending heartbeats to master")

	if len(timeout_ids) != 1 {
		t.Errorf("no time-out slaves found")
	}

	// 我们模拟了3个 influxdb data node: 1-101, 1-102, 1-103
	// master 应该分配出3个 slave, 并将其记录在 activeSlaves; 后备节点还剩2个.
	// 但是我们现在让 activeSlaves 中的一个 slave 心跳超时, master 就会将这个
	// 超时的 slave 踢出, 然后从后备节点中选择一个将其取代
	assignedSlaveIds := toSortedInts(assignedSlaves)
	if len(assignedSlaveIds) != 3 {
		t.Error("len(assignedSlaveIds) != 3")
	}
	activeSlaveIds := toSortedInts2(m.activeSlaves)
	if len(activeSlaveIds) != len(assignedSlaves) {
		t.Errorf("len(activeSlaveIds) != len(assignedSlaves)")
	}
	if reflect.DeepEqual(activeSlaveIds, assignedSlaveIds) {
		t.Errorf("activeSlaveIds %v == assignedSlaveIds %v", activeSlaveIds, assignedSlaveIds)
	}
	if len(m.backupSlaves) != 1 {
		t.Errorf("len(m.backupSlaves) != 1")
	}

	// 根据发送的心跳包 pkgs 来看, 每个文件都有3个副本, 并且应该存储在3个相同的 slave 上面
	// ——这些 slave 就是 master 分配出去的那3个.
	for _, file := range []string{"file1.tsm", "file2.tsm", "file3.tsm"} {
		if _, ok := m.fileReplicaAddr[file][timeout_id]; ok {
			t.Errorf("timeout slave %d should not exist in fileReplicaAddr", timeout_id)
		}
	}
}

func TestConcurrentHeartbeatAndRPC(t *testing.T) {
	rpcname := "Master.AssignSlave"
	rpcport := 1234
	stopC := make(chan struct{})
	notifyC := make(chan *string)
	m := NewMaster(nil, rpcport, notifyC, stopC, func(id int) {})
	m.Run()

	pkgs := []heartbeatPackage{
		{1, "10.0.0.1", 10, 20, []FileStore{}},
		{2, "10.0.0.2", 10, 20, []FileStore{}},
		{3, "10.0.0.3", 10, 20, []FileStore{}},
		{4, "10.0.0.4", 10, 20, []FileStore{}},
		{5, "10.0.0.5", 10, 20, []FileStore{}},
		{6, "10.0.0.6", 10, 20, []FileStore{}},
	}
	args1 := []AssignSlaveArg{
		{Filename: "xxx_1_101.tsm"},
		{Filename: "xxx_1_102.tsm"},
		{Filename: "xxx_1_103.tsm"},
	}
	args2 := []AssignSlaveArg{
		{Filename: "xxx_2_201.tsm"},
		{Filename: "xxx_2_202.tsm"},
		{Filename: "xxx_2_203.tsm"},
	}

	var mu sync.Mutex
	id2addr := make(map[int]string)

	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(1000 * time.Millisecond):
				for _, p := range pkgs {
					v := p.mustMarshal()
					notifyC <- &v
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(1000 * time.Millisecond):
				for _, arg := range args1 {
					var reply AssignSlaveReply
					if ok := call(rpcname, rpcport, &arg, &reply); !ok {
						t.Fatal("RPC error")
					}
					mu.Lock()
					id2addr[reply.SlaveId] = reply.SlaveAddr
					mu.Unlock()
				}
			}
		}
	}()

	time.Sleep(5 * time.Second)

	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(1000 * time.Millisecond):
				for _, arg := range args2 {
					var reply AssignSlaveReply
					if ok := call(rpcname, rpcport, &arg, &reply); !ok {
						t.Fatal("RPC error")
					}
					mu.Lock()
					id2addr[reply.SlaveId] = reply.SlaveAddr
					mu.Unlock()
				}
			}
		}
	}()

	time.Sleep(5 * time.Second)

	// 心跳包 pkgs 来自 6 个 slaves, args1 和 args2 代表来自 6 个 data node 的 AssignSlave RPC 参数.
	// master 应该把 6 个 slaves 全部分配出去, 不重不漏.
	if len(id2addr) != len(pkgs) {
		t.Error("Some slaves may be assigned to more than one data nodes")
	}

	close(done)
}

func TestSlaveTimeout(t *testing.T) {
	rpcport := 1234
	stopC := make(chan struct{})
	notifyC := make(chan *string)
	m := NewMaster(nil, rpcport, notifyC, stopC, func(id int) {})
	m.Run()

	pkgs := []heartbeatPackage{
		{1, "10.0.0.1", 10, 20, []FileStore{{GroupId: 1, PeerId: 101, Filename: "xxx.tsm", Md5sum: "abcde"}}},
		{2, "10.0.0.2", 10, 20, []FileStore{{GroupId: 1, PeerId: 102, Filename: "xxx.tsm", Md5sum: "abcde"}}},
		{3, "10.0.0.3", 10, 20, []FileStore{{GroupId: 1, PeerId: 103, Filename: "xxx.tsm", Md5sum: "abcde"}}},
		{4, "10.0.0.4", 10, 20, []FileStore{}},
		{5, "10.0.0.5", 10, 20, []FileStore{}},
		{6, "10.0.0.6", 10, 20, []FileStore{}},
	}

	n := len(pkgs)
	var wg sync.WaitGroup
	done := make([]chan struct{}, n)
	for i := 0; i < n; i++ {
		done[i] = make(chan struct{})
		wg.Add(1)
		go func(ii int) {
			for {
				select {
				case <-done[ii]:
					wg.Done()
					return
				case <-time.After(1000 * time.Millisecond):
					v := pkgs[ii].mustMarshal()
					notifyC <- &v
				}
			}
		}(i)
	}

	time.Sleep(HeartbeatTimeout)

	// 让一个存储有文件的 slave{1,2,3} 中的任意一个宕机, master 发现它心跳超时后
	// 应该从 backupSlaves{4,5,6} 选择一个替补上去.
	// 最后 activeSlaves 应包含 {4,5,6} 中的一个, backupSlaves 只剩两个, 且为 {4,5,6} 的子集
	timeoutSlaveId := pickKey2(m.activeSlaves)
	close(done[timeoutSlaveId-1])

	// waiting for the timeout slave to be detected
	time.Sleep(HeartbeatTimeout)

	for i := 0; i < n; i++ {
		if i == timeoutSlaveId-1 {
			continue
		}
		close(done[i])
	}

	if exp, got := 3, len(m.activeSlaves); exp != got {
		t.Errorf("len(m.activeSlaves) exp %d, got %v", exp, got)
	}
	if exp, got := 2, len(m.backupSlaves); exp != got {
		t.Errorf("len(m.backupSlaves) exp %d, got %v", exp, got)
	}

	var res []int
	res = intersectedKeys1(m.activeSlaves, []int{1, 2, 3})
	if len(res) != 2 {
		t.Errorf("activeSlaves: %v", m.activeSlaves)
	}
	res = intersectedKeys1(m.activeSlaves, []int{4, 5, 6})
	if len(res) != 1 {
		t.Errorf("activeSlaves: %v", m.activeSlaves)
	}
	res = intersectedKeys2(m.backupSlaves, []int{4, 5, 6})
	if len(res) != 2 {
		t.Errorf("backupSlaves: %v", m.backupSlaves)
	}

	wg.Wait()
}
