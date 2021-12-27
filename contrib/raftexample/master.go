package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/raft"
)

const (
	HeartbeatTimeout = 5000 * time.Millisecond
	HeartbeatKey     = "/heartbeat"
)

type Master struct {
	sync.RWMutex
	rc                 *raftNode
	rpcport            int
	stopC              <-chan struct{}
	notifyC            <-chan *string
	timeoutCallback    func(int)
	slaveHost          map[int]string               // slaveID -> host
	slavePort          map[int]int                  // slaveID -> port
	slaveCmdPort       map[int]int                  // slaveID -> cmd port
	slaveFileStore     map[int]map[string]FileStore // slaveID -> []FileStore (一个slave上面存储了多个文件)
	slaveLastHeartbeat map[int]time.Time            // slaveID -> last heartbeat time
	activeSlaves       map[int]int                  // active slaveIDs, 活跃的slaves, 即被 AssignSlave 分配出去的slaves. key = slaveID, value = groupID (slave上存储的文件来自哪一个data node集群)
	backupSlaves       map[int]struct{}             // back-up slaveIDs, 后备slaves, 用于替换出故障(心跳超时)的slaves
	node2slave         map[string]int               // influxdb data node -> slaveID, key = data node 的标签: groupID-peerID

	// 每个文件都会有3个副本, 来自 raft 集群, 分别存储在3个slave上, fileReplicaAddr记录了每个文件的3个副本的存储位置, 即 slaveID
	fileReplicaAddr map[string]map[int]struct{}
}

func parseFilename(filename string) (dir, basename string, groudId int, peerId int) {
	dir = filepath.Dir(filename)
	basename = filepath.Base(filename)
	toks1 := strings.Split(basename, ".") // toks1[0] = xxx_<GID>_<PeerID>, toks1[1] = yyy
	toks2 := strings.Split(toks1[0], "_") // toks2[0] = xxx, toks2[1] = <GID>, toks2[2] = <PeerID>
	basename = toks2[0] + "." + toks1[1]  // basename = xxx.yyy
	groudId, err := strconv.Atoi(toks2[1])
	if err != nil {
		panic(err)
	}
	peerId, err = strconv.Atoi(toks2[2])
	if err != nil {
		panic(err)
	}
	return
}

func (m *Master) AssignSlave(arg *AssignSlaveArg, reply *AssignSlaveReply) error {
	m.Lock()
	defer m.Unlock()

	_, _, groupId, peerId := parseFilename(arg.Filename)
	tag := nodeTag(groupId, peerId)

	// 是否有一个 slave 已经分配给 groupId-peerId 标识的 influxdb data node ?
	var slaveId int
	if id, ok := m.node2slave[tag]; ok {
		// 如果有的话就直接把这个 slave 分配出去
		slaveId = id
	} else {
		// 如果没有的话就从后备节点中选择一个
		for id := range m.backupSlaves {
			m.node2slave[tag] = id
			slaveId = id
			break
		}
	}
	if slaveId == 0 {
		return errors.New("no available slaves")
	}

	m.activeSlaves[slaveId] = groupId

	delete(m.backupSlaves, slaveId) // slave 被分配出去后, 它就不能再被用做后备节点

	reply.SlaveId = slaveId
	reply.SlaveAddr = serverAddr(m.slaveHost[slaveId], m.slavePort[slaveId])
	return nil
}

func (m *Master) GetRemoteNodeAddr(arg *GetRemoteNodeAddrArg, reply *GetRemoteNodeAddrReply) error {
	m.RLock()
	defer m.RUnlock()

	log.Printf("GetRemoteNodeAddr: node2slave %+v", m.node2slave)

	tag := nodeTag(arg.GroupID, arg.PeerID)
	slave := m.node2slave[tag]
	host := m.slaveHost[slave]

	file, err := os.OpenFile("remote_node_ports.json", os.O_RDONLY, 0664)
	if err != nil {
		return err
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	data := make(map[string]int)
	if err = dec.Decode(&data); err != nil {
		return err
	}
	reply.RemoteAddr = fmt.Sprintf("%s:%d", host, data[strconv.Itoa(slave)])
	return nil
}

func (m *Master) serve() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":"+strconv.Itoa(m.rpcport))
	if err != nil {
		panic(err)
	}
	time.Sleep(1 * time.Second)
	log.Printf("rpc port: %d", m.rpcport)
	go http.Serve(l, nil)
}

func (m *Master) checkSlaveTimeout() {
	m.Lock()
	defer m.Unlock()

	for id := range m.slaveLastHeartbeat {
		if time.Since(m.slaveLastHeartbeat[id]) > HeartbeatTimeout {
			log.Printf("slave %d(%s) heartbeat timeout", id, m.slaveHost[id])
			// log.Printf("slave %d(%s) heartbeat timeout. Files:", id, m.slaveHost[id])
			// for _, fs := range m.slaveFileStore[id] {
			// 	log.Printf("    %s", fs)
			// }

			m.processTimeoutSlave(id)
		}
	}
}

func (m *Master) processTimeoutSlave(id int) {
	groupId := m.activeSlaves[id]
	m.timeoutCallback(id)
	m.kickoutTimeoutSlave(id)

	// 如果超时 slave 节点上没有存储有文件就不进行额外操作
	if len(m.slaveFileStore[id]) == 0 {
		log.Printf("no files found in slave %d", id)
		return
	}
	// kick out timeout slave id from slaveFileStore.
	delete(m.slaveFileStore, id)

	var status raft.Status
	if m.rc != nil {
		status = m.rc.node.Status()
		if status.ID != status.Lead {
			log.Printf("master %d is not a leader, shouldn't send cmd to slave", status.ID)
			return
		}
	}

	// 从 activeSlaves 中随机选择一个 slaveSrc, 从 backupSlaves 中随机选择一个 slaveDst,
	// 给 slaveSrc 发送一个命令, 把其上的 TSM 发送到 slaveDst
	activeSlaves := make(map[int]struct{})
	for i := range m.activeSlaves {
		if m.activeSlaves[i] == groupId {
			activeSlaves[i] = struct{}{}
		}
	}
	slaveSrc := pickKey(activeSlaves)
	slaveDst := pickKey(m.backupSlaves)
	if slaveSrc == 0 || slaveDst == 0 {
		log.Printf("WARNING: unable to find an available slave to replace the timeout one")
		log.Printf("         slaveSrc: %v, slaveDst: %v", slaveSrc, slaveDst)
		log.Printf("         activeSlaves: %v, backupSlaves: %v", m.activeSlaves, m.backupSlaves)
		return
	}
	// slaveDst 将承担存储任务, 不再是后备节点了, 将其记录到 activeSlaves 和 assignedSlaves
	delete(m.backupSlaves, slaveDst)
	m.activeSlaves[slaveDst] = groupId

	for tag, slaveId := range m.node2slave {
		if slaveId == id {
			// tag 标识的 influxdb data node 上的 L2 TSM 原本是镜像到 slave id 上面,
			// 现在 slave id 心跳超时而被踢出. 我们选择后备节点 slaveDst 用于替换 slave id
			m.node2slave[tag] = slaveDst
		}
	}

	log.Printf("processTimeoutSlave: node2slave %+v", m.node2slave)

	srcSlaveAddr := serverAddr(m.slaveHost[slaveSrc], m.slaveCmdPort[slaveSrc]) // for receiving cmd from master
	dstSlaveAddr := serverAddr(m.slaveHost[slaveDst], m.slavePort[slaveDst])    // for receiving files from slaveSrc

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		log.Printf("master %d sending cmd to [%d] %s: copy files to [%d] %s", status.ID, slaveSrc, srcSlaveAddr, slaveDst, dstSlaveAddr)
		conn, err := net.Dial("tcp", srcSlaveAddr)
		if err != nil {
			log.Printf("WARNING: unable to connect to slave %s", srcSlaveAddr)
			return
		}
		defer conn.Close()

		data := fmt.Sprintf("%s#%s", dstSlaveAddr, m.slaveTag(slaveDst))
		mustWrite(conn, []byte(data))
		buf := make([]byte, 100)
		n := mustRead(conn, buf) // waiting for response "OK"
		if string(buf[:n]) != "OK" {
			log.Printf("WARNING: unable to recv response from slave %s", srcSlaveAddr)
			return
		}
		log.Printf("master %d DONE sending cmd to [%d] %s: copy files to [%d] %s", status.ID, slaveSrc, srcSlaveAddr, slaveDst, dstSlaveAddr)
	}()
	wg.Wait()
}

func (m *Master) slaveTag(id int) string {
	for tag, id2 := range m.node2slave {
		if id == id2 {
			return tag
		}
	}
	panic("no slave found")
}

func pickKey(a map[int]struct{}) int {
	for k := range a {
		return k
	}
	return 0
}

func pickKey2(a map[int]int) int {
	for k := range a {
		return k
	}
	return 0
}

func mustRead(conn net.Conn, buf []byte) int {
	n, err := conn.Read(buf)
	if err != nil {
		panic(err)
	}
	return n
}

func mustWrite(conn net.Conn, buf []byte) int {
	n, err := conn.Write(buf)
	if err != nil {
		panic(err)
	}
	return n
}

func (m *Master) kickoutTimeoutSlave(id int) {
	delete(m.slaveHost, id)
	delete(m.slavePort, id)
	delete(m.slaveCmdPort, id)
	delete(m.slaveLastHeartbeat, id)
	delete(m.activeSlaves, id)
	delete(m.backupSlaves, id)

	for k := range m.fileReplicaAddr {
		delete(m.fileReplicaAddr[k], id)
	}
}

func (m *Master) printFileReplicaAddr() {
	fileReplicaAddr := make(map[string][]int)
	for f, ids := range m.fileReplicaAddr {
		fileReplicaAddr[f] = []int{}
		for id := range ids {
			fileReplicaAddr[f] = append(fileReplicaAddr[f], id)
		}
	}
	log.Printf("fileReplicaAddr: %v", fileReplicaAddr)
}

func NewMaster(rc *raftNode, rpcport int, notifyC <-chan *string, stopC <-chan struct{}, timeoutCallback func(int)) *Master {
	m := &Master{
		rc:                 rc,
		rpcport:            rpcport,
		notifyC:            notifyC,
		stopC:              stopC,
		timeoutCallback:    timeoutCallback,
		slaveHost:          make(map[int]string),
		slavePort:          make(map[int]int),
		slaveCmdPort:       make(map[int]int),
		slaveFileStore:     make(map[int]map[string]FileStore),
		slaveLastHeartbeat: make(map[int]time.Time),
		activeSlaves:       make(map[int]int),
		backupSlaves:       make(map[int]struct{}),
		node2slave:         make(map[string]int),
		fileReplicaAddr:    make(map[string]map[int]struct{}),
	}
	return m
}

func (m *Master) Run() {
	go func() {
		log.Printf("starting master")
		for {
			select {
			case <-m.stopC:
				return
			case s := <-m.notifyC:
				pkg := &heartbeatPackage{}
				pkg.mustUnmarshal([]byte(*s))
				// log.Printf("heartbeat from slave %d", pkg.SlaveId)

				m.handleHeartbeat(pkg)
			}
			m.checkSlaveTimeout()
		}
	}()

	m.serve()
}

func (m *Master) handleHeartbeat(pkg *heartbeatPackage) {
	m.Lock()
	defer m.Unlock()

	m.slaveHost[pkg.SlaveId] = pkg.SlaveHost
	m.slavePort[pkg.SlaveId] = pkg.SlavePort
	m.slaveCmdPort[pkg.SlaveId] = pkg.SlaveCmdPort
	m.slaveLastHeartbeat[pkg.SlaveId] = time.Now()

	// 如果 slave 节点上存储有来自 data node 的文件, 就将其记录到 activeSlave,
	// 否则记录到 backupSlaves 用做后备节点——如果它未被 AssignSlave RPC 分配出去的话.
	if len(pkg.FileStores) > 0 {
		m.activeSlaves[pkg.SlaveId] = pkg.FileStores[0].GroupId
		delete(m.backupSlaves, pkg.SlaveId)
	} else {
		if _, ok := m.activeSlaves[pkg.SlaveId]; !ok {
			m.backupSlaves[pkg.SlaveId] = struct{}{}
		}
	}

	for _, fs := range pkg.FileStores {
		// 每个文件可能有若干副本, 需要记录每个文件的副本都存储在哪些 slave 节点上
		m.putFileReplicaAddr(fs.Filename, pkg.SlaveId)

		// 记录每个 slave 节点上存储的文件
		m.putSlaveFileStore(pkg.SlaveId, fs)

		// 每个 data node 都会被分配一个 slave 节点, 需要将这种对应关系记录到 node2slave
		tag := nodeTag(fs.GroupId, fs.PeerId)
		m.node2slave[tag] = pkg.SlaveId
	}
	// m.printFileReplicaAddr()
}

func (m *Master) putFileReplicaAddr(filename string, slaveId int) {
	if _, ok := m.fileReplicaAddr[filename]; !ok {
		m.fileReplicaAddr[filename] = make(map[int]struct{})
	}
	m.fileReplicaAddr[filename][slaveId] = struct{}{}
}

func (m *Master) putSlaveFileStore(slaveId int, fs FileStore) {
	if _, ok := m.slaveFileStore[slaveId]; !ok {
		m.slaveFileStore[slaveId] = make(map[string]FileStore)
	}
	m.slaveFileStore[slaveId][fs.Filename] = fs
}

func nodeTag(groupId, peerId int) string {
	tag := fmt.Sprintf("%d_%d", groupId, peerId)
	return tag
}

func serverAddr(host string, port int) string {
	addr := host + ":" + strconv.Itoa(port)
	return addr
}
