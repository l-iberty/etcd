package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	HeartbeatTimeout = 10 * time.Second
	HeartbeatKey     = "/heartbeat"
)

type Master struct {
	sync.RWMutex
	rpcport            int
	stopC              <-chan struct{}
	notifyC            <-chan *string
	timeoutCallback    func(int)
	slaveHost          map[int]string      // slave id -> host
	slaveFileStore     map[int][]FileStore // slave id -> []FileStore (一个Slave上面存储了多个文件, 每个文件对应一个FileStore)
	slaveLastHeartbeat map[int]time.Time   // slave id -> last heartbeat time

	// 每个文件都会有3个副本, 来自 raft 集群, 分别存储在3个 slave 上, fileReplicaAddr 记录了每个文件的3个副本的存储位置, 即 slave id
	fileReplicaAddr map[string]map[int]struct{}
}

type SlaveSortEnt struct {
	Id     int
	NFiles int
}

func getFileTag(filename string) string {
	// xxx_<GID>_<PeerID>.yyy -> xxx_<GID>
	toks := strings.Split(filename, "_")
	tag := fmt.Sprintf("%s_%s", toks[0], toks[1])
	return tag
}

func (m *Master) AssignSlave(arg *AssignSlaveArg, reply *AssignSlaveReply) error {
	m.RLock()
	defer m.RUnlock()

	var slaves []SlaveSortEnt
	for id := range m.slaveFileStore {
		slaves = append(slaves, SlaveSortEnt{
			Id:     id,
			NFiles: len(m.slaveFileStore[id]),
		})
	}

	sort.SliceStable(slaves, func(i, j int) bool {
		if slaves[i].NFiles == slaves[j].NFiles {
			return slaves[i].Id < slaves[j].Id
		}
		return slaves[i].NFiles < slaves[j].NFiles
	})

	for _, slave := range slaves {
		tags := make(map[string]struct{})
		for _, fs := range m.slaveFileStore[slave.Id] {
			t := getFileTag(fs.Filename)
			tags[t] = struct{}{}
		}
		t := getFileTag(arg.Filename)
		if _, ok := tags[t]; !ok {
			m.slaveFileStore[slave.Id] = append(m.slaveFileStore[slave.Id], FileStore{Filename: arg.Filename})
			reply.SlaveId = slave.Id
			reply.SlaveAddr = m.slaveHost[slave.Id]
			return nil
		}
	}

	return errors.New("no slaves can be assigned")
}

func (m *Master) serve() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":"+strconv.Itoa(m.rpcport))
	if err != nil {
		panic(err)
	}
	time.Sleep(2 * time.Second)
	log.Printf("rpc port: %d", m.rpcport)
	go http.Serve(l, nil)
}

func (m *Master) checkSlaveTimeout() {
	m.RLock()
	defer m.RUnlock()

	for id := range m.slaveLastHeartbeat {
		if time.Since(m.slaveLastHeartbeat[id]) > HeartbeatTimeout {
			log.Printf("slave %d(%s) heartbeat timeout. Files:", id, m.slaveHost[id])
			for _, fs := range m.slaveFileStore[id] {
				log.Printf("    %s", fs)
			}
			m.timeoutCallback(id)
		}
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
	log.Printf("%v", fileReplicaAddr)
}

func newMaster(rpcport int, notifyC <-chan *string, stopC <-chan struct{}, timeoutCallback func(int)) *Master {
	m := &Master{
		rpcport:            rpcport,
		notifyC:            notifyC,
		stopC:              stopC,
		timeoutCallback:    timeoutCallback,
		slaveHost:          make(map[int]string),
		slaveFileStore:     make(map[int][]FileStore),
		slaveLastHeartbeat: make(map[int]time.Time),
		fileReplicaAddr:    make(map[string]map[int]struct{}),
	}

	go func() {
		log.Printf("starting master")
		for {
			select {
			case <-m.stopC:
				return
			case s := <-m.notifyC:
				pkg := &heartbeatPackage{}
				pkg.mustUnmarshal([]byte(*s))
				log.Printf("heartbeat from slave %d", pkg.SlaveId)

				m.Lock()
				slaveId, slaveHost, files := pkg.SlaveId, pkg.SlaveHost, pkg.FileStores
				m.slaveHost[slaveId] = slaveHost
				m.slaveFileStore[slaveId] = files
				m.slaveLastHeartbeat[slaveId] = time.Now()

				for _, f := range files {
					tag := getFileTag(f.Filename)
					if _, ok := m.fileReplicaAddr[tag]; !ok {
						m.fileReplicaAddr[tag] = make(map[int]struct{})
					}
					m.fileReplicaAddr[tag][slaveId] = struct{}{}
				}
				m.printFileReplicaAddr()
				m.Unlock()
			case <-time.After(HeartbeatTimeout):
			}
			m.checkSlaveTimeout()
		}
	}()

	m.serve()
	return m
}
