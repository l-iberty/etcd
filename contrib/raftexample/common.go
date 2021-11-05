package main

import (
	"encoding/json"
	"fmt"
)

type heartbeatPackage struct {
	SlaveId      int
	SlaveHost    string
	SlavePort    int
	SlaveCmdPort int
	FileStores   []FileStore
}

type FileStore struct {
	GroupId  int
	PeerId   int
	Filename string
	Md5sum   string
}

func (fs FileStore) String() string {
	return fmt.Sprintf("[GroupId: %d, PeerId: %d, Filename: %s, Md5sum: %s]", fs.GroupId, fs.PeerId, fs.Filename, fs.Md5sum)
}

func (p *heartbeatPackage) mustUnmarshal(data []byte) {
	if err := json.Unmarshal(data, p); err != nil {
		panic(err)
	}
}

func (p *heartbeatPackage) mustMarshal() string {
	s, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	return string(s)
}
