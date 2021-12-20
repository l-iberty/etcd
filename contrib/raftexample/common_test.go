package main

import (
	"reflect"
	"testing"
)

func Test_HeartbeatPackageMarShal(t *testing.T) {
	p := heartbeatPackage{
		SlaveId:      1,
		SlaveHost:    "http://10.0.0.1",
		SlavePort:    1000,
		SlaveCmdPort: 1001,
		FileStores: []FileStore{
			{100, 1, "data/xxx", "12345"},
			{101, 2, "data/yyy", "abcde"},
		},
	}
	exp := `{"SlaveId":1,"SlaveHost":"http://10.0.0.1","SlavePort":1000,"SlaveCmdPort":1001,"FileStores":[{"GroupId":100,"PeerId":1,"Filename":"data/xxx","Md5sum":"12345"},{"GroupId":101,"PeerId":2,"Filename":"data/yyy","Md5sum":"abcde"}]}`
	got := p.mustMarshal()
	if exp != got {
		t.Errorf("\nexp: %v,\ngot: %v", exp, got)
	}
}

func Test_HeartbeatPackageUnmarshal(t *testing.T) {
	data := `{"SlaveId":1,"SlaveHost":"http://10.0.0.1","SlavePort":1000,"SlaveCmdPort":1001,"FileStores":[{"GroupId":1,"PeerId":100,"Filename":"data/xxx","Md5sum":"12345"},{"GroupId":2,"PeerId":101,"Filename":"data/yyy","Md5sum":"abcde"}]}`
	p := heartbeatPackage{}
	p.mustUnmarshal([]byte(data))
	if exp, got := 1, p.SlaveId; exp != got {
		t.Errorf("exp SlaveId: %v, got %v", exp, got)
	}
	if exp, got := "http://10.0.0.1", p.SlaveHost; exp != got {
		t.Errorf("exp SlaveHost: %v, got %v", exp, got)
	}
	if exp, got := 1000, p.SlavePort; exp != got {
		t.Errorf("exp SlavePort: %v, got %v", exp, got)
	}
	if exp, got := 1001, p.SlaveCmdPort; exp != got {
		t.Errorf("exp SlaveCmdPort: %v, got %v", exp, got)
	}
	fs0 := FileStore{
		GroupId:  1,
		PeerId:   100,
		Filename: "data/xxx",
		Md5sum:   "12345",
	}
	fs1 := FileStore{
		GroupId:  2,
		PeerId:   101,
		Filename: "data/yyy",
		Md5sum:   "abcde",
	}
	fs := []FileStore{fs0, fs1}
	if !reflect.DeepEqual(fs, p.FileStores) {
		t.Errorf("exp FileStores: %v,\ngot %v", fs, p.FileStores)
	}
}
