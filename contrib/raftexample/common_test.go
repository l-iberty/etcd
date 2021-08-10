package main

import (
	"reflect"
	"testing"
)

func Test_HeartbeatPackageMarShal(t *testing.T) {
	p := heartbeatPackage{
		SlaveId:   1,
		SlaveHost: "10.0.0.1",
		FileStores: []FileStore{
			{100, 1, "/xxx","12345"},
			{101, 2, "/yyy","abcde"},
		},
	}
	exp := `{"SlaveId":1,"SlaveHost":"10.0.0.1","FileStores":[{"GroupId":100,"PeerId":1,"Filename":"/xxx","Md5sum":"12345"},{"GroupId":101,"PeerId":2,"Filename":"/yyy","Md5sum":"abcde"}]}`
	got := p.mustMarshal()
	if exp != got {
		t.Errorf("\nexp: %v,\ngot: %v", exp, got)
	}
}

func Test_HeartbeatPackageUnmarshal(t *testing.T) {
	data := `{"SlaveId":1,"SlaveHost":"http://10.0.0.1","FileStores":[{"GroupId":1,"PeerId":100,"Filename":"/home/xxx","Md5sum":"12345"},{"GroupId":2,"PeerId":101,"Filename":"/home/yyy","Md5sum":"abcdef"}]}`
	p := heartbeatPackage{}
	p.mustUnmarshal([]byte(data))
	if exp, got := 1, p.SlaveId; exp != got {
		t.Errorf("exp SlaveId: %v, got %v", exp, got)
	}
	if exp, got := "http://10.0.0.1", p.SlaveHost; exp != got {
		t.Errorf("exp SlaveHost: %v, got %v", exp, got)
	}
	if exp, got := 2, len(p.FileStores); exp != got {
		t.Errorf("exp len(Filestores): %v, got %v", exp, got)
	}
	fs0 := FileStore{
		GroupId:  1,
		PeerId:   100,
		Filename: "/home/xxx",
		Md5sum:   "12345",
	}
	fs1 := FileStore{
		GroupId:  2,
		PeerId:   101,
		Filename: "/home/yyy",
		Md5sum:   "abcdef",
	}
	if !reflect.DeepEqual(fs0, p.FileStores[0]) {
		t.Errorf("exp %v,\ngot %v", fs0, p.FileStores[0])
	}
	if !reflect.DeepEqual(fs1, p.FileStores[1]) {
		t.Errorf("exp %v,\ngot %v", fs1, p.FileStores[1])
	}
}
