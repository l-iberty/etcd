package main

type AssignSlaveArg struct {
	Filename string
}

type AssignSlaveReply struct {
	SlaveId   int
	SlaveAddr string
}

type GetRemoteNodeAddrArg struct {
	GroupID int
	PeerID  int
}

type GetRemoteNodeAddrReply struct {
	RemoteAddr string
}