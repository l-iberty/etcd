package main

type AssignSlaveArg struct {
	Filename string
}

type AssignSlaveReply struct {
	SlaveId   int
	SlaveAddr string
}
