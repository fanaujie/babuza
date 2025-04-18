package raft

import (
	"github.com/fanaujie/babuza/ibabuza"
	"sync"
)

type NormalEntryResult interface {
	SendStateMachineAppliedResult(*Entry, ibabuza.ApplyResult)
}

var (
	entryPool = sync.Pool{}
)

type Entry struct {
	index      uint64
	term       uint64
	replyID    uint64
	seqNum     uint64
	reqTime    int64
	command    []byte
	isReplay   bool
	session    ibabuza.Session
	sendResult NormalEntryResult
}

func NewEntry(index, term, replyID, seqNum uint64, reqTime int64, cmd []byte, isReplay bool,
	session ibabuza.Session, sendResult NormalEntryResult) *Entry {

	e := entryPool.Get()
	if e == nil {
		return &Entry{
			index:      index,
			term:       term,
			replyID:    replyID,
			seqNum:     seqNum,
			reqTime:    reqTime,
			command:    cmd,
			isReplay:   isReplay,
			session:    session,
			sendResult: sendResult,
		}
	}
	ee := e.(*Entry)
	ee.index = index
	ee.term = term
	ee.replyID = replyID
	ee.seqNum = seqNum
	ee.reqTime = reqTime
	ee.command = cmd
	ee.isReplay = isReplay
	ee.session = session
	ee.sendResult = sendResult
	return ee
}

func (e *Entry) Term() uint64 {
	return e.term
}

func (e *Entry) Index() uint64 {
	return e.index
}
func (e *Entry) Command() []byte {
	return e.command
}

func (e *Entry) SendResponse(result any, err error) {
	ar := ibabuza.ApplyResult{
		LogIndex: e.index,
		Response: result,
		Error:    err,
	}
	e.session.AddResult(e.seqNum, e.reqTime, ar)
	e.sendResult.SendStateMachineAppliedResult(e, ar)
}

func (e *Entry) ReplyId() uint64 {
	return e.replyID
}

func (e *Entry) Release() {
	e.command = nil
	e.sendResult = nil
	entryPool.Put(e)
}
