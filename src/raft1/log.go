package raft1

import (
	"errors"
)

type LogManager interface {
    AppendLog(Log *LogEntry) int
    RemoveLogsTo(index int) error
	RemoveLogsFrom(index int) error
    GetLastLogIndex() int
    GetSize() int
    GetLogs(buf *[]*LogEntry, from, to int) error
	GetLog(index int) (error, *LogEntry)
	Serialize() []byte
}

type HashLogManager struct {
	m map[int]*LogEntry
	startIndex, endIndex int
}

func NewHashLogManager() *HashLogManager {
	this := &HashLogManager{}
	this.m = make(map[int]*LogEntry)
	this.startIndex = 0
	this.endIndex = 0
	return this
}

func (this *HashLogManager) AppendLog(Log *LogEntry) int {
	this.endIndex++
	this.m[this.endIndex] = Log
	if len(this.m) == 1 {
		this.startIndex = this.endIndex
	}
	return this.endIndex
}

func (this *HashLogManager) RemoveLogsTo(index int) error {
	if index == 0 || index < this.startIndex || index > this.endIndex {
		return errors.New("index out of range")
	}
	for ; this.startIndex <= index; this.startIndex++ {
		delete(this.m, this.startIndex)
	}
	return nil
}

func (this *HashLogManager) RemoveLogsFrom(index int) error {
	if index == 0 || index < this.startIndex || index > this.endIndex {
		return errors.New("index out of range")
	}
	for j := index ;j <= this.endIndex; j++ {
		delete(this.m, j)
	}
	this.endIndex = index - 1
	return nil
}

func (this *HashLogManager) GetLastLogIndex() int { return this.endIndex }

func (this *HashLogManager) GetSize() int { return len(this.m) }

func (this *HashLogManager) GetLogs(buf *[]*LogEntry, from, to int) error {
	if from == 0 || to == 0 { 
		return errors.New("from/to cannot be 0")
	} else if from > to || from < this.startIndex || to > this.endIndex {
		return errors.New("from/to out of range")
	} else {
		for i := from; i <= to; i++ { *buf = append(*buf, this.m[i]) }
		return nil
	}
}

func (this *HashLogManager) GetLog(index int) (error, *LogEntry) {
	if index == 0 || index < this.startIndex || index > this.endIndex {
		return errors.New("index out of range"), nil
	}
	return nil, this.m[index]
}

func (this *HashLogManager) Serialize() []byte {
	return nil
}