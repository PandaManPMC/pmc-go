package mygql

import "github.com/PandaManPMC/pmc-go/fork"

var logs fork.Logs

func InitMyGQL(log fork.Logs) {
	logs = log
}

func Debug(msg string) {
	if nil == logs {
		return
	}
	logs.Debug(msg)
}

func Error(msg string, err error) {
	if nil == logs {
		return
	}
	logs.Error(msg, err)
}
