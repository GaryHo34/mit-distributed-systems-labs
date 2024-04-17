package raft

import (
	"log"
	"runtime"
	"strings"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if !Debug {
		return
	}
	log.Printf(format, a...)
}

func DPrintfWithCaller(format string, a ...interface{}) {
	if !Debug {
		return
	}
	a = append([]interface{}{GetCaller()}, a...)
	log.Printf("(%s) -> "+format, a...)
}

func GetCaller() string {
	pc, _, _, _ := runtime.Caller(3)
	callerNames := strings.Split(runtime.FuncForPC(pc).Name(), ".")
	return callerNames[len(callerNames)-1]
}
