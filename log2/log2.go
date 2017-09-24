package log2

import (
	"time"
	"fmt"
)

type Level float64
type Fields map[string]interface{}

const (
	DEBUG Level = iota * 1000
	INFO
	WARN
	ERROR
	FATAL
)

type Entry struct {
	Level     Level
	Pkg       string
	Function  string
	Message   string
	Fields    Fields
	Timestamp time.Time
}

var E = make(chan Entry)

func Log(l Level, pkg string, function string, message string, f Fields) {
	e := Entry{
		Level:     l,
		Message:   message,
		Pkg:       pkg,
		Function:  function,
		Fields:    f,
		Timestamp: time.Now(),
	}

	E <- e
}

func HandleEntries(l Level, i map[string]map[string]bool, h func(e Entry)) {
	for e := range E {
		if e.Level < l {
			continue
		}

		_, exists:= i[e.Pkg][e.Function]
		if exists {
			continue
		}

		h(e)
	}
}

func Print(e Entry) {
	fmt.Printf("%v %v %v %v %+v %v\n", e.Timestamp.String(), e.Level, e.Pkg, e.Function, e.Fields, e.Message)
}
