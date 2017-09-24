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
	level     Level
	pkg       string
	function  string
	message   string
	fields    Fields
	timestamp time.Time
}

var E = make(chan Entry)

func Log(l Level, pkg string, function string, message string, f Fields) {
	e := Entry{
		level:     l,
		message:   message,
		pkg:       pkg,
		function:  function,
		fields:    f,
		timestamp: time.Now(),
	}

	E <- e
}

func HandleEntries(l Level, i map[string]map[string]bool, h func(e Entry)) {
	for e := range E {
		if e.level < l {
			continue
		}

		_, exists:= i[e.pkg][e.function]
		if exists {
			continue
		}

		h(e)
	}
}

func Print(e Entry) {
	fmt.Printf("%v %v %v %v %+v %v\n", e.timestamp.String(), e.level, e.pkg, e.function, e.fields, e.message)
}
