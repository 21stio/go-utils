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

type entry struct {
	level     Level
	pkg       string
	function  string
	message   string
	fields    Fields
	timestamp time.Time
}

var E = make(chan entry)

func Log(l Level, pkg string, function string, message string, f Fields) {
	e := entry{
		level:     l,
		message:   message,
		pkg:       pkg,
		function:  function,
		fields:    f,
		timestamp: time.Now(),
	}

	E <- e
}

func HandleEntries(l Level, i map[string]map[string]bool, h func(e entry)) {
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

func Print(e entry) {
	fmt.Printf("%v %v %v %v %+v %v\n", e.timestamp.String(), e.level, e.pkg, e.function, e.fields, e.message)
}
