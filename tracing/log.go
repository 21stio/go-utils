package tracing

import (
	"github.com/rs/zerolog"
)

func pkg(event *zerolog.Event, function string) (*zerolog.Event) {
	return event.
		Str("p", "github.com/21stio/go-utils/tracing").
		Str("f", function)
}