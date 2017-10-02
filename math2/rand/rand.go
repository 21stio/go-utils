package rand

import "math/rand"

func IntnRange(min int, max int) (int) {
	return rand.Intn(max-min) + min
}