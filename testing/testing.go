package testing

type TestBag struct {
	results []bool
}

func (t *TestBag) AddResult(result bool) {
	t.results = append(t.results, result)
}

func (t *TestBag) HasFailed() (bool) {
	for _, result := range t.results {
		if result == false {
			return true
		}
	}

	return false
}
