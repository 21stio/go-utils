package statistics

import "math"

type Stats struct {
	Average float64
	Median float64
	Sum float64
	Min float64
	Max float64
	Count uint64
}

func GetStats(numbers []float64) (stats Stats, err error) {
	stats.Average = Average(numbers)

	stats.Median = Median(numbers)

	stats.Sum = Sum(numbers)

	stats.Min = Min(numbers)

	stats.Max = Max(numbers)

	stats.Count = Count(numbers)

	return
}

func Median(numbers []float64) (median float64) {
	l := len(numbers)

	if l == 0 {
		median = math.NaN()

		return
	}

	middle := len(numbers) / 2
	median = numbers[middle]
	if len(numbers)%2 == 0 {
		median = (median + numbers[middle-1]) / 2
	}

	return
}

func Sum(numbers []float64) (total float64) {
	//total = math.NaN()

	for _, x := range numbers {
		total += x
	}

	return total
}

func Min(numbers []float64) (min float64) {
	l := len(numbers)

	if l == 0 {
		min = math.NaN()

		return
	}

	min = numbers[0]
	for i := 1; i < l; i++ {
		if numbers[i] < min {
			min = numbers[i]
		}
	}

	return
}

func Max(numbers []float64) (max float64) {
	l := len(numbers)

	if l == 0 {
		max = math.NaN()

		return
	}

	max = numbers[0]
	for i := 1; i < l; i++ {
		if numbers[i] > max {
			max = numbers[i]
		}
	}

	return
}

func Count(numbers []float64) (count uint64) {
	return uint64(len(numbers))
}

func Average(numbers []float64) (average float64) {
	return Sum(numbers) / float64(len(numbers))
}