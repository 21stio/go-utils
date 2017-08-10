package statistics

import (
	"errors"
)

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

	stats.Median, err = Median(numbers)
	if err != nil {
		return
	}

	stats.Sum = Sum(numbers)

	stats.Min, err = Min(numbers)
	if err != nil {
		return
	}

	stats.Max, err = Max(numbers)
	if err != nil {
		return
	}

	stats.Count = Count(numbers)

	return
}

func Median(numbers []float64) (median float64, err error) {
	l := len(numbers)

	if l == 0 {
		err = errors.New("len(numbers) == 0")
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
	for _, x := range numbers {
		total += x
	}

	return total
}

func Min(numbers []float64) (min float64, err error) {
	l := len(numbers)

	if l == 0 {
		err = errors.New("len(numbers) == 0")
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

func Max(numbers []float64) (max float64, err error) {
	l := len(numbers)

	if l == 0 {
		err = errors.New("len(numbers) == 0")
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