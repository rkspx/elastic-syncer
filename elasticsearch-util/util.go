package esutil

import (
	"math"
	"strconv"
)

func stringToInt(s string) int {
	if s == "" {
		return 0
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}

	if n > math.MaxInt {
		return 0
	}

	if n < 0 {
		return 0
	}

	return int(n)
}
