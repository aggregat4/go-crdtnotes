package crdt

import (
	"bytes"
	"math/rand"
)

const (
	BASE      = 1 << 15 // 32768
	MIN_DIGIT = 0
	MAX_DIGIT = BASE - 1
)

type ID struct {
	Digits  []uint16 `json:"digits"`
	Replica uint16   `json:"replica"`
}

func (a ID) Compare(b ID) int {
	la, lb := len(a.Digits), len(b.Digits)
	for i := 0; i < la && i < lb; i++ {
		if a.Digits[i] < b.Digits[i] {
			return -1
		}
		if a.Digits[i] > b.Digits[i] {
			return 1
		}
	}
	if la < lb {
		return -1
	}
	if la > lb {
		return 1
	}
	if a.Replica < b.Replica {
		return -1
	}
	if a.Replica > b.Replica {
		return 1
	}
	return 0
}

// Between returns a new ID strictly between a and b (where a < b).
// If a==nil => -∞, if b==nil => +∞. Uses variable-length paths in base BASE.
func Between(a *ID, b *ID, replica uint16, rnd *rand.Rand) ID {
	var left, right []uint16
	if a != nil {
		left = a.Digits
	}
	if b != nil {
		right = b.Digits
	}

	var path []uint16
	for i := 0; ; i++ {
		l := MIN_DIGIT
		if i < len(left) {
			l = int(left[i])
		}
		r := MAX_DIGIT
		if i < len(right) {
			r = int(right[i])
		}

		if r-l > 1 {
			d := uint16(l + 1 + rnd.Intn((r - l - 1)))
			path = append(path, d)
			break
		}
		path = append(path, uint16(l))
		if l == r {
			continue
		}
	}
	return ID{Digits: path, Replica: replica}
}

// Key encodes ID for map keys.
func Key(id ID) string {
	buf := bytes.Buffer{}
	for _, d := range id.Digits {
		buf.WriteByte(byte(d >> 8))
		buf.WriteByte(byte(d))
	}
	buf.WriteByte(byte(id.Replica >> 8))
	buf.WriteByte(byte(id.Replica))
	return buf.String()
}
