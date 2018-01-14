package main

import (
	"testing"
)

func Test_mapLength(t *testing.T) {
	for length := 0; length < 20; length++ {
		for b := 1; b < 20; b++ {
			for p := 1; p < 20; p++ {
				addedLength := 0
				for pI := 0; pI < p; pI++ {
					addedLength += mapLength(length, p, b, pI)
				}

				if addedLength != length {
					t.Errorf("Added up length was %v, expected length was %v. For b:%v, p:%v\n", addedLength, length, b, p)
				}
			}
		}
	}
}

func getMinusOneSlice(length int) []int {
	s := make([]int, length)
	for i := 0; i < length; i++ {
		s[i] = -1
	}

	return s
}

func Test_mapIndex(t *testing.T) {
	for length := 0; length < 20; length++ {
		for b := 1; b < 20; b++ {
			for p := 1; p < 20; p++ {
				owner := getMinusOneSlice(length)

				for pI := 0; pI < p; pI++ {
					for i := 0; i < mapLength(length, p, b, pI); i++ {
						ownedIndex := mapIndex(p, b, pI, i)
						if owner[ownedIndex] != -1 {
							t.Errorf("Tried to double own an index, owner already set to pI of %v, currently on pI of %v with i %v. With length:%v, b:%v, p:%v\n", owner[ownedIndex], pI, i, length, b, p)
						} else {
							owner[ownedIndex] = pI
						}
					}
				}

				currenOwner := -1
				for i := 0; i < length; i++ {
					owns := owner[i]
					if owns == -1 {
						t.Errorf("Found unowned index at %v. With length:%v, b:%v, p:%v. Owner slice is %v\n", i, length, b, p, owner)
					}

					if owns == currenOwner {
						continue
					}
					nextOwner := (currenOwner + 1) % p
					if owns == nextOwner {
						currenOwner = nextOwner
						continue
					}

					t.Errorf("Wrong owner transition, was on owner %v and next owner was %v but owner %v found at i: %v. With length:%v, b:%v, p:%v. Owner slice is %v\n", currenOwner, nextOwner, owns, i, length, b, p, owner)
				}
			}
		}
	}
}
