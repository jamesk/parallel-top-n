package main

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func isNeutralised(list []int, pivotValue int, leftOrRight int) bool {
	for _, v := range list {
		if (leftOrRight == 0 || leftOrRight == -1) && v >= pivotValue {
			return false
		}
		if (leftOrRight == 0 || leftOrRight == 1) && v < pivotValue {
			return false
		}
	}

	return true
}

type LeftRight struct {
	left, right SubListDefinition
}

func Test_neutralise(t *testing.T) {
	shuffles := 10
	lists := [][]int{
		[]int{1, 2},
		[]int{2, 2},
		[]int{1, 2, 2},
		[]int{1, 2, 3},
		[]int{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}

	for _, originalList := range lists {
		leftRights := []LeftRight{}
		for leftLength := 1; leftLength <= len(originalList)-1; leftLength++ {
			for leftI := 0; leftI+leftLength <= len(originalList)-1; leftI++ {
				leftEndIndex := leftI + leftLength - 1

				for rightLength := 1; rightLength+leftLength <= len(originalList); rightLength++ {
					for rightI := leftEndIndex + 1; rightI+rightLength <= len(originalList)-1; rightI++ {
						left := SubListDefinition{leftI, leftEndIndex}
						right := SubListDefinition{rightI, rightI + rightLength - 1}
						leftRights = append(leftRights, LeftRight{left, right})
					}
				}
			}
		}

		pivotValues := map[int]bool{}
		max, min := math.MinInt64, math.MaxInt64
		for _, v := range originalList {
			pivotValues[v] = true
			if v > max {
				max = v
			}
			if v < min {
				min = v
			}
		}
		pivotValues[max+1] = true
		pivotValues[min-1] = true

		for _, leftRight := range leftRights {
			left := leftRight.left
			right := leftRight.right
			for pivotValue := range pivotValues {
				for i := 0; i < shuffles; i++ {
					list := make([]int, len(originalList))
					copy(list, originalList)
					Shuffle(list)

					caseDescription := fmt.Sprintf("Pivot Value %v, left %v, right %v, list %v", pivotValue, left, right, list)
					leftOrRight, index := neutralise(list, left, 0, right, 0, pivotValue)
					caseDescription = fmt.Sprintf("%v, LorR %v, index %v, new list %v", caseDescription, leftOrRight, index, list)

					if leftOrRight == -1 {
						assert.True(t, isNeutralised(list[left.beginIndex:left.endIndex+1], pivotValue, -1), caseDescription)
						assert.True(t, isNeutralised(list[right.beginIndex:right.beginIndex+index], pivotValue, 1), caseDescription)
					} else if leftOrRight == 1 {
						assert.True(t, isNeutralised(list[right.beginIndex:right.endIndex+1], pivotValue, 1), caseDescription)
						assert.True(t, isNeutralised(list[left.beginIndex:left.beginIndex+index], pivotValue, -1), caseDescription)
					} else {
						assert.True(t, isNeutralised(list[left.beginIndex:left.endIndex+1], pivotValue, -1), caseDescription)
						assert.True(t, isNeutralised(list[right.beginIndex:right.endIndex+1], pivotValue, 1), caseDescription)
					}
				}
			}
		}
	}
}

func Shuffle(slice interface{}) {
	rv := reflect.ValueOf(slice)
	swap := reflect.Swapper(slice)
	length := rv.Len()
	for i := length - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		swap(i, j)
	}
}
