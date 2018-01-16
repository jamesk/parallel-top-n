package main

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func generateList(n int) []int {
	list := make([]int, n)
	for i := 0; i < n; i++ {
		list[i] = rand.Int()
	}
	return list
}

func Test_selectTopFaA_random(t *testing.T) {
	n := 10 * 1000
	b := 100
	top := 100

	list := generateList(n)

	k := selectTopFaA(list, top, b)

	assert.Equal(t, top, k)

	actual := map[int]int{}
	for i := 0; i < top; i++ {
		v := list[i]
		count := actual[v]
		actual[v] = count + 1
	}

	sort.Ints(list)
	expected := map[int]int{}
	for i := 0; i < top; i++ {
		v := list[i]
		count := expected[v]
		expected[v] = count + 1
	}

	assert.Equal(t, expected, actual)
}

func Test_partitionParallel_allthesame(t *testing.T) {
	list := []int{2, 4, 7, 3, 1, 9, 2, 2, 5, 2, 4}
	pivotIndex := partitionParallel(list, 0, len(list)-1, 2, 2)

	assert.Equal(t, []int{1, 4, 7, 3, 2, 9, 2, 2, 5, 2, 4}, list)
	assert.Equal(t, 1, pivotIndex)
}

func Test_partitionParallel_remainder(t *testing.T) {
	list := []int{1, 4, 7, 3, 2, 9, 10, 8, 5, 6, 4}
	pivotIndex := partitionParallel(list, 0, len(list)-1, 2, 8)

	assert.Equal(t, []int{1, 4, 7, 3, 2, 4, 5, 6, 10, 8, 9}, list)
	assert.Equal(t, 8, pivotIndex)
}

func Test_partitionParallel(t *testing.T) {
	list := []int{1, 4, 7, 3, 2, 9, 10, 8, 5, 6}
	pivotIndex := partitionParallel(list, 0, len(list)-1, 2, 8)

	assert.Equal(t, []int{1, 4, 7, 3, 2, 5, 6, 8, 9, 10}, list)
	assert.Equal(t, 7, pivotIndex)
}

func Test_selectTopFaA_duplicates(t *testing.T) {
	list := []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	i := selectTopFaA(list, 5, 1)

	assert.Equal(t, 5, i)
}

func Test_selectTopFaA_different(t *testing.T) {
	//	list := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	list := []int{1, 4, 7, 3, 2, 9, 10, 8, 5, 6}
	i := selectTopFaA(list, 5, 1)

	fmt.Println(i)
	fmt.Println(list)
	sum := 0
	for i := 0; i < 5; i++ {
		sum += list[i]
	}
	//First 5 numbers are 1, 2, 3, 4, 5
	assert.Equal(t, 15, sum)
}

func Test_partition(t *testing.T) {
	list := []int{1, 3, 2, 4, 7, 9, 10, 8, 5, 6}
	left := 4
	right := 9

	pivotIndex := partition(list, left, right, 8)

	assert.Equal(t, []int{1, 3, 2, 4, 7, 5, 6, 8, 9, 10}, list)
	assert.Equal(t, 7, pivotIndex)
}

func TestTakeLeftRight(t *testing.T) {
	//Don't use zeros in the test list, tests assume 0 is an unset value in output
	lists := [][]int{[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}
	leftRightCombos := []struct {
		left, right int
	}{
		{0, 9},
	}
	cases := []struct {
		takePattern []int
	}{
		{
			[]int{1},
		},
		{
			[]int{-1},
		},
		{
			[]int{-1, 1},
		},
		{
			[]int{-1, 1, -1},
		},
		{
			[]int{-1, 1, -1, -1},
		},
		{
			[]int{1, -1},
		},
		{
			[]int{1, -1, 1},
		},
		{
			[]int{1, -1, 1, 1},
		},
		{
			[]int{1, -1, -1, 1},
		},
		{
			[]int{-1, 1, 1, -1},
		},
	}

	for _, c := range cases {
		for listI, list := range lists {
			for _, leftRight := range leftRightCombos {
				left := leftRight.left
				right := leftRight.right

				for b := 1; b <= len(list); b++ {
					t.Run(fmt.Sprintf("Running case %v with block size of %v with left %v and right %v for list at index %v", c.takePattern, b, left, right, listI), func(t *testing.T) {
						s := NewLeftRightSubLists(list, left, right, b)

						output := make([]int, len(list))
						bLeft := s.TakeNextLeft()
						bRight := s.TakeNextRight()
						for bLeft != nil || bRight != nil {
							if bLeft != nil {
								for i := bLeft.beginIndex; i <= bLeft.endIndex; i++ {
									if output[i] != 0 {
										t.Errorf("Output at index %v has already been set! On left %v", i, bLeft)
									}
									output[i] = list[i]
								}
							}
							if bRight != nil {
								for i := bRight.beginIndex; i <= bRight.endIndex; i++ {
									if output[i] != 0 {
										t.Errorf("Output at index %v has already been set! On right %v", i, bRight)
									}
									output[i] = list[i]
								}
							}

							bLeft = s.TakeNextLeft()
							bRight = s.TakeNextRight()
						}

						assert.Equal(t, list, output, "Output list did not match input list")
					})
				}
			}
		}
	}
}
