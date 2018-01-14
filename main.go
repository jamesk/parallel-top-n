package main

import (
	"fmt"
	"math/rand"
)

type LeftRightSubLists struct {
	list            []int
	blockSize       int
	totalBlocks     int
	leftBlockIndex  int
	rightBlockIndex int
}

func NewLeftRightSubLists(list []int, blockSize int) *LeftRightSubLists {
	if len(list) == 0 {
		return &LeftRightSubLists{
			list, blockSize, 0, -1, -1,
		}
	}

	totalBlocks := len(list) / blockSize
	if len(list)%blockSize > 0 {
		totalBlocks++
	}

	return &LeftRightSubLists{
		list, blockSize, totalBlocks, 0, totalBlocks - 1,
	}
}

type SubListDefinition struct {
	beginIndex int
	endIndex   int
}

// TakeNextLeft Get the next left most block that is available
func (s *LeftRightSubLists) TakeNextLeft() *SubListDefinition {
	if s.leftBlockIndex == -1 {
		return nil
	}

	left := s.leftBlockIndex * s.blockSize
	right := left + s.blockSize - 1 //Right is inclusive so -1
	if right >= len(s.list) {
		right = len(s.list) - 1
	}
	d := SubListDefinition{left, right}

	//Update
	s.leftBlockIndex++

	blocksInRight := s.totalBlocks - s.rightBlockIndex
	blocksInLeft := s.leftBlockIndex
	//Greater than, they can share the same block until someone claims it
	if blocksInLeft+blocksInRight > s.totalBlocks {
		s.leftBlockIndex = -1
		s.rightBlockIndex = -1
	}

	return &d
}

// TakeNextRight Get the next right most block that is available
func (s *LeftRightSubLists) TakeNextRight() *SubListDefinition {
	if s.rightBlockIndex == -1 {
		return nil
	}

	left := s.rightBlockIndex * s.blockSize
	right := left + s.blockSize - 1 //Right is inclusive so -1
	if right >= len(s.list) {
		right = len(s.list) - 1
	}
	d := SubListDefinition{left, right}

	//Update
	s.rightBlockIndex--

	blocksInRight := s.totalBlocks - s.rightBlockIndex
	blocksInLeft := s.leftBlockIndex
	//Greater than, they can share the same block until someone claims it
	if blocksInLeft+blocksInRight > s.totalBlocks {
		s.leftBlockIndex = -1
		s.rightBlockIndex = -1
	}

	return &d
}

// selectTopFaA select the top X elements of the list (inclusive)
func selectTopFaA(list []int, top int) int {
	left := 0
	right := len(list) - 1
	for {
		if left == right {
			return left
		}

		pivotIndex := left + rand.Intn(right-left+1)
		pivotValue := list[pivotIndex]
		fmt.Printf("before partition L: %v, R: %v, pivot index of %v, list %v\n", left, right, pivotIndex, list)
		pivotIndex = partition(list, SubListDefinition{left, right - 1}, SubListDefinition{right, right}, pivotValue)
		fmt.Printf("after partition L: %v, R: %v, pivot index of %v, list %v\n", left, right, pivotIndex, list)

		if top == pivotIndex {
			return pivotIndex
		} else if top > pivotIndex {
			for list[pivotIndex] == pivotValue {
				if top == pivotIndex {
					return pivotIndex
				}
				pivotIndex++
			}
			left = pivotIndex
		} else if top < pivotIndex {
			for list[pivotIndex] == pivotValue {
				if top == pivotIndex {
					return pivotIndex
				}
				pivotIndex--
			}
			right = pivotIndex
		}
	}

}

// left and right must be disjoint
func partition(list []int, left SubListDefinition, right SubListDefinition, pivotValue int) int {
	subLength := (left.endIndex - left.beginIndex + 1) + (right.endIndex - right.beginIndex + 1)
	storeIndex := 0

	for i := 0; i < subLength; i++ {
		actualI := mapIndexLR(i, left, right)
		if list[actualI] < pivotValue {
			actualStoreIndex := mapIndexLR(storeIndex, left, right)
			list[actualI], list[actualStoreIndex] = list[actualStoreIndex], list[actualI]
			storeIndex++
		}
	}

	return mapIndexLR(storeIndex, left, right)
}

/*
// selectTopFaA select the top X elements of the list (inclusive)
func selectTopFaA(list []int, top int) int {
	left := 0
	right := len(list) - 1
	for {
		if left == right {
			return left
		}

		pivotIndex := left + rand.Intn(right-left+1)
		pivotValue := list[pivotIndex]
		fmt.Printf("before partition L: %v, R: %v, pivot index of %v, list %v\n", left, right, pivotIndex, list)
		pivotIndex = partition(list, SubListDefinition{left, right - 1}, SubListDefinition{right, right}, pivotValue)
		fmt.Printf("after partition L: %v, R: %v, pivot index of %v, list %v\n", left, right, pivotIndex, list)

		if top == pivotIndex {
			return pivotIndex
		} else if top > pivotIndex {
			for list[pivotIndex] == pivotValue {
				if top == pivotIndex {
					return pivotIndex
				}
				pivotIndex++
			}
			left = pivotIndex
		} else if top < pivotIndex {
			for list[pivotIndex] == pivotValue {
				if top == pivotIndex {
					return pivotIndex
				}
				pivotIndex--
			}
			right = pivotIndex
		}
	}

}

// left and right must be disjoint
func partition(list []int, left SubListDefinition, right SubListDefinition, pivotValue int) int {
	subLength := (left.endIndex - left.beginIndex + 1) + (right.endIndex - right.beginIndex + 1)
	storeIndex := 0

	for i := 0; i < subLength; i++ {
		actualI := mapIndexLR(i, left, right)
		if list[actualI] < pivotValue {
			actualStoreIndex := mapIndexLR(storeIndex, left, right)
			list[actualI], list[actualStoreIndex] = list[actualStoreIndex], list[actualI]
			storeIndex++
		}
	}

	return mapIndexLR(storeIndex, left, right)
}
*/
func mapIndexLR(i int, left SubListDefinition, right SubListDefinition) int {
	actualI := left.beginIndex + i
	if actualI > left.endIndex {
		leftLength := left.endIndex - left.beginIndex + 1
		actualI = right.beginIndex + (i - leftLength)
	}

	return actualI
}

func mapLength(length, p, b, pI int) int {
	blocks := length / b
	lastBlockLength := length % b

	//Get minimum length of piece list
	pLength := (blocks / p) * b
	//Add extra block for pieces that get a whole extra
	if blocks%p > pI {
		pLength += b
	}
	//For the last p add the remaining partial block
	if blocks%p == pI {
		pLength += lastBlockLength
	}

	return pLength
}

func mapIndex(p, b, pI, i int) int {
	//Distance (in the overall list) between each block this sublist owns
	blocksDistance := (p * b)
	//which block of this sublist are we on
	block := i / b
	//the index in the overall list that this sublist starts at
	startingIndex := (pI * b)
	//The index (overall) that this block starts at
	blockStartIndex := startingIndex + (block * blocksDistance)
	//The offset into the block that we are at
	offset := i % b

	return blockStartIndex + offset
}
