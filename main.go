package main

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
)

type LeftRightSubLists struct {
	list            []int
	left, right     int
	length          int
	blockSize       int
	totalBlocks     int
	leftBlockIndex  int
	rightBlockIndex int
	mutex           *sync.Mutex
}

func NewLeftRightSubLists(list []int, left int, right int, blockSize int) *LeftRightSubLists {
	if len(list) == 0 {
		return &LeftRightSubLists{
			list, 0, 0, 0, blockSize, 0, -1, -1, &sync.Mutex{},
		}
	}

	length := right - left + 1

	totalBlocks := length / blockSize
	if length%blockSize > 0 {
		totalBlocks++
	}

	return &LeftRightSubLists{
		list, left, right, length, blockSize, totalBlocks, 0, totalBlocks - 1, &sync.Mutex{},
	}
}

type SubListDefinition struct {
	beginIndex int
	endIndex   int
}

func (s *SubListDefinition) String() string {
	return fmt.Sprintf("SL %v - %v", s.beginIndex, s.endIndex)
}

// TakeNextLeft Get the next left most block that is available
func (s *LeftRightSubLists) TakeNextLeft() *SubListDefinition {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	rightBlocksClaimed := s.totalBlocks - (s.rightBlockIndex + 1)
	leftBlocksClaimed := s.leftBlockIndex
	//Greater than (not or equal), they can share the same block until someone claims it
	if leftBlocksClaimed+rightBlocksClaimed >= s.totalBlocks {
		return nil
	}

	left := s.left + s.leftBlockIndex*s.blockSize
	right := left + s.blockSize - 1 //Right is inclusive so -1
	if right >= s.length {
		right = s.left + s.length - 1
	}
	d := SubListDefinition{left, right}

	//Update
	s.leftBlockIndex++

	return &d
}

// TakeNextRight Get the next right most block that is available
func (s *LeftRightSubLists) TakeNextRight() *SubListDefinition {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	rightBlocksClaimed := s.totalBlocks - (s.rightBlockIndex + 1)
	leftBlocksClaimed := s.leftBlockIndex
	//Greater than (not or equal), they can share the same block until someone claims it
	if leftBlocksClaimed+rightBlocksClaimed >= s.totalBlocks {
		return nil
	}

	left := s.left + s.rightBlockIndex*s.blockSize
	right := left + s.blockSize - 1 //Right is inclusive so -1
	if right >= s.length {
		right = s.left + s.length - 1
	}
	d := SubListDefinition{left, right}

	//Update
	s.rightBlockIndex--

	return &d
}

// selectTopFaA select the top X elements of the list (inclusive)
func selectTopFaA(list []int, top int, blockSize int) int {
	left := 0
	right := len(list) - 1
	for {
		if left == right {
			return left
		}

		pivotIndex := left + rand.Intn(right-left+1)
		pivotValue := list[pivotIndex]

		//Shared mutable
		s := NewLeftRightSubLists(list, left, right, blockSize)
		remainingLeftBlocks := []*SubListDefinition{}
		neutralisedLeftBlocks := []*SubListDefinition{}
		remainingRightBlocks := []*SubListDefinition{}
		neutralisedRightBlocks := []*SubListDefinition{}

		//Start of "parallel" code
		leftBlock := s.TakeNextLeft()
		rightBlock := s.TakeNextRight()
		i := 0
		j := 0
		for leftBlock != nil && rightBlock != nil {
			fmt.Printf("before partition L: %v, R: %v, pivot index of %v, list %v\n", left, right, pivotIndex, list)
			leftOrRight, index := neutralise(list, *leftBlock, i, *rightBlock, j, pivotValue)
			fmt.Printf("after partition L: %v, R: %v, pivot index of %v, list %v\n", left, right, pivotIndex, list)

			if leftOrRight > 0 {
				//right block, all greater than or equal to pivot (neutralised), get another
				neutralisedRightBlocks = append(neutralisedRightBlocks, rightBlock)
				rightBlock = s.TakeNextRight()
				j = 0
				i = index
			}
			if leftOrRight < 0 {
				//left block, all less than pivot (neutralised), get another
				neutralisedLeftBlocks = append(neutralisedLeftBlocks, leftBlock)
				leftBlock = s.TakeNextLeft()
				j = index
				i = 0
			}
			if leftOrRight == 0 {
				//both neutralised
				neutralisedLeftBlocks = append(neutralisedLeftBlocks, leftBlock)
				neutralisedRightBlocks = append(neutralisedRightBlocks, rightBlock)
				rightBlock = s.TakeNextRight()
				leftBlock = s.TakeNextLeft()
				j = 0
				i = 0
			}
		}
		if leftBlock != nil {
			remainingLeftBlocks = append(remainingLeftBlocks, leftBlock)
		} else if rightBlock != nil {
			remainingRightBlocks = append(remainingRightBlocks, rightBlock)
		}

		//Sequential copy of unneutralised blocks into middle
		sort.Slice(remainingLeftBlocks, func(i, j int) bool {
			return remainingLeftBlocks[i].beginIndex < remainingLeftBlocks[j].beginIndex
		})
		sort.Slice(neutralisedLeftBlocks, func(i, j int) bool {
			return neutralisedLeftBlocks[i].beginIndex > neutralisedLeftBlocks[j].beginIndex
		})
		swapBlock := func(a *SubListDefinition, b *SubListDefinition) {
			aSlice := list[a.beginIndex : a.endIndex+1]
			bSlice := list[b.beginIndex : b.endIndex+1]

			temp := make([]int, len(aSlice))
			copy(temp, aSlice)
			copy(aSlice, bSlice)
			copy(bSlice, temp)
		}

		nI := 0
		for _, s := range remainingLeftBlocks {
			if nI >= len(neutralisedLeftBlocks) {
				break
			}

			swapBlock(s, neutralisedLeftBlocks[nI])
			nI++
		}
		{
			sort.Slice(remainingRightBlocks, func(i, j int) bool {
				return remainingRightBlocks[i].beginIndex > remainingRightBlocks[j].beginIndex
			})
			sort.Slice(neutralisedRightBlocks, func(i, j int) bool {
				return neutralisedRightBlocks[i].beginIndex < neutralisedRightBlocks[j].beginIndex
			})

			nI := 0
			for sI := 0; sI < len(remainingRightBlocks); {
				if nI >= len(neutralisedRightBlocks) {
					break
				}
				s := remainingRightBlocks[sI]
				n := neutralisedRightBlocks[nI]

				uLen := s.endIndex - s.beginIndex + 1
				nLen := n.endIndex - n.beginIndex + 1

				if uLen == nLen {
					swapBlock(s, neutralisedRightBlocks[nI])
					nI++
					sI++
				} else if uLen < nLen {
					swapBlock(s, neutralisedRightBlocks[nI]) //rely on copy behaviour, will do at most min(uLen, nLen)
					neutralisedRightBlocks[nI].beginIndex += uLen
					sI++
				} else if uLen > nLen {
					partialS := &SubListDefinition{s.endIndex + 1 - nLen, s.endIndex}
					swapBlock(partialS, n)
					s.endIndex = partialS.beginIndex - 1
					nI++
				}
			}
		}
		//Check and re-loop
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

func partitionParallel(list []int, left, right int, blockSize int, pivotValue int) int {
	fmt.Printf("pp, left %v right %v blockSize %v, value %v, list %v\n", left, right, blockSize, pivotValue, list)

	//Shared mutable
	s := NewLeftRightSubLists(list, left, right, blockSize)
	if s.length <= blockSize {
		//Shortcut if the list is equal to or smaller than blocksize
		return partition(list, left, right, pivotValue)
	}

	remainingLeftBlocks := []*SubListDefinition{}
	neutralisedLeftBlocks := []*SubListDefinition{}
	remainingRightBlocks := []*SubListDefinition{}
	neutralisedRightBlocks := []*SubListDefinition{}

	//Start of "parallel" code
	leftBlock := s.TakeNextLeft()
	rightBlock := s.TakeNextRight()
	i := 0
	j := 0
	for leftBlock != nil && rightBlock != nil {
		fmt.Printf("before neutralise left %v, right %v, i %v, j %v, remaining left %v, right %v, neutralised left %v, right %v, list %v\n", leftBlock, rightBlock, i, j, remainingLeftBlocks, remainingRightBlocks, neutralisedLeftBlocks, neutralisedRightBlocks, list)
		leftOrRight, index := neutralise(list, *leftBlock, i, *rightBlock, j, pivotValue)
		fmt.Printf("after neutralise leftOrRight %v, index %v, list %v\n", leftOrRight, index, list)

		if leftOrRight > 0 {
			//right block, all greater than or equal to pivot (neutralised), get another
			neutralisedRightBlocks = append(neutralisedRightBlocks, rightBlock)
			rightBlock = s.TakeNextRight()
			j = 0
			i = index
		}
		if leftOrRight < 0 {
			//left block, all less than pivot (neutralised), get another
			neutralisedLeftBlocks = append(neutralisedLeftBlocks, leftBlock)
			leftBlock = s.TakeNextLeft()
			j = index
			i = 0
		}
		if leftOrRight == 0 {
			//both neutralised
			neutralisedLeftBlocks = append(neutralisedLeftBlocks, leftBlock)
			neutralisedRightBlocks = append(neutralisedRightBlocks, rightBlock)
			rightBlock = s.TakeNextRight()
			leftBlock = s.TakeNextLeft()
			j = 0
			i = 0
		}
	}
	if leftBlock != nil {
		remainingLeftBlocks = append(remainingLeftBlocks, leftBlock)
	} else if rightBlock != nil {
		remainingRightBlocks = append(remainingRightBlocks, rightBlock)
	}

	fmt.Printf("Loop done left %v, right %v, i %v, j %v, remaining left %v, right %v, neutralised left %v, right %v, list %v\n", leftBlock, rightBlock, i, j, remainingLeftBlocks, remainingRightBlocks, neutralisedLeftBlocks, neutralisedRightBlocks, list)

	//Sequential copy of unneutralised blocks into middle
	sort.Slice(remainingLeftBlocks, func(i, j int) bool {
		return remainingLeftBlocks[i].beginIndex < remainingLeftBlocks[j].beginIndex
	})
	sort.Slice(neutralisedLeftBlocks, func(i, j int) bool {
		return neutralisedLeftBlocks[i].beginIndex > neutralisedLeftBlocks[j].beginIndex
	})
	swapBlock := func(a *SubListDefinition, b *SubListDefinition) {
		aSlice := list[a.beginIndex : a.endIndex+1]
		bSlice := list[b.beginIndex : b.endIndex+1]

		temp := make([]int, len(aSlice))
		copy(temp, aSlice)
		copy(aSlice, bSlice)
		copy(bSlice, temp)
	}

	//TODO: assuming the left blocks are all of blockSize
	nI := 0
	sI := 0
	newLeft := left
	if len(neutralisedLeftBlocks) > 0 {
		newLeft = neutralisedLeftBlocks[0].endIndex + 1
	}
	for sI < len(remainingLeftBlocks) {
		if nI >= len(neutralisedLeftBlocks) {
			break
		}
		s := remainingLeftBlocks[sI]
		n := neutralisedLeftBlocks[nI]

		if s.beginIndex > n.beginIndex {
			break //got all the neutralised blocks to the left
		}

		swapBlock(s, n)
		newLeft = s.endIndex + 1
		nI++
		sI++
	}

	sort.Slice(remainingRightBlocks, func(i, j int) bool {
		return remainingRightBlocks[i].beginIndex > remainingRightBlocks[j].beginIndex
	})
	sort.Slice(neutralisedRightBlocks, func(i, j int) bool {
		return neutralisedRightBlocks[i].beginIndex < neutralisedRightBlocks[j].beginIndex
	})
	newRight := right
	if len(neutralisedRightBlocks) > 0 {
		newRight = neutralisedRightBlocks[0].beginIndex - 1
	}
	{
		nI := 0
		sI := 0
		for sI < len(remainingRightBlocks) {
			if nI >= len(neutralisedRightBlocks) {
				break
			}
			s := remainingRightBlocks[sI]
			n := neutralisedRightBlocks[nI]

			if s.beginIndex < n.beginIndex {
				break //got all the neutralised blocks to the right
			}

			uLen := s.endIndex - s.beginIndex + 1
			nLen := n.endIndex - n.beginIndex + 1

			if uLen == nLen {
				swapBlock(s, n)
				nI++
				sI++
				newRight = s.beginIndex - 1
			} else if uLen < nLen {
				swapBlock(s, n) //rely on copy behaviour, will do at most min(uLen, nLen)
				n.beginIndex += uLen
				newRight = s.beginIndex - 1
				sI++
			} else if uLen > nLen {
				partialS := &SubListDefinition{s.endIndex + 1 - nLen, s.endIndex}
				swapBlock(partialS, n)
				s.endIndex = partialS.beginIndex - 1
				newRight = partialS.beginIndex
				nI++
			}
		}
	}

	if newLeft >= newRight {
		return newLeft
	}

	return partitionParallel(list, newLeft, newRight, blockSize, pivotValue)
}

/* sequential, part of the way to parallel
// selectTopFaA select the top X elements of the list (inclusive)
func selectTopFaA(list []int, top int, blockSize int) int {
	left := 0
	right := len(list) - 1
	for {
		if left == right {
			return left
		}

		pivotIndex := left + rand.Intn(right-left+1)
		pivotValue := list[pivotIndex]

		s := NewLeftRightSubLists(list, left, right, blockSize)

		leftBlock := s.TakeNextLeft()
		rightBlock := s.TakeNextRight()
		lastRightBegin := rightBlock.beginIndex //TODO: assumes there are at least 2 blocks
		i := 0
		j := 0
		for leftBlock != nil && rightBlock != nil {
			lastRightBegin = rightBlock.beginIndex

			fmt.Printf("before partition L: %v, R: %v, pivot index of %v, list %v\n", left, right, pivotIndex, list)
			leftOrRight, index := neutralise(list, *leftBlock, i, *rightBlock, j, pivotValue)
			fmt.Printf("after partition L: %v, R: %v, pivot index of %v, list %v\n", left, right, pivotIndex, list)

			if leftOrRight > 0 {
				//right block, all greater than or equal to pivot (neutralised), get another
				rightBlock = s.TakeNextRight()
				j = 0
				i = index
			}
			if leftOrRight < 0 {
				//left block, all less than pivot (neutralised), get another
				leftBlock = s.TakeNextLeft()
				j = index
				i = 0
			}
			if leftOrRight == 0 {
				//both neutralised
				rightBlock = s.TakeNextRight()
				leftBlock = s.TakeNextLeft()
				j = 0
				i = 0
			}
		}

		//Sequential partition on unneutralised block
		if leftBlock != nil {
			pivotIndex = partition(list, leftBlock.beginIndex, leftBlock.endIndex, pivotValue)
		} else if rightBlock != nil {
			pivotIndex = partition(list, rightBlock.beginIndex, rightBlock.endIndex, pivotValue)
		} else {
			//both nil, pivot index is between them
			pivotIndex = lastRightBegin
		}

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
*/

func partition(list []int, left int, right, pivotValue int) int {
	storeIndex := left
	for i := left; i <= right; i++ {
		if list[i] < pivotValue {
			list[i], list[storeIndex] = list[storeIndex], list[i]
			storeIndex++
		}
	}

	return storeIndex
}

// left and right must be disjoint
func neutralise(list []int, left SubListDefinition, i int, right SubListDefinition, j int, pivotValue int) (leftOrRight int, index int) {
	leftLength := left.endIndex - left.beginIndex + 1
	rightLength := right.endIndex - right.beginIndex + 1

	for i < leftLength && j < rightLength {
		for ; i < leftLength; i++ {
			actualI := left.beginIndex + i
			if list[actualI] >= pivotValue {
				break
			}
		}

		for ; j < rightLength; j++ {
			actualJ := right.beginIndex + j
			if list[actualJ] < pivotValue {
				break
			}
		}

		if i == leftLength || j == rightLength {
			break
		}

		actualI := left.beginIndex + i
		actualJ := right.beginIndex + j
		list[actualI], list[actualJ] = list[actualJ], list[actualI]
		i++
		j++
	}

	if i == leftLength && j == rightLength {
		return 0, -1
	}
	if i == leftLength {
		return -1, j //left is neutralised
	}

	return 1, i //right is neutralised
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
