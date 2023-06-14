package utils

import (
	"bytes"
	"github.com/hardcore-os/corekv/utils/codec"
	"math/rand"
	"sync"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header   *Element
	rand     *rand.Rand
	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	headers := &Element{
		levels: make([]*Element, defaultMaxLevel),
	}
	return &SkipList{
		header:   headers,
		maxLevel: defaultMaxLevel - 1,
		rand:     r,
	}
}

type Element struct {
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	list.lock.Lock()
	defer list.lock.Unlock()

	key := data.Key
	score := list.calcScore(data.Key)
	prevElem := list.header
	prevElements := make([]*Element, defaultMaxLevel)

	for level := defaultMaxLevel - 1; level >= 0; level-- {
		for next := prevElem.levels[level]; next != nil; next = prevElem.levels[level] {
			if compare := list.compare(score, key, next); compare <= 0 {
				if compare == 0 {
					next.entry = data
					return nil
				} else {
					prevElem = next
				}
			} else {
				break
			}
		}
		prevElements[level] = prevElem
	}

	level := list.randLevel()
	element := newElement(score, data, level)
	for i := 0; i < level; i++ {
		element.levels[i] = prevElements[i].levels[i]
		prevElements[i].levels[i] = element
	}

	list.length++
	list.size = list.size + int64(level)

	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	list.lock.RLock()
	defer list.lock.RUnlock()

	if list.length == 0 {
		return nil
	}

	score := list.calcScore(key)
	prev, level := list.header, defaultMaxLevel-1

	for ; level >= 0; level-- {
		for next := prev.levels[level]; next != nil; next = prev.levels[level] {
			if compare := list.compare(score, key, next); compare <= 0 {
				if compare == 0 {
					return next.entry
				} else {
					prev = next
				}
			} else {
				break
			}
		}
	}
	return nil
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}

	if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) randLevel() int {
	for level := 1; level < defaultMaxLevel; level++ {
		if list.rand.Intn(2) == 0 {
			return level
		}
	}
	return defaultMaxLevel
}

func (list *SkipList) Size() int64 {
	return list.size
}
