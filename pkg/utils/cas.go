package utils

import "sync/atomic"

func CASUpdateU32Min(min *atomic.Uint32, newVal uint32) {
	for {
		curr := min.Load()
		if newVal >= curr {
			return
		}

		if min.CompareAndSwap(curr, newVal) {
			return
		}
	}
}

func CASUpdateU32Max(max *atomic.Uint32, newVal uint32) {
	for {
		curr := max.Load()
		if newVal <= curr {
			return
		}

		if max.CompareAndSwap(curr, newVal) {
			return
		}
	}
}
