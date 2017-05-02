package raft

func maxInt(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func minInt(x int, y int) int {
	if x < y {
		return x
	}
	return y
}
