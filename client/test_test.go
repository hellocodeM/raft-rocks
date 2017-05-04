package main

import (
	"strings"
	"testing"
)

func BenchmarkGet(b *testing.B) {
	client, err := MakeClerk(strings.Split(ServerAddrs, ","))
	if err != nil {
		panic(err)
	}
	defer client.closeSession()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.Get("1")
	}
}
