package main

import (
	"math/rand"
	"strconv"
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

func BenchmarkPut(b *testing.B) {
	client, err := MakeClerk(strings.Split(ServerAddrs, ","))
	if err != nil {
		panic(err)
	}
	defer client.closeSession()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k := strconv.FormatInt(rand.Int63(), 10)
		v := strconv.FormatInt(rand.Int63(), 10)
		client.Put(k, v)
	}
}
