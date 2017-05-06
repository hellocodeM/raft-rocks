package main

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkGet(b *testing.B) {
	b.RunParallel(func(sub *testing.PB) {
		b.StopTimer()
		client, err := MakeClerk(strings.Split(ServerAddrs, ","))
		if err != nil {
			panic(err)
		}
		defer client.closeSession()

		b.StartTimer()
		for sub.Next() {
			k := strconv.FormatInt(rand.Int63(), 10)
			client.Get(k)
		}
	})
}

func BenchmarkPut(b *testing.B) {
	b.RunParallel(func(sub *testing.PB) {
		b.StopTimer()
		client, err := MakeClerk(strings.Split(ServerAddrs, ","))
		if err != nil {
			panic(err)
		}
		defer client.closeSession()

		b.StartTimer()
		for sub.Next() {
			k := strconv.FormatInt(rand.Int63(), 10)
			v := strconv.FormatInt(rand.Int63(), 10)
			client.Put(k, v)
		}

	})
}
