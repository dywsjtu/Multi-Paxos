package main

import (
	"cos518/proj/kvpaxos"
	// "math/rand"
	"flag"
	"log"
	"strconv"
	"time"
)

func port(tag string, host int) string {
	s := "10.0.0."
	s += strconv.Itoa(host+2)
	s += ":8888"
	return s
}

func timer() {
	for {
		log.Printf("+++++++++++++++++++++++++++++")
		time.Sleep(10 * time.Second)
	}
}

func run_client(ck *kvpaxos.Clerk) {
	for {
		start := 10 * time.Now().UnixNano() / int64(time.Millisecond)
		ck.Put("a", "b")
		time.Sleep(20*time.Millisecond)
		end := 10 * time.Now().UnixNano() / int64(time.Millisecond)
		diff := end - start
		log.Printf("Duration(ms): %d", diff)
		
	}
}

func main() {
	n := flag.Int("n", 3, "# number of servers")
	c := flag.Int("c", 1, "# number of clients")
	flag.Parse()

	var kvh []string = make([]string, *n)
	for j := 0; j < *n; j++ {
		kvh[j] = port("basic", j)
	}
	go timer()
	var cks []*kvpaxos.Clerk = make([]*kvpaxos.Clerk, *c)
	for i := 0; i < *c; i++ {
		ci := (i % *n)
		cks[i] = kvpaxos.MakeClerk([]string{kvh[ci]})
		// cks[i] = kvpaxos.MakeClerk(kvh)
		go run_client(cks[i])
	}
	time.Sleep(600 * time.Second)
}
