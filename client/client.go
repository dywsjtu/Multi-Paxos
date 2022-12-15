package main

import (
	"cos518/proj/kvpaxos"
	"flag"
	"fmt"
	"strconv"
)

func port(tag string, host int) string {
	s := "localhost:1000"
	s += strconv.Itoa(host)
	return s
}

func main() {
	n := flag.Int("n", 3, "# number of servers")
	flag.Parse()

	var kvh []string = make([]string, *n)
	for j := 0; j < *n; j++ {
		kvh[j] = port("basic", j)
	}
	ck := kvpaxos.MakeClerk(kvh)
	ck.Put("a", "b")
	fmt.Println(ck.Get("a"))
}
