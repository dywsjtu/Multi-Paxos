package main

import (
	"cos518/proj/kvpaxos"
	"flag"
	"strconv"
)

func port(tag string, host int) string {
	s := "localhost:1000"
	s += strconv.Itoa(host)
	return s
}

func main() {
	i := flag.Int("i", 0, "# idx of this server")
	n := flag.Int("n", 3, "# number of servers")
	flag.Parse()

	var kvh []string = make([]string, *n)
	for j := 0; j < *n; j++ {
		kvh[j] = port("basic", j)
	}
	kvpaxos.StartServer(kvh, *i)
	for {
	}
}
