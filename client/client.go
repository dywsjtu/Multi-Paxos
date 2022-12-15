package main

import (
	"cos518/proj/kvpaxos"
	"flag"
	"log"
	"strconv"
	"time"
)

func port(tag string, host int) string {
	s := "localhost:1000"
	s += strconv.Itoa(host)
	return s
}

func timer() {
	for {
		log.Printf("+++++++++++++++++++++++++++++")
		time.Sleep(1 * time.Second)
	}
}

func main() {
	n := flag.Int("n", 3, "# number of servers")
	flag.Parse()

	var kvh []string = make([]string, *n)
	for j := 0; j < *n; j++ {
		kvh[j] = port("basic", j)
	}
	go timer()
	ck := kvpaxos.MakeClerk(kvh)
	for {
		start := time.Now().UnixNano() / int64(time.Millisecond)
		ck.Put("a", "b")
		end := time.Now().UnixNano() / int64(time.Millisecond)
		diff := end - start
		log.Printf("Duration(ms): %d", diff)
		// fmt.Println(ck.Get("a"))
	}
}
