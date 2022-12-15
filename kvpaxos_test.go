package kvpaxos

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func check(t *testing.T, ck *Clerk, key string, value string) {
	v := ck.Get(key)
	if v != value {
		t.Fatalf("Get(%v) -> %v, expected %v", key, v, value)
	}
}

func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "kv-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

// func port(tag string, host int) string {
// 	s := "localhost:1000"
// 	s += strconv.Itoa(host)
// 	return s
// }

func cleanup(kva []*KVPaxos) {
	for i := 0; i < len(kva); i++ {
		if kva[i] != nil {
			kva[i].kill()
		}
	}
}

// predict effect of Append(k, val) if old value is prev.
func NextValue(prev string, val string) string {
	return prev + val
}

func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(6)

	const nservers = 3
	var kva []*KVPaxos = make([]*KVPaxos, nservers)
	var kvh []string = make([]string, nservers)
	defer cleanup(kva)

	for i := 0; i < nservers; i++ {
		kvh[i] = port("basic", i)
	}
	for i := 0; i < nservers; i++ {
		kva[i] = StartServer(kvh, i)
	}

	var cka [nservers]*Clerk
	for i := 0; i < nservers; i++ {
		cka[i] = MakeClerk([]string{kvh[i]})
	}

	for iters := 0; iters < 1; iters++ {
		const npara = 10000
		var ca [npara]chan bool
		for nth := 0; nth < npara; nth++ {
			ca[nth] = make(chan bool)
			go func(me int) {
				defer func() { ca[me] <- true }()
				ci := (nth % nservers)
				myck := MakeClerk([]string{kvh[ci]})
				myck.Put("b", strconv.Itoa(rand.Int()))
			}(nth)
		}
		for nth := 0; nth < npara; nth++ {
			<-ca[nth]
		}
		// var va [nservers]string
		// for i := 0; i < nservers; i++ {
		// 	va[i] = cka[i].Get("b")
		// 	// fmt.Printf("%v, %v, %v\n", va[0], va[i], i)
		// 	if va[i] != va[0] {
		// 		t.Fatalf("mismatch")
		// 	}
		// }
	}

	fmt.Printf("  ... Passed\n")

	time.Sleep(1 * time.Second)
}

func pp(tag string, src int, dst int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	s += "kv-" + tag + "-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += strconv.Itoa(src) + "-"
	s += strconv.Itoa(dst)
	return s
}

func cleanpp(tag string, n int) {
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			ij := pp(tag, i, j)
			os.Remove(ij)
		}
	}
}


func randclerk(kvh []string) *Clerk {
	sa := make([]string, len(kvh))
	copy(sa, kvh)
	for i := range sa {
		j := rand.Intn(i + 1)
		sa[i], sa[j] = sa[j], sa[i]
	}
	return MakeClerk(sa)
}
