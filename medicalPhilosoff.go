package main

import (
	"fmt"
	"math/rand"
	"time"
)

/**
The reason the system will not deadlock is due to the asymmetry in the picking up of forks.
With even indexed philosophers reaching for left to right forks and odd number reaching for right to left.
This makes it so we won't reach a situation where all philosophers have picked up a fork each.
*/

type Request struct {
	id int
	ch chan bool
}

func main() {
	numPhilosophers := 5

	forkRequests := make([]chan Request, numPhilosophers)
	forkReleases := make([]chan int, numPhilosophers)

	for i := 0; i < numPhilosophers; i++ {
		forkRequests[i] = make(chan Request)
		forkReleases[i] = make(chan int)
	}

	for i := 0; i < numPhilosophers; i++ {
		go Fork(i, forkRequests[i], forkReleases[i])
	}

	names := []string{"Plato", "Sun Tzu", "Socrates", "Voltaire", "René Descartes"}
	for i := 0; i < numPhilosophers; i++ {
		leftForkIdx := i
		rightForkIdx := (i + 1) % numPhilosophers

		if i%2 == 0 {
			go Philosophers(i, names[i],
				forkRequests[leftForkIdx], forkReleases[leftForkIdx],
				forkRequests[rightForkIdx], forkReleases[rightForkIdx])
		} else {
			go Philosophers(i, names[i],
				forkRequests[rightForkIdx], forkReleases[rightForkIdx],
				forkRequests[leftForkIdx], forkReleases[leftForkIdx])
		}
	}
	select {}
}

func Fork(id int, requests chan Request, releases chan int) {
	isFree := true
	for {
		if isFree {
			req := <-requests
			isFree = false
			req.ch <- true
		} else {
			<-releases
			isFree = true
		}
	}
}

func Philosophers(id int, name string,
	firstForkReq chan Request, firstForkRel chan int,
	secondForkReq chan Request, secondForkRel chan int) {
	eatCount := 0
	replyChan := make(chan bool)

	for {
		fmt.Printf("%s is thinking\n", name)
		thinkTime := time.Duration(rand.Intn(100)+50) * time.Millisecond
		time.Sleep(thinkTime)

		firstForkReq <- Request{id: id, ch: replyChan}
		<-replyChan

		secondForkReq <- Request{id: id, ch: replyChan}
		<-replyChan

		eatCount++
		fmt.Printf("%s is eating %d\n", name, eatCount)
		eatTime := time.Duration(rand.Intn(100)+50) * time.Millisecond
		time.Sleep(eatTime)

		secondForkRel <- id
		firstForkRel <- id
	}
}
