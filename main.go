package main

import (
	"fmt"
	"math/rand"
	"time"
)

/*
*
 */
func main() {
	fork1 := make(chan int, 1)
	fork2 := make(chan int, 1)
	fork3 := make(chan int, 1)
	fork4 := make(chan int, 1)
	fork5 := make(chan int, 1)

	go fork(fork1)
	go fork(fork2)
	go fork(fork3)
	go fork(fork4)
	go fork(fork5)

	go Philosopher(fork1, fork5, "Plato, ")
	go Philosopher(fork1, fork2, "Sun Tzu")
	go Philosopher(fork2, fork3, "Sokrates")
	go Philosopher(fork3, fork4, "Voltaire")
	go Philosopher(fork4, fork5, "René Descartes")
	for {
	}
}

func fork(me chan int) {
	me <- 1
}

func Philosopher(forkleft chan int, forkright chan int, name string) {
	eatCount := 0
	for {
		//
		if TryEat(forkleft, forkright) {
			<-forkleft
			<-forkright
			eatCount++
			time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
			fmt.Println("I, the one and only: ", name, " have eaten ", eatCount, " times")
			forkright <- 1
			forkleft <- 1
			fmt.Println(name, " is thinking")
			time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
		} else {
			//If a philosopher cannot pick up a total of two forks, they will instead think and then try again.
			//In this way, there will never be a scenario where every philosopher has 1 fork each.
			//Additionally to avoid the same philosophers eating again and again, they will stop
			//to think after eating giving the others a chance to grab the forks
			fmt.Println(name, " is thinking")
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		}
	}
}

func TryEat(forkleft chan int, forkright chan int) bool {
	if len(forkleft) == 1 && len(forkright) == 1 {
		return true
	}
	return false
}
