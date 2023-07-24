package batch_deal

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var wg sync.WaitGroup

type MyBatch struct {
	BaseBatch
	results chan<- int
}

func (b *MyBatch) Callback(par interface{}) {
	fmt.Println("MyBatch Callback is called with batch size:", len(b.Lists()), par)

	for _, item := range b.Lists() {
		b.results <- item.(int)
		log.Println(item.(int))
		wg.Done()
	}

	b.List = nil
}

func newMyBatch(results chan<- int) IBaseBatch {
	return &MyBatch{results: results}
}

func TestSendBatch(t *testing.T) {
	rand.Seed(time.Now().UnixNano()) // 确保每次运行程序时，都会得到不同的随机数
	results := make(chan int)
	num := 20
	go func() {
		model := InitBatch(100, 100, 5, func() IBaseBatch { return newMyBatch(results) })
		for i := 1; i <= num; i++ {
			is := i
			min := 100
			max := 500
			randomMillisecond := rand.Intn(max-min+1) + min
			time.Sleep(time.Duration(randomMillisecond) * time.Millisecond)
			wg.Add(1)
			model.SendBatch(is)
		}

		wg.Wait()      // Wait for all the items to be processed
		close(results) // Then close the channel
	}()

	total := 0
	for sum := range results {
		total += sum
	}

	expected := (1 + num) * num / 2
	if total != expected {
		t.Errorf("Expected sum %d but got %d", expected, total)
	} else {
		t.Log("success", total)
	}
}
