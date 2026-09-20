// ============================================================
// Задача: Producer-Consumer с bounded buffer  🟡 Middle
// ============================================================
//
// Классика на собесах Junior/Middle уровня.
//
// Реализуй через каналы:
//   - M производителей генерируют числа 0..N
//   - K потребителей читают, возводят в квадрат, пишут в results
//   - Буфер между ними ограничен (размер B)
//
// Требования:
//   - Потребители завершаются когда производители закончили И буфер пуст
//   - Нет утечек горутин
//   - Все числа должны быть обработаны ровно один раз
//
// Реализуй ДВА варианта:
//   1. Через каналы (идиоматично в Go)
//   2. Через sync.Cond (для понимания классических примитивов)
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
)

// === Вариант 1: через каналы ===

// TODO: реализуй producerConsumerChan
// Подсказка: два буферизованных канала и два WaitGroup — для производителей и потребителей
func producerConsumerChan(producers, consumers, n, bufSize int) []int {
	prodChan := make(chan int, bufSize)
	consChan := make(chan int, bufSize)
	res := []int{}

	var nCounter atomic.Int64

	var prodWg sync.WaitGroup
	var consWg sync.WaitGroup
	var resWg sync.WaitGroup

	for range producers {
		prodWg.Add(1)

		go func() {
			defer prodWg.Done()

			for {
				// start with 0, so skipping doesn't bother us
				next := nCounter.Add(1)
				cur := next - 1
				if cur >= int64(n) {
					return
				}

				prodChan <- int(cur)
			}
		}()
	}

	go func() {
		prodWg.Wait()
		close(prodChan)
	}()

	for range consumers {
		consWg.Add(1)

		go func() {
			defer consWg.Done()

			for i := range prodChan {
				consChan <- i * i
			}
		}()
	}

	resWg.Add(1)
	go func() {
		defer resWg.Done()
		for i := range consChan {
			res = append(res, i)
		}
	}()

	go func() {
		consWg.Wait()
		close(consChan)
	}()

	resWg.Wait()

	return res
}

func TestProducerConsumer(t *testing.T) {
	results := producerConsumerChan(3, 4, 20, 5)
	sort.Ints(results)

	if len(results) != 20 {
		t.Fatalf("ожидали 20 результатов, получили %d", len(results))
	}

	// Проверяем что это квадраты чисел 0..19
	for i, v := range results {
		want := i * i
		if v != want {
			t.Errorf("[%d] = %d, want %d", i, v, want)
		}
	}
}

// === Вариант 2: через sync.Cond ===

// TODO: реализуй producerConsumerCond
// Подсказка: буфер — обычный срез; производители ждут пока буфер полон, потребители — пока пуст
func producerConsumerCond(producers, consumers, n, bufSize int) []int {
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	buffer := make([]int, 0, bufSize)
	res := []int{}

	var prodWg sync.WaitGroup
	var consWg sync.WaitGroup
	var nCounter atomic.Int64

	producersDone := false

	for range producers {
		prodWg.Add(1)
		go func() {
			defer prodWg.Done()

			for {
				next := nCounter.Add(1)
				cur := next - 1
				if cur >= int64(n) {
					return
				}

				mu.Lock()
				for len(buffer) == bufSize {
					cond.Wait()
				}

				buffer = append(buffer, int(cur))

				cond.Broadcast()
				mu.Unlock()
			}
		}()
	}

	go func() {
		prodWg.Wait()
		mu.Lock()
		producersDone = true
		cond.Broadcast()
		mu.Unlock()
	}()

	for range consumers {
		consWg.Add(1)
		go func() {
			defer consWg.Done()
			for {
				mu.Lock()
				for len(buffer) == 0 && !producersDone {
					cond.Wait()
				}

				for len(buffer) == 0 && producersDone {
					mu.Unlock()
					return
				}

				val := buffer[0]
				buffer = buffer[1:]

				cond.Broadcast()
				mu.Unlock()

				squared := val * val

				mu.Lock()
				res = append(res, squared)
				mu.Unlock()
			}
		}()
	}

	consWg.Wait()
	return res
}

func TestProducerConsumerCond(t *testing.T) {
	results := producerConsumerCond(3, 4, 20, 5)
	sort.Ints(results)

	if len(results) != 20 {
		t.Fatalf("ожидали 20 результатов, получили %d", len(results))
	}
	for i, v := range results {
		if v != i*i {
			t.Errorf("[%d] = %d, want %d", i, v, i*i)
		}
	}
}

func main() {
	results := producerConsumerChan(2, 3, 10, 3)
	sort.Ints(results)
	fmt.Println("Результаты:", results)
}
