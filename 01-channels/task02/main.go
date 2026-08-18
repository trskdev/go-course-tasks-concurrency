// ============================================================
// Задача: Fan-Out / Fan-In  🟡 Middle
// ============================================================
//
// Реализуй паттерн "распределить и собрать":
//
//   1. fanOut(in <-chan int, n int) []<-chan int
//      Распределяет задачи из одного канала по n воркерам.
//      Каждый воркер получает примерно равное количество задач.
//
//   2. fanIn(channels ...<-chan int) <-chan int
//      Сливает несколько каналов в один.
//      Закрывает выходной канал когда все входные закрыты.
//
//   3. process(in <-chan int) <-chan int
//      Воркер: умножает число на 2. Имитирует задержку.
//
// Схема:
//   source → fanOut → [worker1, worker2, worker3] → fanIn → results
//
// Требования:
//   - Порядок результатов не важен (параллельная обработка)
//   - Нет утечек горутин (проверяй через runtime.NumGoroutine)
//   - Работает с -race без ошибок
//
// Ожидаемый вывод (порядок может отличаться):
//   Обработано 10 задач. Сумма: 110  (1+2+...+10)*2 = 110

package main

import (
	"fmt"
	"sync"
	"time"
)

// process имитирует "тяжёлую" работу: удваивает число
func process(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for n := range in {
			time.Sleep(10 * time.Millisecond) // имитация работы
			out <- n * 2
		}
	}()
	return out
}

// TODO: реализуй fanOut — раздай задачи n воркерам
func fanOut(in <-chan int, n int) []<-chan int {
	channels := make([]<-chan int, n)
	for i := range n {
		ch := make(chan int)
		channels[i] = ch

		go func(out chan<- int) {
			defer close(out)
			for num := range in {
				out <- num
			}
		}(ch)
	}
	return channels
}

// TODO: реализуй fanIn — слей все каналы в один
func fanIn(channels ...<-chan int) <-chan int {
	out := make(chan int)
	var wg sync.WaitGroup

	for _, ch := range channels {
		wg.Add(1)
		go func(ch <-chan int) {
			defer wg.Done()
			for num := range ch {
				out <- num
			}
		}(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func main() {
	const numJobs = 10
	const numWorkers = 3

	// Источник задач
	source := make(chan int, numJobs)
	for i := 1; i <= numJobs; i++ {
		source <- i
	}
	close(source)

	// Распределяем и обрабатываем
	workers := fanOut(source, numWorkers)
	var processedChans []<-chan int
	for _, w := range workers {
		processedChans = append(processedChans, process(w))
	}

	// Собираем результаты
	sum := 0
	count := 0
	for result := range fanIn(processedChans...) {
		sum += result
		count++
	}

	fmt.Printf("Обработано %d задач. Сумма: %d\n", count, sum)
	// Ожидаемо: Обработано 10 задач. Сумма: 110
}
