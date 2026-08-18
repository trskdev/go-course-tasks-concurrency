// ============================================================
// Задача: Merge N Channels  🟡 Middle
// ============================================================
//
// Реализуй три варианта слияния каналов:
//
//   1. merge2(a, b <-chan int) <-chan int
//      Сливает ровно 2 канала. Используй select.
//
//   2. mergeN(channels ...<-chan int) <-chan int
//      Сливает произвольное количество каналов.
//      Закрывает выходной канал когда все входные закрыты.
//
//   3. mergeOrdered(channels ...<-chan int) <-chan int
//      Сливает N каналов СОХРАНЯЯ ОТНОСИТЕЛЬНЫЙ ПОРЯДОК внутри каждого.
//      Т.е. если channel[0] выдаёт 1,3,5 и channel[1] выдаёт 2,4,6,
//      то выходной может дать 1,2,3,4,5,6 или 1,3,2,4,5,6 (нет гарантий между каналами)
//      но никогда 3,1,...  (нарушение порядка внутри одного канала)
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"fmt"
	"sort"
	"sync"
)

// TODO: реализуй merge2
// Подсказка: когда один из каналов закрылся — надо продолжать читать из другого,
// но select всё равно может выбрать закрытый (он отдаёт zero-value) — подумай как его исключить
func merge2(a, b <-chan int) <-chan int {
	out := make(chan int)

	var wg sync.WaitGroup

	merger := func(ch <-chan int) {
		defer wg.Done()
		for num := range ch {
			out <- num
		}
	}

	wg.Add(2)
	go merger(a)
	go merger(b)

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// TODO: реализуй mergeN
// Подсказка: запусти по горутине на каждый канал; нужна синхронизация чтобы
// понять когда все каналы иссякли — только после этого можно закрыть out
func mergeN(channels ...<-chan int) <-chan int {
	out := make(chan int)

	var wg sync.WaitGroup

	merger := func(ch <-chan int) {
		defer wg.Done()
		for num := range ch {
			out <- num
		}
	}

	wg.Add(len(channels))
	for _, ch := range channels {
		go merger(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// TODO: реализуй mergeOrdered — сохрани порядок внутри каждого канала
// Подсказка: достаточно ли уже готового mergeN, или нужно что-то ещё?
// Подумай: если горутина читает из канала последовательно — может ли она нарушить порядок?
func mergeOrdered(channels ...<-chan int) <-chan int {
	return mergeN(channels...)
}

func sourceChan(nums ...int) <-chan int {
	ch := make(chan int, len(nums))
	for _, n := range nums {
		ch <- n
	}
	close(ch)
	return ch
}

func main() {
	a := sourceChan(1, 3, 5)
	b := sourceChan(2, 4, 6)

	var result []int
	for v := range merge2(a, b) {
		result = append(result, v)
	}
	sort.Ints(result)
	fmt.Println("merge2:", result) // [1 2 3 4 5 6]

	channels := make([]<-chan int, 4)
	for i := range channels {
		start := i*5 + 1
		nums := make([]int, 5)
		for j := range nums {
			nums[j] = start + j
		}
		channels[i] = sourceChan(nums...)
	}

	var result2 []int
	for v := range mergeN(channels...) {
		result2 = append(result2, v)
	}
	sort.Ints(result2)
	fmt.Println("mergeN:", result2) // [1 2 3 ... 20]
}
