// ============================================================
// Задача: Канальный пайплайн  🟢 Junior
// ============================================================
//
// Реализуй трёхступенчатый пайплайн:
//
//   generate(nums...) → square() → filterEven() → вывод
//
//   generate    — принимает срез чисел, отправляет в канал по одному
//   square      — возводит каждое число в квадрат
//   filterEven  — пропускает только чётные числа
//
// Каждая стадия:
//   - принимает <-chan int
//   - возвращает <-chan int
//   - запускает горутину которая закрывает выходной канал когда входной закрыт
//
// Ожидаемый вывод для generate(1,2,3,4,5):
//   4    (2²)
//   16   (4²)
//
// Запуск:
//   go run main.go
//   go test -v ./...

package main

import "fmt"

// TODO: реализуй generate — принимает числа, отправляет в канал
func generate(nums ...int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for _, num := range nums {
			out <- num
		}
	}()
	return out
}

// TODO: реализуй square — возводит в квадрат каждое число
func square(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for num := range in {
			out <- num * num
		}
	}()
	return out
}

// TODO: реализуй filterEven — пропускает только чётные числа
func filterEven(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for num := range in {
			if num%2 == 0 {
				out <- num
			}
		}
	}()
	return out
}

func main() {
	// Пайплайн: 1,2,3,4,5 → квадраты → только чётные
	c := filterEven(square(generate(1, 2, 3, 4, 5)))
	for v := range c {
		fmt.Println(v)
	}
}
