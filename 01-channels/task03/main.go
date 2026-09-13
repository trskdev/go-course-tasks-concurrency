// ============================================================
// Задача: Done Channel — отмена цепочки горутин  🟡 Middle
// ============================================================
//
// Классический вопрос на собеседованиях уровня Middle.
//
// У тебя есть пайплайн из трёх стадий. При вызове cancel()
// весь пайплайн должен немедленно завершиться без утечек горутин.
//
// Реализуй:
//   1. withDone(done <-chan struct{}, in <-chan int) <-chan int
//      Оборачивает любой канал — останавливает чтение при закрытии done
//
//   2. Перепиши generate и square с поддержкой done
//      (используй withDone или добавь параметр напрямую)
//
// Сценарии:
//   A) Читаем 3 значения, потом cancel() — остальные значения дропаются
//   B) cancel() до начала чтения — ни одного значения не получаем
//
// Проверь:
//   go test -race -v ./...
//
// Ожидаемый вывод:
//   Получено 3 значения: [4 16 36]
//   Горутин после отмены: N (должно быть близко к начальному)

package main

import (
	"fmt"
	"runtime"
	"time"
)

// withDone оборачивает канал in — при закрытии done прекращает чтение.
// TODO: реализуй
// Подсказка: оба направления — чтение и запись — должны учитывать отмену
func withDone(done <-chan struct{}, in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for {
			select {
			case <-done:
				return
			case n, ok := <-in:
				if !ok {
					return
				}

				select {
				case <-done:
					return
				case out <- n:
				}
			}
		}
	}()
	return out
}

func generate(done <-chan struct{}, nums ...int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		// TODO: защити отправку каждого числа от блокировки при отмене
		for _, num := range nums {
			select {
			case <-done:
				return
			case out <- num:
			}
		}
	}()
	return out
}

// TODO: реализуй square с поддержкой отмены через done
func square(done <-chan struct{}, in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		// TODO: аналогично generate, но читаем из канала, а не из среза
		for {
			select {
			case <-done:
				return
			case n, ok := <-in:
				if !ok {
					return
				}

				select {
				case <-done:
					return
				case out <- n * n:
				}
			}
		}
	}()
	return out
}

func main() {
	goroutinesBefore := runtime.NumGoroutine()
	fmt.Printf("Горутин в начале: %d\n", goroutinesBefore)

	done := make(chan struct{})

	nums := make([]int, 100)
	for i := range nums {
		nums[i] = i + 1
	}

	results := square(done, generate(done, nums...))

	// Читаем только 3 значения, потом отменяем
	var got []int
	for v := range results {
		got = append(got, v)
		if len(got) == 3 {
			close(done) // отменяем пайплайн
			break
		}
	}

	fmt.Printf("Получено %d значений: %v\n", len(got), got)

	// Даём горутинам время завершиться
	time.Sleep(50 * time.Millisecond)

	goroutinesAfter := runtime.NumGoroutine()
	fmt.Printf("Горутин после отмены: %d (было %d)\n", goroutinesAfter, goroutinesBefore)

	if goroutinesAfter > goroutinesBefore+2 {
		fmt.Println("⚠ Возможна утечка горутин!")
	} else {
		fmt.Println("✓ Утечек горутин нет")
	}
}
