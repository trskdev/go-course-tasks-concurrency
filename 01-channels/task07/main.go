// ============================================================
// Задача: Ordered Parallel Pipeline  🟡 Middle
// ============================================================
//
// Есть поток входных задач и функция process(x) которая отрабатывает
// неопределённое время. Нужно запустить обработку ПАРАЛЛЕЛЬНО (N воркеров),
// но на выходе сохранить ИСХОДНЫЙ порядок задач.
//
// Интерфейс:
//
//   func OrderedMap[I, O any](
//       in <-chan I,
//       workers int,
//       fn func(I) O,
//   ) <-chan O
//
// Требования:
//   - Обработка параллельная: workers горутин одновременно вызывают fn
//   - Порядок результатов СТРОГО такой же как порядок входов
//   - Медленная задача не блокирует следующие от старта, но блокирует их ВЫВОД
//   - Нет утечек горутин: после закрытия in — out тоже закроется
//
// Пример:
//   in: 1, 2, 3, 4, 5
//   fn(x) = x*x но со случайной задержкой
//   out: 1, 4, 9, 16, 25 — порядок сохраняется
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"fmt"
	"math/rand"
	"time"
)

type task[I, O any] struct {
	val   I
	resCh chan O
}

// TODO: реализуй OrderedMap
// Подсказка: параллельно запусти workers горутин; каждой задаче нужен способ
// дождаться своей очереди на выход. Подумай про промежуточный канал per-job
// (канал-заглушку, который закроется когда результат готов).
func OrderedMap[I, O any](in <-chan I, workers int, fn func(I) O) <-chan O {
	out := make(chan O)
	queue := make(chan chan O, workers)
	taskCh := make(chan task[I, O])

	go func() {
		defer close(queue)
		defer close(taskCh)
		for val := range in {
			resCh := make(chan O, 1)
			taskCh <- task[I, O]{val: val, resCh: resCh}
			queue <- resCh
		}
	}()

	for range workers {
		go func() {
			for t := range taskCh {
				res := fn(t.val)
				t.resCh <- res
				close(t.resCh)
			}
		}()
	}

	go func() {
		defer close(out)

		for resCh := range queue {
			out <- <-resCh
		}
	}()

	return out
}

func main() {
	in := make(chan int, 10)
	for i := 1; i <= 10; i++ {
		in <- i
	}
	close(in)

	out := OrderedMap(in, 4, func(n int) int {
		time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
		return n * n
	})

	for v := range out {
		fmt.Println(v) // ожидаем: 1 4 9 16 25 36 49 64 81 100 (в строгом порядке)
	}
}
