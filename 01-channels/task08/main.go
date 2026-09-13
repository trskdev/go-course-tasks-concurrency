// ============================================================
// Задача: Tee Channel — раздвоение потока  🟡 Middle
// ============================================================
//
// Реализуй аналог unix-команды `tee` для каналов:
//
//   func Tee[T any](done <-chan struct{}, in <-chan T) (<-chan T, <-chan T)
//
// Каждое значение из in должно попасть В ОБА выходных канала.
// При закрытии in — оба выхода тоже закрываются.
// При закрытии done — горутина Tee завершается без утечки.
//
// Важно: медленный читатель одного из выходов НЕ должен влиять на скорость
// отправки в другой больше чем нужно — но при этом значение всё равно должно
// попасть ОБА. Т.е. мы ждём пока оба прочитают текущее значение, потом читаем
// следующее из in. (Это простейший вариант — без буфера.)
//
// Более продвинутый вариант (бонус):
//   func TeeN[T any](done <-chan struct{}, in <-chan T, n int) []<-chan T
//   раздвоение в N выходов.
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"fmt"
	"reflect"
	"sync"
)

// TODO: реализуй Tee
// Подсказка: наивное "out1 <- v; out2 <- v" сериализует получателей.
// Подумай как через select отправить в оба канала независимо
// (поиск: "nil channel trick" если застрял).
func Tee[T any](done <-chan struct{}, in <-chan T) (<-chan T, <-chan T) {
	out1 := make(chan T)
	out2 := make(chan T)

	go func() {
		defer close(out1)
		defer close(out2)

		for {
			select {
			case <-done:
				return
			case item, ok := <-in:
				if !ok {
					return
				}

				ch1, ch2 := out1, out2

				for ch1 != nil || ch2 != nil {
					select {
					case ch1 <- item:
						ch1 = nil
					case ch2 <- item:
						ch2 = nil
					case <-done:
						return
					}
				}
			}
		}
	}()

	return out1, out2
}

func TeeN[T any](done <-chan struct{}, in <-chan T, n int) []<-chan T {
	if n <= 0 {
		return nil
	}

	outputs := make([]chan T, n)
	result := make([]<-chan T, n)
	for i := 0; i < n; i++ {
		ch := make(chan T)
		outputs[i] = ch
		result[i] = ch
	}

	go func() {
		defer func() {
			for _, ch := range outputs {
				close(ch)
			}
		}()

		for {
			select {
			case <-done:
				return
			case item, ok := <-in:
				if !ok {
					return
				}

				cases := make([]reflect.SelectCase, n+1)
				cases[0] = reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(done),
				}

				for i, ch := range outputs {
					cases[i+1] = reflect.SelectCase{
						Dir:  reflect.SelectSend,
						Chan: reflect.ValueOf(ch),
						Send: reflect.ValueOf(item),
					}
				}

				remaining := n

				for remaining > 0 {
					chosen, _, _ := reflect.Select(cases)

					if chosen == 0 {
						return
					}

					cases[chosen].Chan = reflect.Value{}
					remaining--
				}
			}
		}
	}()

	return result
}

func main() {
	done := make(chan struct{})
	defer close(done)

	source := make(chan int)
	go func() {
		defer close(source)
		for i := 1; i <= 5; i++ {
			source <- i
		}
	}()

	a, b := Tee(done, source)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for v := range a {
			fmt.Println("A:", v)
		}
	}()
	go func() {
		defer wg.Done()
		for v := range b {
			fmt.Println("B:", v)
		}
	}()

	wg.Wait()
	// Оба A и B должны получить 1,2,3,4,5
}
