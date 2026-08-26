// ============================================================
// Задача: FooBar — LeetCode 1115  🟢 Junior
// ============================================================
//
// Две горутины: одна печатает "Foo", другая "Bar".
// Должны чередоваться строго: FooBar FooBar FooBar...
//
// Реализуй класс FooBar:
//   func (fb *FooBar) Foo(fn func())  // fn() печатает "Foo"
//   func (fb *FooBar) Bar(fn func())  // fn() печатает "Bar"
//
// Foo и Bar запускаются в разных горутинах одновременно и должны выводить:
//   FooBar FooBar FooBar... (n раз)
//
// Реализуй ДВА варианта:
//   A) через два канала (семафорная техника)
//   B) через sync.Mutex + condition variables
//
// Проверь что вывод всегда правильный:
//   go test -race -v -count=10 ./...

package main

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// === Вариант A: каналы ===

type FooBarChan struct {
	n      int
	fooSem chan struct{}
	barSem chan struct{}
}

// TODO: реализуй NewFooBarChan
// Подсказка: два бинарных семафора (каналы ёмкостью 1); в один заранее положи токен — тот, кто стартует первым
func NewFooBarChan(n int) *FooBarChan {
	ch := &FooBarChan{
		n:      n,
		fooSem: make(chan struct{}, 1),
		barSem: make(chan struct{}, 1),
	}
	ch.fooSem <- struct{}{}
	return ch
}

// TODO: реализуй Foo — жди разрешения, вызови fn, передай разрешение Bar
func (fb *FooBarChan) Foo(fn func()) {
	for range fb.n {
		<-fb.fooSem
		fn()
		fb.barSem <- struct{}{}
	}
}

// TODO: реализуй Bar — жди разрешения от Foo, вызови fn, передай разрешение обратно Foo
func (fb *FooBarChan) Bar(fn func()) {
	for range fb.n {
		<-fb.barSem
		fn()
		fb.fooSem <- struct{}{}
	}
}

// === Вариант B: Mutex + флаг ===

type FooBarMutex struct {
	n    int
	mu   sync.Mutex
	cond *sync.Cond
	turn int // 0 = foo, 1 = bar
}

func NewFooBarMutex(n int) *FooBarMutex {
	fb := &FooBarMutex{n: n}
	fb.cond = sync.NewCond(&fb.mu)
	return fb
}

// TODO: реализуй Foo и Bar для варианта B
// Подсказка: sync.Cond позволяет эффективно ожидать смены флага turn
func (fb *FooBarMutex) Foo(fn func()) {
	for range fb.n {
		fb.mu.Lock()

		for fb.turn != 0 {
			fb.cond.Wait()
		}

		fn()
		fb.turn = 1

		fb.cond.Broadcast()
		fb.mu.Unlock()
	}
}

func (fb *FooBarMutex) Bar(fn func()) {
	for range fb.n {
		fb.mu.Lock()

		for fb.turn != 1 {
			fb.cond.Wait()
		}

		fn()
		fb.turn = 0

		fb.cond.Broadcast()
		fb.mu.Unlock()
	}
}

// === Тесты ===

func testFooBar(t *testing.T, n int, foo func(func()), bar func(func())) {
	var mu sync.Mutex
	var sb strings.Builder
	var wg sync.WaitGroup
	wg.Add(2)

	go func() { defer wg.Done(); foo(func() { mu.Lock(); sb.WriteString("Foo"); mu.Unlock() }) }()
	go func() { defer wg.Done(); bar(func() { mu.Lock(); sb.WriteString("Bar"); mu.Unlock() }) }()

	wg.Wait()
	result := sb.String()
	expected := strings.Repeat("FooBar", n)
	if result != expected {
		t.Errorf("got %q, want %q", result, expected)
	}
}

func TestFooBarChan(t *testing.T) {
	fb := NewFooBarChan(5)
	testFooBar(t, 5, fb.Foo, fb.Bar)
}

func TestFooBarMutex(t *testing.T) {
	fb := NewFooBarMutex(5)
	testFooBar(t, 5, fb.Foo, fb.Bar)
}

func main() {
	fb := NewFooBarChan(3)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); fb.Foo(func() { fmt.Print("Foo") }) }()
	go func() { defer wg.Done(); fb.Bar(func() { fmt.Print("Bar") }) }()
	wg.Wait()
	fmt.Println()
}
