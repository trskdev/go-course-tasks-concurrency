// ============================================================
// Задача: Print In Order — LeetCode 1114  🟢 Junior
// ============================================================
//
// Задача с LeetCode, часто задают на собесах Junior-уровня.
//
// Три метода: first(), second(), third() запускаются в произвольном порядке
// в разных горутинах. Гарантируй что они выполнятся строго в порядке: 1 → 2 → 3.
//
// Реализуй через:
//   A) каналы (простейший способ)
//   B) sync.WaitGroup
//   C) atomic + spin (для понимания, не для продакшна)
//
// Проверь через тест что порядок всегда правильный при любом порядке запуска горутин.

package main

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// === Вариант A: через каналы ===

type OrderedPrinterChan struct {
	after1 chan struct{}
	after2 chan struct{}
}

// TODO: реализуй NewOrderedPrinterChan
// Подсказка: нужны сигналы "первая уже отработала" и "вторая уже отработала"
func NewOrderedPrinterChan() *OrderedPrinterChan {
	return &OrderedPrinterChan{
		after1: make(chan struct{}),
		after2: make(chan struct{}),
	}
}

// TODO: реализуй First — вызови fn и сигнализируй что можно запускать Second
func (p *OrderedPrinterChan) First(fn func()) {
	fn()
	close(p.after1)
}

// TODO: реализуй Second — дождись сигнала от First, вызови fn, сигнализируй Third
func (p *OrderedPrinterChan) Second(fn func()) {
	<-p.after1
	fn()
	close(p.after2)
}

// TODO: реализуй Third — дождись сигнала от Second и вызови fn
func (p *OrderedPrinterChan) Third(fn func()) {
	<-p.after2
	fn()
}

// === Вариант B: через WaitGroup ===

type OrderedPrinterWG struct {
	wg1 sync.WaitGroup
	wg2 sync.WaitGroup
}

func NewOrderedPrinterWG() *OrderedPrinterWG {
	p := &OrderedPrinterWG{}
	p.wg1.Add(1)
	p.wg2.Add(1)
	return p
}

// TODO: реализуй First, Second, Third через WaitGroup
func (p *OrderedPrinterWG) First(fn func()) {
	fn()
	p.wg1.Done()
}
func (p *OrderedPrinterWG) Second(fn func()) {
	p.wg1.Wait()
	fn()
	p.wg2.Done()
}
func (p *OrderedPrinterWG) Third(fn func()) {
	p.wg2.Wait()
	fn()
}

// === Вариант C: через atomic ===

type OrderedPrinterAtomic struct {
	state atomic.Int32
}

// TODO: реализуй через spin-ожидание atomic
func (p *OrderedPrinterAtomic) First(fn func()) {
	fn()
	p.state.Store(1)
}
func (p *OrderedPrinterAtomic) Second(fn func()) {
	for p.state.Load() != 1 {
	}

	fn()
	p.state.Store(2)
}
func (p *OrderedPrinterAtomic) Third(fn func()) {
	for p.state.Load() != 2 {
	}

	fn()
}

// === Тесты ===

func runInOrder(first, second, third func(func())) string {
	var sb strings.Builder
	var wg sync.WaitGroup
	wg.Add(3)

	// Запускаем в "неправильном" порядке
	go func() { defer wg.Done(); third(func() { sb.WriteString("third") }) }()
	go func() { defer wg.Done(); second(func() { sb.WriteString("second") }) }()
	go func() { defer wg.Done(); first(func() { sb.WriteString("first") }) }()

	wg.Wait()
	return sb.String()
}

func TestOrderChan(t *testing.T) {
	p := NewOrderedPrinterChan()
	result := runInOrder(p.First, p.Second, p.Third)
	if result != "firstsecondthird" {
		t.Errorf("порядок нарушен: %q", result)
	}
}

func TestOrderWG(t *testing.T) {
	p := NewOrderedPrinterWG()
	result := runInOrder(p.First, p.Second, p.Third)
	if result != "firstsecondthird" {
		t.Errorf("порядок нарушен: %q", result)
	}
}

func TestOrderAtomic(t *testing.T) {
	p := &OrderedPrinterAtomic{}
	result := runInOrder(p.First, p.Second, p.Third)
	if result != "firstsecondthird" {
		t.Errorf("порядок нарушен: %q", result)
	}
}

func main() {
	p := NewOrderedPrinterChan()
	var wg sync.WaitGroup
	wg.Add(3)

	go func() { defer wg.Done(); p.Third(func() { fmt.Print("third ") }) }()
	go func() { defer wg.Done(); p.First(func() { fmt.Print("first ") }) }()
	go func() { defer wg.Done(); p.Second(func() { fmt.Print("second ") }) }()

	wg.Wait()
	fmt.Println()
}
