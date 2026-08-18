// ============================================================
// Задача: Token Bucket Rate Limiter  🟡 Middle
// ============================================================
//
// Вопрос с собесов уровня Middle.
//
// Реализуй rate limiter без сторонних библиотек (не golang.org/x/time/rate).
//
// Алгоритм Token Bucket:
//   - Есть "ведро" ёмкостью capacity токенов
//   - Каждые 1/rate секунд добавляется 1 токен (но не больше capacity)
//   - Allow() забирает 1 токен и возвращает true, или false если ведро пусто
//
// Реализуй ДВА варианта:
//
//   1. TokenBucket — на основе time.Ticker и горутины
//   2. LazyTokenBucket — ленивый: считает токены математически при каждом вызове
//      (без горутин! Используй time.Since для вычисления накопленных токенов)
//
// LazyTokenBucket предпочтителен в продакшне: не создаёт горутины.
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// === Вариант 1: с горутиной ===

type TokenBucket struct {
	tokens   atomic.Int64
	capacity int64
	quit     chan struct{}
}

// TODO: реализуй NewTokenBucket
// Подсказка: фоновая горутина добавляет токены с нужной частотой; начинай с полным ведром
func NewTokenBucket(rate float64, capacity int64) *TokenBucket {
	tb := &TokenBucket{capacity: capacity, quit: make(chan struct{})}

	tb.tokens.Store(capacity)

	tick := time.Duration(float64(time.Second) / rate)
	ticker := time.NewTicker(tick)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				for {
					count := tb.tokens.Load()
					if count >= capacity {
						break
					}

					if tb.tokens.CompareAndSwap(count, count+1) {
						break
					}
				}
			case <-tb.quit:
				return
			}
		}
	}()

	return tb
}

// TODO: Allow забирает 1 токен. Возвращает false если ведро пусто.
// Подсказка: операция должна быть потокобезопасной без мьютекса
func (tb *TokenBucket) Allow() bool {
	for {
		count := tb.tokens.Load()

		if count <= 0 {
			return false
		}

		if tb.tokens.CompareAndSwap(count, count-1) {
			return true
		}
	}
}

func (tb *TokenBucket) Close() { close(tb.quit) }

// === Вариант 2: ленивый (без горутин) ===

type LazyTokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	capacity   float64
	rate       float64 // токенов в секунду
	lastRefill time.Time
}

// TODO: реализуй NewLazyTokenBucket
func NewLazyTokenBucket(rate, capacity float64) *LazyTokenBucket {
	return &LazyTokenBucket{tokens: capacity, capacity: capacity, rate: rate, lastRefill: time.Now()}
}

// TODO: реализуй Allow для LazyTokenBucket
// Подсказка: горутина не нужна — используй lastRefill и time.Since между вызовами
func (lb *LazyTokenBucket) Allow() bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	farmedTokens := time.Since(lb.lastRefill).Seconds() * lb.rate
	lb.lastRefill = time.Now()

	totalTokens := lb.tokens + farmedTokens
	lb.tokens = min(totalTokens, lb.capacity)

	if lb.tokens < 1.0 {
		return false
	}

	lb.tokens -= 1.0
	return true
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func main() {
	fmt.Println("=== TokenBucket (10 req/s, burst 3) ===")
	tb := NewTokenBucket(10, 3)
	defer tb.Close()

	for i := 0; i < 6; i++ {
		ok := tb.Allow()
		fmt.Printf("запрос %d: %v\n", i+1, ok)
		if i == 2 {
			time.Sleep(200 * time.Millisecond) // ждём накопления токенов
		}
	}

	fmt.Println("\n=== LazyTokenBucket (5 req/s, burst 2) ===")
	lb := NewLazyTokenBucket(5, 2)

	for i := 0; i < 5; i++ {
		ok := lb.Allow()
		fmt.Printf("запрос %d: %v\n", i+1, ok)
		time.Sleep(100 * time.Millisecond)
	}
}
