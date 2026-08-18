// ============================================================
// Задача: Debounce и Throttle  🟡 Middle
// ============================================================
//
// Два похожих паттерна часто путают на собесах — реализуй оба.
//
// Debounce:
//   - Каждый вызов сбрасывает таймер
//   - fn вызовется только если между вызовами прошло >= d
//   - Пример: автокомплит — запрос в API только после того как пользователь
//     перестал печатать на 300мс
//
//   type Debouncer struct { ... }
//
//   func NewDebouncer(d time.Duration, fn func()) *Debouncer
//   func (db *Debouncer) Trigger()
//   func (db *Debouncer) Stop()
//
// Throttle:
//   - fn вызывается максимум раз в d
//   - Все промежуточные вызовы игнорируются (leading edge)
//   - Пример: scroll-handler — обрабатывать не чаще раза в 16мс
//
//   type Throttler struct { ... }
//
//   func NewThrottler(d time.Duration, fn func()) *Throttler
//   func (t *Throttler) Trigger()
//
// Требования:
//   - Оба безопасны при конкурентных Trigger
//   - Debouncer.Stop отменяет запланированный вызов
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

// === Debouncer ===

type Debouncer struct {
	d     time.Duration
	fn    func()
	mu    sync.Mutex
	timer *time.Timer
}

// TODO: реализуй NewDebouncer
func NewDebouncer(d time.Duration, fn func()) *Debouncer {
	return &Debouncer{d: d, fn: fn}
}

// TODO: реализуй Trigger
// Подсказка: time.Timer умеет Reset — используй его. Stop+new также работает.
func (db *Debouncer) Trigger() {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.timer == nil {
		db.timer = time.AfterFunc(db.d, db.fn)
		return
	}
	db.timer.Reset(db.d)
}

// TODO: реализуй Stop
func (db *Debouncer) Stop() {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.timer != nil {
		db.timer.Stop()
	}
}

// === Throttler ===

type Throttler struct {
	d      time.Duration
	fn     func()
	lastNs atomic.Int64
}

// TODO: реализуй NewThrottler
func NewThrottler(d time.Duration, fn func()) *Throttler {
	return &Throttler{d: d, fn: fn}
}

// TODO: реализуй Trigger
// Подсказка: запомни время последнего успешного вызова и используй CAS
// для атомарной проверки "прошло ли d с прошлого раза"
func (t *Throttler) Trigger() {
	for {
		now := time.Now().UnixNano()
		lastCall := t.lastNs.Load()

		if now-lastCall < t.d.Nanoseconds() {
			return
		}

		if t.lastNs.CompareAndSwap(lastCall, now) {
			break
		}
	}

	t.fn()
}

func main() {
	var dbCalls atomic.Int32
	db := NewDebouncer(50*time.Millisecond, func() {
		dbCalls.Add(1)
	})

	// 10 триггеров подряд — должен вызваться только 1 раз
	for i := 0; i < 10; i++ {
		db.Trigger()
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("Debouncer: %d вызовов (ожидаем 1)\n", dbCalls.Load())

	var thCalls atomic.Int32
	th := NewThrottler(30*time.Millisecond, func() {
		thCalls.Add(1)
	})

	// 100 триггеров в течение ~100мс — должно быть ~4 вызова (100/30)
	for i := 0; i < 100; i++ {
		th.Trigger()
		time.Sleep(1 * time.Millisecond)
	}
	fmt.Printf("Throttler: %d вызовов (ожидаем 3-4)\n", thCalls.Load())
}
