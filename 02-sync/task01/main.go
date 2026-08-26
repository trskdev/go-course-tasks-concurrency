// ============================================================
// Задача: Потокобезопасный TTL-кеш на RWMutex  🟡 Middle
// ============================================================
//
// Частый вопрос на собесах уровня Middle.
//
// Реализуй TTLCache — кеш с временем жизни записей.
//
//   type TTLCache[K comparable, V any] struct { ... }
//
//   func NewTTLCache[K comparable, V any](ttl time.Duration) *TTLCache[K, V]
//   func (c *TTLCache[K, V]) Set(key K, value V)
//   func (c *TTLCache[K, V]) Get(key K) (V, bool)
//   func (c *TTLCache[K, V]) Delete(key K)
//   func (c *TTLCache[K, V]) Len() int
//
// Требования:
//   - Get использует RLock (параллельное чтение)
//   - Set и Delete используют Lock (эксклюзивная запись)
//   - Get возвращает (zero, false) для устаревших записей
//   - Устаревшие записи удаляются лениво (при следующем Get)
//   - Дополнительно: метод Cleanup() удаляет все устаревшие записи
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"fmt"
	"sync"
	"time"
)

type entry[V any] struct {
	value  V
	expiry time.Time
}

type TTLCache[K comparable, V any] struct {
	mu    sync.RWMutex
	items map[K]entry[V]
	ttl   time.Duration
}

// TODO: реализуй NewTTLCache
func NewTTLCache[K comparable, V any](ttl time.Duration) *TTLCache[K, V] {
	return &TTLCache[K, V]{
		items: make(map[K]entry[V]),
		ttl:   ttl,
	}
}

// TODO: реализуй Set — записывает значение с временем жизни ttl
func (c *TTLCache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	val := entry[V]{value: value, expiry: time.Now().Add(c.ttl)}
	c.items[key] = val
}

// TODO: реализуй Get — возвращает значение если оно есть и не устарело
// Подсказка: Get в основном читает — подумай какой Lock подойдёт
// Отдельный вопрос: что делать если нашли устаревшую запись? Можно ли её удалить здесь?
func (c *TTLCache[K, V]) Get(key K) (val V, ok bool) {
	c.mu.RLock()
	value, ok := c.items[key]
	if ok && time.Now().Before(value.expiry) {
		defer c.mu.RUnlock()
		return value.value, ok
	}
	c.mu.RUnlock()

	if !ok {
		return val, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok = c.items[key]
	if !ok || value.expiry.Before(time.Now()) {
		delete(c.items, key)
		return val, false
	}

	return value.value, true
}

// TODO: реализуй Delete
func (c *TTLCache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.items, key)
}

// TODO: реализуй Len — количество ЖИВЫХ (не устаревших) записей
func (c *TTLCache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	counter := 0
	curTime := time.Now()
	for _, v := range c.items {
		if v.expiry.After(curTime) {
			counter++
		}
	}

	return counter
}

// TODO: реализуй Cleanup — удаляет все устаревшие записи
func (c *TTLCache[K, V]) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	curTime := time.Now()
	for k, v := range c.items {
		if v.expiry.Before(curTime) {
			delete(c.items, k)
		}
	}
}

func main() {
	cache := NewTTLCache[string, int](100 * time.Millisecond)

	cache.Set("a", 1)
	cache.Set("b", 2)

	if v, ok := cache.Get("a"); ok {
		fmt.Printf("a = %d\n", v)
	}

	fmt.Printf("Len = %d\n", cache.Len()) // 2

	time.Sleep(150 * time.Millisecond)

	_, ok := cache.Get("a")
	fmt.Printf("a после TTL: ok=%v\n", ok) // false

	fmt.Printf("Len после TTL = %d\n", cache.Len()) // 0
}
