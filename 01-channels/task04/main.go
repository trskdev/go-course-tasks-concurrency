// ============================================================
// Задача: Timeout & Select — первый ответ выигрывает  🟡 Middle
// ============================================================
//
// Реализуй функцию fastest(ctx, urls) которая:
//   - Параллельно запрашивает все переданные URL (mock)
//   - Возвращает первый успешный ответ
//   - Отменяет остальные запросы
//   - Возвращает ошибку если все запросы упали или истёк таймаут ctx
//
// Это классический паттерн "hedged requests" широко используемый в продакшне.
//
// Дополнительно реализуй withTimeout(d, fn):
//   - Запускает fn с таймаутом d
//   - Если fn не завершилась за d — возвращает ErrTimeout
//
// Проверь:
//   go test -race -v ./...

package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

var ErrTimeout = errors.New("таймаут истёк")
var ErrAllFailed = errors.New("все запросы завершились ошибкой")

type Result struct {
	URL  string
	Body string
}

type resultResponse struct {
	result Result
	err    error
}

type stringResponse struct {
	result string
	err    error
}

// mockFetch имитирует HTTP-запрос с случайной задержкой
func mockFetch(ctx context.Context, url string) (Result, error) {
	delay := time.Duration(50+rand.Intn(200)) * time.Millisecond

	select {
	case <-time.After(delay):
		if rand.Float64() < 0.2 { // 20% вероятность ошибки
			return Result{}, fmt.Errorf("%s: server error", url)
		}
		return Result{URL: url, Body: fmt.Sprintf("response from %s", url)}, nil
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

// TODO: реализуй fastest
// Подсказка: отмена ctx распространяется на все запросы автоматически — не заботься об явном завершении
func fastest(ctx context.Context, urls []string) (Result, error) {
	// TODO: реализуй
	size := len(urls)
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resChan := make(chan resultResponse, size)
	errCount := 0

	for _, url := range urls {
		go func(url string) {
			res, err := mockFetch(cancelCtx, url)
			resp := resultResponse{result: res, err: err}

			resChan <- resp
		}(url)
	}

	for {
		select {
		case res := <-resChan:
			if res.err == nil {
				return res.result, nil
			}

			errCount++
			if errCount == size {
				return Result{}, ErrAllFailed
			}
		case <-ctx.Done():
			return Result{}, ErrTimeout
		}
	}
}

// TODO: реализуй withTimeout
func withTimeout(d time.Duration, fn func() (string, error)) (string, error) {
	// TODO
	ch := make(chan stringResponse, 1)
	timer := time.NewTimer(d)
	defer timer.Stop()

	go func() {
		result, err := fn()
		ch <- stringResponse{result: result, err: err}
	}()

	select {
	case <-timer.C:
		return "", ErrTimeout
	case res := <-ch:
		return res.result, res.err
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	urls := []string{
		"https://api1.example.com",
		"https://api2.example.com",
		"https://api3.example.com",
	}

	result, err := fastest(ctx, urls)
	if err != nil {
		fmt.Println("Ошибка:", err)
		return
	}
	fmt.Printf("Быстрейший ответ от %s: %s\n", result.URL, result.Body)
}
