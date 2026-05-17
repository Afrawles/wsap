package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type result struct {
	timeSpent time.Duration
	statusCode int
	err error
}

type Config struct {
	URL string
	N uint16 // no. requests to send in total
	C uint16 //no. requests to send simultaneously
	Method string
}

type job struct {
	url string
	method string
}

func worker(ctx context.Context, client *http.Client, jobs <-chan job, results chan<- result, wg *sync.WaitGroup)  {
	defer wg.Done()
	for {
		select {
		case <- ctx.Done():
			return
		case j, ok := <- jobs:
			if !ok {
				return
			}

			req, err := http.NewRequestWithContext(ctx, j.method, j.url, nil)
			if err != nil {
				results <- result{err: err}
				continue
			}

			start := time.Now()
			resp, err := client.Do(req)
			duration := time.Since(start)

			if err != nil {
				results <- result{timeSpent: duration, err: err}
				continue
			}

			resp.Body.Close()
			results <- result{timeSpent: duration, statusCode: resp.StatusCode}

		}
	}
}

func aggResults(results <-chan result) {
	var successes, failures int
	var totalTime time.Duration

	for r := range results {
		if r.err != nil || r.statusCode >= 400 {
			failures++
		} else {
			successes++
			totalTime += r.timeSpent
		}

	}

	fmt.Printf(">>>>Results<<<<<\n")
	fmt.Printf("Successful Requests: %d\n", successes)
	fmt.Printf("Failed Requests: %d\n", failures)
	if successes > 0 {
		fmt.Printf("Average Latency: %v\n", totalTime/time.Duration(successes))
	}
}

func main() {
	cfg := Config{
		URL: "https://httpbin.org",
		N: 20,
		C: 5,
		Method: http.MethodGet,
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns: int(cfg.C),
			MaxIdleConnsPerHost: int(cfg.C),
		},
	}

	//  TODO: use signal
	ctx := context.WithoutCancel(context.Background())

	jobs := make(chan job, cfg.N)
	results := make(chan result, cfg.N)

	var wg sync.WaitGroup
	
	for w := 1; w < int(cfg.C); w++ {
		wg.Add(1)
		go worker(ctx, client, jobs, results, &wg)
	}

	for j := 0; j < int(cfg.N); j++ {
		jobs <- job{url: cfg.URL, method: cfg.Method}
	}

	close(jobs)
	wg.Wait()

	close(results)

	aggResults(results)

}
