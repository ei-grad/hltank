package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"time"

	"github.com/ei-grad/lancer"
	"github.com/valyala/fasthttp"
)

func main() {

	var (
		ammoFileName = flag.String("ammo", "", ".ammo file name")
		target       = flag.String("target", "127.0.0.1:80", "target")
		nWorkers     = flag.Int("w", 1000, "number of workers")
		lowRPS       = flag.Float64("low", 200, "RPS to start")
		highRPS      = flag.Float64("high", 2000, "RPS to finish")
		duration     = flag.Duration("d", time.Second*120, "test duration")
		cycle        = flag.Bool("cycle", false, "cycle requests if needed (when not specified - exit with error if there is not enought requests to produce the specified load)")
		timeout      = flag.Duration("timeout", time.Second*2, "request timeout")
	)

	flag.Parse()

	var wg sync.WaitGroup

	lance := make(chan time.Duration)
	ctx, cancel := context.WithCancel(context.Background())

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		<-interrupt
		cancel()
	}()

	reqChan := make(chan *fasthttp.Request)

	// load requests to RAM
	requests := ParseRequests(*ammoFileName)

	if !*cycle {
		neededRequestsCount := int((*lowRPS + (*highRPS-*lowRPS)/2) * duration.Seconds())
		if len(requests) < neededRequestsCount {
			log.Fatalf("Ammo file contains %d requests, but %d requests are needed to produce %d->%d load during %s. Generate more bullets or use -cycle if requests are idempotempt.",
				len(requests), neededRequestsCount, int(*lowRPS), int(*highRPS), duration)
		}
	}

	statChan := make(chan time.Duration)
	client := &fasthttp.HostClient{
		Addr:     *target,
		MaxConns: *nWorkers,
	}

	reqCtx, reqCancel := context.WithCancel(ctx)

	wg.Add(1)
	if *cycle {
		go func() {
			defer wg.Done()
			defer close(reqChan)
			CycleRequests(reqCtx, requests, reqChan)
		}()
	} else {
		go func() {
			defer wg.Done()
			defer close(reqChan)
			for i := range requests {
				select {
				case reqChan <- &requests[i]:
				case <-reqCtx.Done():
					return
				}
			}
			log.Print("all requests sent")
		}()
	}

	var wgStat sync.WaitGroup
	wgStat.Add(1)
	go func() {
		defer wgStat.Done()
		WriteStat(ctx, statChan)
	}()

	var wgWorkers sync.WaitGroup
	wgWorkers.Add(*nWorkers)
	for i := 0; i < *nWorkers; i++ {
		go func() {
			defer wgWorkers.Done()
			SendRequests(ctx, client, reqChan, lance, statChan, *timeout)
		}()
	}

	runtime.Gosched()

	err := lancer.Linear(ctx, lance, *lowRPS, *highRPS, *duration)
	if err != nil {
		log.Print("lancer error: ", err)
	}

	close(lance)
	reqCancel()
	wgWorkers.Wait()

	close(statChan)
	wgStat.Wait()

}
