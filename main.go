package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sort"
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

	reqChan := make(chan *fasthttp.Request, *nWorkers)

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
	client := &fasthttp.HostClient{Addr: *target}

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

func ParseRequests(fileName string) []fasthttp.Request {

	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal("can't open ammo file: ", err)
	}
	defer f.Close()

	var bodies [][]byte
	r := bufio.NewReader(f)

	for {
		bulletHeader, err := r.ReadSlice('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal("can't read bullet header: ", err)
		}
		bulletLen, err := ParseInt(bulletHeader)
		if err != nil {
			log.Fatal("can't parse bullet length: ", err)
		}
		buf := make([]byte, bulletLen)
		_, err = io.ReadFull(r, buf)
		if err != nil {
			log.Fatal("can't read request from file: ", err)
		}
		bodies = append(bodies, buf)
	}

	var (
		requests  = make([]fasthttp.Request, len(bodies))
		reqReader = bufio.NewReader(bytes.NewReader([]byte{}))
	)

	for i, body := range bodies {
		reqReader.Reset(bytes.NewReader(body))
		err = requests[i].Read(reqReader)
		if err != nil {
			log.Print(body)
			log.Fatal("can't parse request: ", err)
		}
	}

	return requests
}

func CycleRequests(ctx context.Context, requests []fasthttp.Request, ch chan *fasthttp.Request) {
	for {
		for i := 0; i < len(requests); i++ {
			req := fasthttp.AcquireRequest()
			requests[i].CopyTo(req)
			select {
			case ch <- req:
			case <-ctx.Done():
				return
			}
		}
	}
}

func SendRequests(ctx context.Context, client *fasthttp.HostClient,
	reqChan chan *fasthttp.Request, lance chan time.Duration,
	stat chan time.Duration, timeout time.Duration) {

	var resp fasthttp.Response

	for {
		select {
		case req := <-reqChan:
			if req == nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case t := <-lance:
				if t == 0 {
					return
				}
			}
			t0 := time.Now()
			if err := client.DoTimeout(req, &resp, timeout); err != nil {
				log.Fatal("request failed: ", err)
			}
			stat <- time.Since(t0)
			fasthttp.ReleaseRequest(req)
			resp.Reset()
		case <-ctx.Done():
			return
		}
	}

}

type Duration []time.Duration

func (p Duration) Len() int           { return len(p) }
func (p Duration) Less(i, j int) bool { return p[i] < p[j] }
func (p Duration) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (p Duration) Quantile(q float64) time.Duration {
	return p[int(float64(len(p))*q)]
}

func (p Duration) Avg() time.Duration {
	return p.Sum() / time.Duration(len(p))
}

func (p Duration) Sum() time.Duration {
	var sum time.Duration
	for _, i := range p {
		sum += i
	}
	return sum
}

func WriteStat(ctx context.Context, ch chan time.Duration) {

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var (
		fullstat, stat Duration
		done           bool
	)

	for !done {
		select {
		case <-ctx.Done():
			done = true
		case i := <-ch:
			if i == 0 {
				done = true
				continue
			}
			stat = append(stat, i)
		case <-ticker.C:
			if len(stat) == 0 {
				log.Printf("RPS=0")
				continue
			}
			sort.Sort(stat)
			log.Printf("RPS=%d min=%s p50=%s p90=%s max=%s avg=%s", len(stat), stat[0],
				stat.Quantile(0.5), stat.Quantile(0.9),
				stat[len(stat)-1], stat.Avg())
			fullstat = append(fullstat, stat...)
			stat = stat[:0]
		}
	}

	log.Printf("Processing the inflight requests...")

	t0 := time.Now()
	for i := range ch {
		stat = append(stat, i)
	}

	sort.Sort(stat)
	log.Printf("Tail processed in %s: requests=%d min=%s p50=%s p90=%s max=%s avg=%s",
		time.Since(t0), len(stat), stat[0],
		stat.Quantile(0.5), stat.Quantile(0.9),
		stat[len(stat)-1], stat.Avg())

	fullstat = append(fullstat, stat...)
	sort.Sort(fullstat)
	log.Printf("Summary: requests=%d min=%s p50=%s p90=%s max=%s avg=%s",
		len(fullstat), fullstat[0],
		fullstat.Quantile(0.5), fullstat.Quantile(0.9),
		fullstat[len(fullstat)-1],
		fullstat.Avg())

	log.Printf("Total score: %s", fullstat.Sum())

}
