package main

import (
	"context"
	"log"
	"sort"
	"time"
)

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

	if len(stat) > 0 {
		sort.Sort(stat)
		log.Printf("Tail processed in %s: requests=%d min=%s p50=%s p90=%s max=%s avg=%s",
			time.Since(t0), len(stat), stat[0],
			stat.Quantile(0.5), stat.Quantile(0.9),
			stat[len(stat)-1], stat.Avg())
	} else {
		log.Printf("No tail requests, hm...")
	}

	if len(fullstat) == 0 {
		log.Printf("No requests - no statistics.")
		return
	}

	fullstat = append(fullstat, stat...)
	sort.Sort(fullstat)
	log.Printf("Summary: requests=%d min=%s p50=%s p90=%s max=%s avg=%s",
		len(fullstat), fullstat[0],
		fullstat.Quantile(0.5), fullstat.Quantile(0.9),
		fullstat[len(fullstat)-1],
		fullstat.Avg())

	log.Printf("Total score: %f", fullstat.Sum().Seconds())

}
