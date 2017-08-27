package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"time"

	"github.com/valyala/fasthttp"
)

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
