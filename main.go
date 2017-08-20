package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"io"
	"log"
	"os"

	"github.com/valyala/fasthttp"
)

var (
	errEmptyInt               = errors.New("empty integer")
	errUnexpectedFirstChar    = errors.New("unexpected first char found. Expecting 0-9")
	errUnexpectedTrailingChar = errors.New("unexpected traling char found. Expecting 0-9")
	errTooLongInt             = errors.New("too long int")
)

var maxIntChars = 10

func ParseInt(b []byte) (int, error) {
	n := len(b)
	if n == 0 {
		return 0, errEmptyInt
	}
	var v int
	for i := 0; i < n; i++ {
		c := b[i]
		if c == ' ' {
			return v, nil
		}
		k := c - '0'
		if k > 9 {
			if i == 0 {
				return 0, errUnexpectedFirstChar
			}
			return 0, errUnexpectedTrailingChar
		}
		if i >= maxIntChars {
			return 0, errTooLongInt
		}
		v = 10*v + int(k)
	}
	return v, nil
}

var pool = &SizedBufferPool{}

func main() {

	ammoFileName := flag.String("ammo", "", ".ammo file name")
	target := flag.String("target", "127.0.0.1:80", "target")
	nWorkers := flag.Int("w", 32, "number of connections")

	flag.Parse()

	f, err := os.Open(*ammoFileName)
	if err != nil {
		log.Fatal("can't open ammo file:", err)
	}
	defer f.Close()

	for {
		r := bufio.NewReader(f)

		requests := make(chan []byte)
		defer close(requests)

		for i := 0; i < *nWorkers; i++ {
			go worker(*target, requests)
		}

		for {
			bulletHeader, err := r.ReadSlice('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal("can't read bullet header:", err)
			}
			bulletLen, err := ParseInt(bulletHeader)
			if err != nil {
				log.Fatal("can't parse bullet length:", err)
			}
			buf := pool.Get(bulletLen)
			_, err = io.ReadFull(r, buf)
			if err != nil {
				log.Fatal("can't read request from file:", err)
			}
			requests <- buf
		}
		f.Seek(0, 0)
	}

}

func worker(target string, requests chan []byte) {

	var (
		client    = &fasthttp.HostClient{Addr: target}
		resp      fasthttp.Response
		req       fasthttp.Request
		reqReader = bufio.NewReader(bytes.NewReader([]byte{}))
	)

	for b := range requests {
		// XXX: we need MOAR bytes.NewReader's! SIC!
		// TODO: write custom reader
		reqReader.Reset(bytes.NewReader(b))
		err := req.Read(reqReader)
		pool.Put(b)
		if err != nil {
			log.Fatal("can't read request from buffer:", err)
		}
		err = client.Do(&req, &resp)
		if err != nil {
			log.Fatal("request failed:", err)
		}
		req.Reset()
		resp.Reset()
	}
}
