//usr/bin/env go run "$0" "$@"; exit "$?"

package main

// this implementation doesn't use web-server from net/http but instead
// I implemented crude http-request parsing myself, this allows to avoid
// creating separate goroutines for multiple clients waiting on the same topic
// (as they anyway need to be processed in FIFO order)
// instead we parallelize work using specified amount of parallel workers, it's fast :)

import (
	"bytes"
	"errors"
	"fmt"
	"hash/maphash"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

const readBufMax = 2048 // based upon popular limit on URL length (2000 chars)
var noNewlineError = errors.New("No newline found when parsing http request")

type mqWebServer struct {
	readBuf  []byte
	workers  []*worker
	hashSeed maphash.Seed
}

type worker struct {
	ch      chan *request
	topics  map[string]*queue[string]
	waiting map[string]*queue[*request]
}

type request struct {
	conn     net.Conn
	method   string
	path     string
	msg      string
	deadline int64
}

type queue[E any] struct {
	body []E
	base int
}

func newQueue[E any]() *queue[E] {
	return &queue[E]{body: make([]E, 0, 8), base: 0}
}

func (q *queue[E]) add(elem E) {
	q.body = append(q.body, elem)
}

func (q *queue[E]) pop() E {
	res := q.body[0]
	q.body = q.body[1:]
	q.base++
	if q.base > len(q.body) {
		old := q.body
		q.body = make([]E, len(old))
		copy(q.body, old)
		q.base = 0
	}
	return res
}

func (s *mqWebServer) ListenAndServe(port string) {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		println("error on listen:", err.Error())
		return
	}
	for {
		conn, err := listener.Accept()
		if err == nil {
			req, err := s.reqParse(conn)
			if err == nil {
				h := int(maphash.String(s.hashSeed, req.path) & 0xFFFFFFFF)
				workerIndex := h % len(s.workers)
				dbg("req", req.method, req.path, "to worker #", workerIndex)
				s.workers[workerIndex].ch <- req
			} else {
				println("error on parsing request:", err.Error())
				conn.Close()
			}
		} else {
			println("error on accept:", err.Error())
		}
	}
}

func (w *worker) work() {
	lastEvictTs := time.Now().UnixMilli()
	for {
		select {
		case req := <-w.ch:
			w.process(req)
		default:
			time.Sleep(30 * time.Millisecond)
		}
		ts := time.Now().UnixMilli()
		if lastEvictTs+300 < ts {
			w.evict(ts)
			lastEvictTs = ts
		}
	}
}

func (w *worker) process(req *request) {
	switch req.method {
	case "PUT":
		if req.msg == "" {
			respWrite(req.conn, http.StatusBadRequest, "")
			return
		}
		waitQueue := w.waiting[req.path]
		if waitQueue != nil {
			dbg("waiter found on", req.path)
			client := waitQueue.pop()
			if len(waitQueue.body) == 0 {
				delete(w.waiting, req.path)
			}
			respWrite(client.conn, http.StatusOK, req.msg+"\n")
			respWrite(req.conn, http.StatusOK, "")
			return
		}
		msgQueue := w.topics[req.path]
		if msgQueue == nil {
			msgQueue = newQueue[string]()
			w.topics[req.path] = msgQueue
		}
		msgQueue.add(req.msg)
		respWrite(req.conn, http.StatusOK, "")
	case "GET":
		msgQueue := w.topics[req.path]
		if msgQueue != nil {
			msg := msgQueue.pop()
			if len(msgQueue.body) == 0 {
				delete(w.topics, req.path)
			}
			respWrite(req.conn, http.StatusOK, msg+"\n")
			return
		}
		if req.deadline > 0 {
			waitQueue := w.waiting[req.path]
			if waitQueue == nil {
				waitQueue = newQueue[*request]()
				w.waiting[req.path] = waitQueue
			}
			waitQueue.add(req)
			dbg("waiting on", req.path)
		} else {
			respWrite(req.conn, http.StatusNotFound, "")
		}
	default:
		respWrite(req.conn, http.StatusMethodNotAllowed, "")
	}
}

func (w *worker) evict(ts int64) {
	for topic, clients := range w.waiting {
		for len(clients.body) > 0 && clients.body[0].deadline < ts {
			respWrite(clients.pop().conn, http.StatusNotFound, "")
		}
		if len(clients.body) == 0 {
			delete(w.waiting, topic)
		}
	}
}

func (s *mqWebServer) reqParse(conn net.Conn) (*request, error) {
	n, err := conn.Read(s.readBuf)
	if err != nil {
		return nil, err
	}
	crpos := bytes.IndexByte(s.readBuf[:n], '\n')
	if crpos == -1 {
		return nil, noNewlineError
	}
	data := s.readBuf[:n]
	var method, path, httpVer string
	fmt.Sscanf(string(data), "%s %s %s", &method, &path, &httpVer)
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	req := &request{
		conn:     conn,
		method:   method,
		path:     u.Path,
		msg:      u.Query().Get("v"),
		deadline: 0,
	}
	if to, err := strconv.Atoi(u.Query().Get("timeout")); err == nil {
		req.deadline = time.Now().UnixMilli() + int64(to)*1000
	}
	return req, nil
}

func respWrite(conn net.Conn, status int, data string) {
	b := []byte(data)
	text := fmt.Sprintf("HTTP/1.1 %d %s\r\n", status, http.StatusText(status))
	text += fmt.Sprintf("Content-Length: %d\r\n\r\n", len(b))
	conn.Write([]byte(text))
	conn.Write(b)
	conn.Close()
}

func newMqWebServer() *mqWebServer {
	numWorkers, err := strconv.Atoi(os.Getenv("NUM_WORKERS"))
	if err != nil {
		println("use NUM_WORKERS env variable to set number of workers")
		numWorkers = 1
	}
	workers := make([]*worker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = &worker{
			ch:      make(chan *request),
			topics:  map[string]*queue[string]{},
			waiting: map[string]*queue[*request]{},
		}
		go workers[i].work()
	}
	return &mqWebServer{
		readBuf:  make([]byte, readBufMax),
		hashSeed: maphash.MakeSeed(),
		workers:  workers,
	}
}

func dbg(args ...any) {
	if os.Getenv("DBG_EN") != "" {
		fmt.Println(args...)
	}
}

func main() {
	port := "8000"
	if len(os.Args) > 1 {
		port = os.Args[1]
	} else {
		println("To change the default port set it as a first command line parameter")
	}
	srv := newMqWebServer()
	println("Starting at port =", port, "with num workers = ", len(srv.workers))
	srv.ListenAndServe(":" + port)
}
