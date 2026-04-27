//usr/bin/env go run "$0" "$@"; exit "$?"

package main

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
)

type mqWebServer struct {
	topics map[string]chan string
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
			method, path, query, err := reqParse(conn)
			if err == nil {
				s.process(conn, method, path, query)
			} else {
				println("error on parsing request:", err.Error())
			}
			conn.Close()
		} else {
			println("error on accept:", err.Error())
		}
	}
}

func (s *mqWebServer) process(conn net.Conn, method, path string, query url.Values) {
	msg := query.Get("v")
	switch method {
	case "PUT":
		if msg == "" {
			respWrite(conn, http.StatusBadRequest, "")
			return
		}
		c, ok := s.topics[path]
		if !ok {
			c = make(chan string, 16)
			s.topics[path] = c
		}
		c <- msg
		respWrite(conn, http.StatusOK, "")
	case "GET":
		msg := ""
		c := s.topics[path]
		if c != nil {
			select {
			case m := <-c:
				msg = m
			default:
			}
		}
		if msg != "" {
			respWrite(conn, http.StatusOK, msg)
		} else {
			respWrite(conn, http.StatusNotFound, "")
		}
	default:
		respWrite(conn, http.StatusMethodNotAllowed, "")
	}
}

func reqParse(conn net.Conn) (string, string, url.Values, error) {
	b := make([]byte, 1024)
	n, err := conn.Read(b)
	if err != nil {
		return "", "", nil, err
	}
	b = b[:n]
	var method, path, httpVer string
	fmt.Sscanf(string(b), "%s %s %s", &method, &path, &httpVer)
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	u, err := url.Parse(path)
	if err != nil {
		return "", "", nil, err
	}
	return method, u.Path, u.Query(), nil
}

func respWrite(conn net.Conn, status int, data string) {
	b := []byte(data)
	text := fmt.Sprintf("HTTP/1.1 %d %s\r\n", status, http.StatusText(status))
	text += fmt.Sprintf("Content-Length: %d\r\n\r\n", len(b))
	conn.Write([]byte(text))
	conn.Write(b)
}

func newMqWebServer() *mqWebServer {
	return &mqWebServer{topics: map[string]chan string{}}
}

func main() {
	port := "8000"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}
	srv := newMqWebServer()
	srv.ListenAndServe(":" + port)
}
