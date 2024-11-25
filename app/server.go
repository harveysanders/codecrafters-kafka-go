package main

import (
	"fmt"
	"log"
	"net"
	"os"
)

type server struct {
}

func (s *server) ListenAndServe() error {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		return fmt.Errorf("failed to bind to port 9092: %w", err)

	}
	defer func() { _ = l.Close() }()

	return s.Serve(l)
}

func (s *server) Serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handle(conn)
	}
}

func handle(conn net.Conn) {
	req := &request{}

	nRead, err := req.ReadFrom(conn)
	if err != nil {
		fmt.Printf("Error reading request: %v\n", err)
		return
	}
	fmt.Printf("read %d bytes from request\n", nRead)

	resp := response{
		header: responseHeader{
			correlationID: req.header.correlationID,
		},
	}
	switch req.header.requestAPIKey {
	case APIKeyApiVersions:
		handleAPIVersionsRequest(&resp, req)
	default:

	}

	n, err := resp.WriteTo(conn)
	if err != nil {
		fmt.Printf("Error writing response %v\n", err)
		return
	}

	fmt.Printf("wrote response of len %d\n", n)
}

func main() {
	srv := server{}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
