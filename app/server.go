package main

import (
	"fmt"
	"net"
	"os"
)

func handle(conn net.Conn) {
	req := &request{}

	nRead, err := req.ReadFrom(conn)
	if err != nil {
		fmt.Printf("Error reading request: %v\n", err)
		return
	}
	fmt.Printf("read %d bytes from request\n", nRead)

	resp := response{
		header: responseHeader{correlationID: req.header.correlationID},
	}
	switch req.header.requestAPIKey {
	case APIKeyApiVersions:
		resp.body = ApiVersionsResponse{
			errorCode: APIVersionsErrUnsupportedVersion,
		}
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
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	defer func() {
		_ = l.Close()
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handle(conn)
	}

}
