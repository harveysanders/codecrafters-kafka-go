package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

type responseHeader struct {
	correlationID int32
}
type response struct {
	msgSize int32
	header  responseHeader
}

// MarshalBinary serializes the response to Kafka wire protocol.
// 4 bytes - message size
// 4 bytes - header
func (r response) MarshalBinary() ([]byte, error) {
	data := make([]byte, 0, 8)
	w := bytes.NewBuffer(data)

	err := binary.Write(w, binary.BigEndian, r.msgSize)
	if err != nil {
		return w.Bytes(), fmt.Errorf("write message size: %w", err)
	}
	err = binary.Write(w, binary.BigEndian, r.header.correlationID)
	if err != nil {
		return w.Bytes(), fmt.Errorf("write header: %w", err)
	}
	return w.Bytes(), nil
}

func (r response) WriteTo(w io.Writer) (n int64, err error) {
	err = binary.Write(w, binary.BigEndian, r.msgSize)
	if err != nil {
		return 0, fmt.Errorf("write message size: %w", err)
	}
	err = binary.Write(w, binary.BigEndian, r.header.correlationID)
	if err != nil {
		return 4, fmt.Errorf("write header: %w", err)
	}
	return 8, nil
}

func handle(conn net.Conn) {
	resp := response{
		header: responseHeader{correlationID: 7},
	}

	n, err := resp.WriteTo(conn)
	if err != nil {
		fmt.Printf("Error writing response %v\n", err)
		os.Exit(1)
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
