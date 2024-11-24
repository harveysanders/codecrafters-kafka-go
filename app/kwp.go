package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// API Keys (identifiers)
const (
	APIKeyProduce     = 0
	APIKeyApiVersions = 18
)

type responseHeader struct {
	correlationID int32
}
type response struct {
	msgSize int32
	header  responseHeader
	body    io.WriterTo
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
	// Need internal buffer to know how long the message is before
	// writing to w.
	var buf bytes.Buffer

	err = binary.Write(&buf, binary.BigEndian, r.header.correlationID)
	if err != nil {
		return 0, fmt.Errorf("write header: %w", err)
	}

	_, err = r.body.WriteTo(&buf)
	if err != nil {
		return 0, fmt.Errorf("write body: %w", err)
	}

	r.msgSize = int32(buf.Len()) + 4 // include int32 for msgSize
	err = binary.Write(w, binary.BigEndian, r.msgSize)
	if err != nil {
		return 0, fmt.Errorf("write message size: %w", err)
	}
	nWritten, err := w.Write(buf.Bytes())
	return int64(nWritten), err
}

type request struct {
	msgSize int32
	header  requestHeader
}

// requestHeader v2
type requestHeader struct {
	requestAPIKey     int16    // The API key for the request.
	requestAPIVersion int16    // The version of the API for the request.
	correlationID     int32    // A unique ID for the request.
	clientID          *string  // The client ID for the request.
	tagBuffer         []string // Optional tagged fields.
}

func (r *request) UnmarshalBinary(data []byte) error {
	buf := bytes.NewReader(data)
	_, err := r.ReadFrom(buf)
	return err
}

func (r *request) ReadFrom(rdr io.Reader) (n int64, err error) {
	if err := binary.Read(rdr, binary.BigEndian, &r.msgSize); err != nil {
		return 0, fmt.Errorf("read message size: %w", err)
	}

	r.header = requestHeader{}
	if err := binary.Read(rdr, binary.BigEndian, &r.header.requestAPIKey); err != nil {
		return 4, fmt.Errorf("read request API key: %w", err)
	}

	if err := binary.Read(rdr, binary.BigEndian, &r.header.requestAPIVersion); err != nil {
		return 4 + 2, fmt.Errorf("read request API version: %w", err)
	}

	if err := binary.Read(rdr, binary.BigEndian, &r.header.correlationID); err != nil {
		return 4 + 2 + 2, fmt.Errorf("read correlation ID: %w", err)
	}
	return 4 + 2 + 2 + 4, nil
}

const (
	// Denotes the version of ApiVersions requested by the client is not supported by the broker.
	// Assume that your broker only supports versions 0 to 4.
	APIVersionsErrUnsupportedVersion = int16(35)
)

type apiKey struct {
	val        int16
	minVersion int16
	maxVersion int16
}

func (a apiKey) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.BigEndian, a.val); err != nil {
		return 0, fmt.Errorf("write api_key: %w", err)
	}
	if err := binary.Write(w, binary.BigEndian, a.minVersion); err != nil {
		return 2, fmt.Errorf("write min_version: %w", err)
	}
	if err := binary.Write(w, binary.BigEndian, a.maxVersion); err != nil {
		return 4, fmt.Errorf("write max_version: %w", err)
	}
	return 6, nil
}

type ApiVersionsResponse struct {
	errorCode int16
	apiKeys   []apiKey
}

func (a ApiVersionsResponse) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.BigEndian, a.errorCode); err != nil {
		return 0, fmt.Errorf("write error code: %w", err)
	}
	apiKeySize := 3 * 2 // 3 int16 fields
	data := make([]byte, 0, len(a.apiKeys)*apiKeySize)
	buf := bytes.NewBuffer(data)
	for _, apiKey := range a.apiKeys {
		_, err := apiKey.WriteTo(buf)
		if err != nil {
			return 0, err
		}
	}

	nW, err := w.Write(buf.Bytes())
	return int64(nW), err
}
