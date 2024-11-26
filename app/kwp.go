package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// API Keys (identifiers)
// https://kafka.apache.org/protocol.html#protocol_api_keys
type apiIndex int16

const (
	APIKeyProduce                 apiIndex = 0
	APIKeyApiVersions             apiIndex = 18
	APIKeyDescribeTopicPartitions apiIndex = 75
)

type supportedAPIs map[apiIndex]struct {
	minVersion, maxVersion int16
}
type app struct {
	supportedAPIs supportedAPIs
}

func newApp() *app {
	return &app{
		supportedAPIs: supportedAPIs{
			APIKeyApiVersions: {
				minVersion: 3,
				maxVersion: 4,
			},
			APIKeyDescribeTopicPartitions: {
				minVersion: 0,
				maxVersion: 0,
			},
		},
	}
}

type responseHeader struct {
	correlationID int32
}
type response struct {
	msgSize int32
	header  responseHeader
	body    io.WriterTo
}

// MarshalBinary serializes the response to Kafka wire protocol.
func (r response) MarshalBinary() ([]byte, error) {
	data := make([]byte, 0, r.msgSize)
	w := bytes.NewBuffer(data)

	_, err := r.WriteTo(w)
	if err != nil {
		return nil, err
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

	r.msgSize = int32(buf.Len())
	err = binary.Write(w, binary.BigEndian, r.msgSize)
	if err != nil {
		return 0, fmt.Errorf("write message size: %w", err)
	}
	_, err = w.Write(buf.Bytes())
	return int64(r.msgSize), err
}

type request struct {
	msgSize int32
	header  requestHeader
}

// requestHeader v2
type requestHeader struct {
	requestAPIKey     apiIndex     // The API key for the request.
	requestAPIVersion int16        // The version of the API for the request.
	correlationID     int32        // A unique ID for the request.
	clientID          *string      // The client ID for the request.
	tagBuffer         taggedFields // Optional tagged fields.
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

	buf := make([]byte, r.msgSize)
	nRead, err := io.ReadFull(rdr, buf)
	if err != nil {
		return int64(4 + nRead), fmt.Errorf("read full msg: %w", err)
	}

	r.header = requestHeader{}
	r.header.requestAPIKey = apiIndex(binary.BigEndian.Uint16(buf[:2]))
	r.header.requestAPIVersion = int16(binary.BigEndian.Uint16(buf[2:4]))
	r.header.correlationID = int32(binary.BigEndian.Uint32(buf[4:8]))

	return int64(4 + nRead), nil
}

type taggedField []byte

func (tf taggedField) WriteTo(w io.Writer) (int64, error) {
	// TODO:
	return 0, nil
}

// taggedFields or tag section begins with a number of tagged fields,
// serialized as a unsigned variable-length integer.
// If this number is 0, there are no tagged fields present.
// In that case, the tag section takes up only one byte.
// If the number of tagged fields is greater than zero,
// the tagged fields follow.
// They are serialized in ascending order of their tag.
// Each tagged field begins with a tag header.
type taggedFields []taggedField

func (tf taggedFields) WriteTo(w io.Writer) (int64, error) {
	// https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields#KIP482:TheKafkaProtocolshouldSupportOptionalTaggedFields-Serialization
	buf := make([]byte, 8)
	n := binary.PutUvarint(buf, uint64(len(tf)))
	if _, err := w.Write(buf[:n]); err != nil {
		return 0, fmt.Errorf("write number of tagged fields: %w", err)
	}
	nWritten := int64(n)
	for _, field := range tf {
		n, err := field.WriteTo(w)
		if err != nil {
			return nWritten, err
		}
		nWritten += n
	}
	return nWritten, nil
}

func (app *app) handleAPIVersionsRequest() func(resp *response, req *request) {
	minVersion := app.supportedAPIs[APIKeyApiVersions].minVersion
	maxVersion := app.supportedAPIs[APIKeyApiVersions].maxVersion
	apiKeys := make([]apiKey, 0, len(app.supportedAPIs))
	for key, api := range app.supportedAPIs {
		apiKeys = append(apiKeys, apiKey{
			index:      key,
			minVersion: api.minVersion,
			maxVersion: api.maxVersion,
		})
	}

	return func(resp *response, req *request) {
		requestedVer := req.header.requestAPIVersion

		if requestedVer > maxVersion || requestedVer < minVersion {
			resp.body = apiVersionsResponse{
				errorCode: APIVersionsErrUnsupportedVersion,
			}
			return
		}

		resp.body = apiVersionsResponse{
			apiKeys: apiKeys,
		}
	}
}

const (
	// Denotes the version of ApiVersions requested by the client is not supported by the broker.
	// Assume that your broker only supports versions 0 to 4.
	APIVersionsErrUnsupportedVersion = int16(35)
)

type apiKey struct {
	index        apiIndex
	minVersion   int16
	maxVersion   int16
	taggedFields byte // Unused
}

func (a apiKey) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.BigEndian, a.index); err != nil {
		return 0, fmt.Errorf("write api_key: %w", err)
	}
	if err := binary.Write(w, binary.BigEndian, a.minVersion); err != nil {
		return 2, fmt.Errorf("write min_version: %w", err)
	}
	if err := binary.Write(w, binary.BigEndian, a.maxVersion); err != nil {
		return 4, fmt.Errorf("write max_version: %w", err)
	}
	if err := binary.Write(w, binary.BigEndian, a.taggedFields); err != nil {
		return 4, fmt.Errorf("write tagged_fields: %w", err)
	}
	return 6, nil
}

type apiVersionsResponse struct {
	errorCode      int16                    //The top-level error code.
	apiKeys        compactArrayResp[apiKey] // 	The APIs supported by the broker.
	throttleTimeMs int32                    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	taggedFields   taggedFields             // Unused
}

func (a apiVersionsResponse) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.BigEndian, a.errorCode); err != nil {
		return 0, fmt.Errorf("write error code: %w", err)
	}

	nW, err := a.apiKeys.WriteTo(w)
	if err != nil {
		return 0, fmt.Errorf("write api_keys:= %w", err)
	}
	if err := binary.Write(w, binary.BigEndian, a.throttleTimeMs); err != nil {
		return 0, fmt.Errorf("write throttle_time_ms: %w", err)
	}

	nTagged, err := a.taggedFields.WriteTo(w)
	if err != nil {
		return 4, fmt.Errorf("write tagged_fields: %w", err)
	}
	return nW + 4 + nTagged, err
}

type topic struct {
	name      compactString
	tagBuffer taggedFields
}

func (c *topic) ReadFrom(r io.Reader) (int64, error) {
	c = &topic{}
	return c.name.ReadFrom(r)
}

type describeTopicPartitionsRequest struct {
	topics compactArrayReq[*topic]
}

// compactString contains a 32-bit unsigned varint representing the
// string's length + 1, followed by the string bytes.
type compactString string

func (c *compactString) ReadFrom(r io.Reader) (int64, error) {
	var buf bytes.Buffer
	nInt, err := io.CopyN(&buf, r, binary.MaxVarintLen32)
	if err != nil {
		return int64(nInt), fmt.Errorf("reading string length: %w", err)
	}
	strLenPlus1, _ := binary.Uvarint(buf.Bytes()[:nInt])
	if n, err := io.CopyN(&buf, r, nInt-int64(strLenPlus1)); err != nil {
		if !errors.Is(err, io.EOF) {
			return nInt + int64(n), fmt.Errorf("reading string contents: %w", err)
		}
	}
	*c = compactString(buf.Bytes()[nInt : uint64(nInt)+strLenPlus1-1])
	return int64(buf.Len()), nil
}

type compactArrayResp[T io.WriterTo] []T

func (c compactArrayResp[T]) WriteTo(w io.Writer) (int64, error) {
	buf := make([]byte, 8)
	n := binary.PutUvarint(buf, uint64(len(c)+1))
	if _, err := w.Write(buf[:n]); err != nil {
		return 0, fmt.Errorf("write number of tagged fields: %w", err)
	}
	nWritten := int64(n)

	for _, item := range c {
		n, err := item.WriteTo(w)
		if err != nil {
			return nWritten, err
		}
		nWritten += n
	}
	return nWritten, nil
}

type compactArrayReq[T io.ReaderFrom] []T

func (c *compactArrayReq[T]) ReadFrom(r io.Reader) (int64, error) {
	// var buf bytes.Buffer
	// nInt, err := io.CopyN(&buf, r, binary.MaxVarintLen64)
	// if err != nil {
	// 	if !errors.Is(err, io.EOF) {
	// 		return nInt, fmt.Errorf("reading array length: %w", err)
	// 	}
	// }

	rdr := bufio.NewReader(r)
	arrLen, err := binary.ReadUvarint(rdr)
	if err != nil {
		return int64(rdr.Buffered()), fmt.Errorf("reading array length: %w", err)
	}
	// TODO: Fix the read bytes count from Uvarint.
	n := 1 + rdr.Buffered()
	// Length has padding of + 1 to represent nulls.
	if arrLen == 0 {
		// Array is nil.
		return int64(n), nil
	}
	arrLen -= 1

	if arrLen == 0 {
		return int64(n), nil
	}

	items := make([]T, arrLen)

	for _, v := range items {
		nRead, err := v.ReadFrom(rdr)
		if err != nil {
			return int64(nRead), err
		}
		n += int(nRead)
	}
	*c = items
	return int64(n), nil
}
