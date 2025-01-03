package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/codecrafters-io/kafka-starter-go/app/metadata"
	"github.com/google/uuid"
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
	supportedAPIs supportedAPIs     // List of supported APIs.
	metadataFile  io.Reader         // Topic metadata log contents.
	metadataSrv   *metadata.Service // Cluster topic metadata service.
}

type option func(*app)

// WithMetadataLogFile sets and loads the cluster's topic's log file.
func WithMetadataLogFile(r io.Reader) func(a *app) {
	return func(a *app) {
		a.metadataSrv = &metadata.Service{}
		if err := a.metadataSrv.Load(r); err != nil {
			panic(fmt.Sprintf("load metadata %v", err))
		}
		a.metadataFile = r
	}
}

func newApp(opts ...option) *app {
	app := &app{
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
		metadataSrv: &metadata.Service{},
	}

	for _, opt := range opts {
		opt(app)
	}
	return app
}

type version string

const (
	// v0 headers and v1 headers are nearly identical,
	// the only difference is that v1 contains an additional tag_buffer field at the end.
	headerVersion0 version = "v0"
	headerVersion1 version = "v1"
)

type responseHeader struct {
	version       version
	correlationID int32
	tagBuffer     taggedFields
}

func (h responseHeader) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.BigEndian, h.correlationID); err != nil {
		return 0, fmt.Errorf("write correlation ID: %w", err)
	}
	if h.version == headerVersion0 {
		// Do not write tag buffer for v0
		return 4, nil
	}
	if _, err := h.tagBuffer.WriteTo(w); err != nil {
		return 0, fmt.Errorf("write tag buffer: %w", err)
	}
	return 0, nil
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

	_, err = r.header.WriteTo(&buf)
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
	body    io.Reader
}

// requestHeader v2
type requestHeader struct {
	requestAPIKey     apiIndex        // The API key for the request.
	requestAPIVersion int16           // The version of the API for the request.
	correlationID     int32           // A unique ID for the request.
	clientID          *nullableString // The client ID for the request.
	tagBuffer         taggedFields    // Optional tagged fields.
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
	clientID := nullableString("")
	nClientID, err := clientID.ReadFrom(bytes.NewReader(buf[8:]))
	if err != nil {
		return int64(4 + nRead), fmt.Errorf("read client ID: %w", err)
	}
	r.header.clientID = &clientID
	offset := 8 + nClientID + 1 // 1 byte for tagged fields length
	r.body = bytes.NewReader(buf[offset:])

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

func (app *app) handleDescribeTopicPartitionsRequest() func(resp *response, req *request) {
	minVersion := app.supportedAPIs[APIKeyDescribeTopicPartitions].minVersion
	maxVersion := app.supportedAPIs[APIKeyDescribeTopicPartitions].maxVersion
	return func(resp *response, req *request) {
		requestedVer := req.header.requestAPIVersion

		if requestedVer > maxVersion || requestedVer < minVersion {
			resp.body = apiVersionsResponse{
				errorCode: APIVersionsErrUnsupportedVersion,
			}
			return
		}

		// Read request from body
		br := bufio.NewReader(req.body)
		dtpReq := describeTopicPartitionsRequest{}
		_, err := dtpReq.ReadFrom(br)
		if err != nil {
			log.Printf("Error reading describe topic partitions request: %v\n", err)
			return
		}

		// Look up topic (only one for now)
		meta, err := app.findTopicMeta(string(dtpReq.topics[0].name))
		if err != nil {
			log.Printf("Error finding topic metadata: %v\n", err)
			if !errors.Is(err, metadata.ErrNotFound) {
				// TODO: Error response
				return
			}
		}

		partitions := make([]partition, 0, len(meta.Partitions))
		for _, v := range meta.Partitions {
			partitions = append(partitions, partition{
				partitionIndex: v.Index,
			})
		}
		respBody := describeTopicPartitionsResponse{
			topics: []topicResponse{
				{
					topicID:    meta.ID,
					name:       compactString(meta.Name),
					partitions: partitions,
				},
			},
		}

		resp.body = respBody
	}
}

const (
	ErrUnknownTopicOrPartition int16 = 3
)

func (a *app) findTopicMeta(name string) (*metadata.TopicMeta, error) {
	res, err := a.metadataSrv.FindTopicMeta(name)
	if err != nil {
		return nil, fmt.Errorf("metadataSrv.FindTopicMeta with name %q: %w", name, err)
	}

	return res, nil
}

type topics []topic

// ReadFrom reads the topics from the reader. The first bytes are
// the length of the array, serialized as a 32-bit unsigned variable-length
// integer. The following bytes are the topics.
// If the length is 0, the array is nil. If the length field is 1, the array
// length is 0 and so on.
func (t *topics) ReadFrom(r io.Reader) (int64, error) {
	length, n, err := readCompactArrayLen(r)
	if err != nil {
		return 0, fmt.Errorf("reading topics length: %w", err)
	}

	if length == 0 {
		// Array is nil.
		*t = nil
		return n, nil
	}
	length -= 1
	topics := make([]topic, 0, length)

	for i := 0; i < int(length); i++ {
		topic := topic{}
		nRead, err := topic.ReadFrom(r)
		if err != nil {
			return n, fmt.Errorf("reading topic %d: %w", i, err)
		}
		n += nRead
		topics = append(topics, topic)
	}
	*t = topics
	return n, nil
}

func readCompactArrayLen(r io.Reader) (len uint64, n int64, err error) {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	arrLen, err := binary.ReadUvarint(br)
	if err != nil {
		return 0, 0, fmt.Errorf("reading array length: %w", err)
	}
	// TODO: Fix the read bytes count from Uvarint.
	// https://gophers.slack.com/archives
	return arrLen, 1, nil
}

type topic struct {
	name      compactString
	tagBuffer taggedFields
}

func (c *topic) ReadFrom(r io.Reader) (int64, error) {
	*c = topic{}
	n, err := c.name.ReadFrom(r)
	if err != nil {
		return n, fmt.Errorf("read topic name: %w", err)
	}
	return n, nil
}

type describeTopicPartitionsRequest struct {
	topics topics
}

func (d *describeTopicPartitionsRequest) ReadFrom(r io.Reader) (int64, error) {
	topics := topics{}
	n, err := topics.ReadFrom(r)
	d.topics = topics
	return n, err
}

// Represents a sequence of characters or null. For non-null strings,
// first the length N is given as an INT16. Then N bytes follow which are
// the UTF-8 encoding of the character sequence. A null value is encoded
// with length of -1 and there are no following bytes.
type nullableString string

func (ns *nullableString) ReadFrom(r io.Reader) (int64, error) {
	var strLen int16
	err := binary.Read(r, binary.BigEndian, &strLen)
	if err != nil {
		return 0, fmt.Errorf("read string length: %w", err)
	}
	if strLen < 0 {
		return 2, nil
	}
	if strLen == 0 {
		*ns = ""
		return 2, nil
	}
	buf := make([]byte, strLen)
	nRead, err := io.ReadFull(r, buf)
	if err != nil {
		return 2, fmt.Errorf("read string contents: %w", err)
	}
	*ns = nullableString(buf)
	return int64(nRead + 2), nil
}

type partition struct {
	errorCode              int16
	partitionIndex         int32
	leaderID               int32
	leaderEpoch            int32
	replicaNodes           []int32
	isrNodes               []int32
	eligibleLeaderReplicas []int32
	lastKnownELR           []int32
	offlineReplicas        []int32
}

func (p partition) WriteTo(w io.Writer) (int64, error) {
	buf := make([]byte, 0, 1024)
	buf = binary.BigEndian.AppendUint16(buf, uint16(p.errorCode))
	buf = binary.BigEndian.AppendUint32(buf, uint32(p.leaderID))
	buf = binary.BigEndian.AppendUint32(buf, uint32(p.leaderEpoch))
	buf = writeNodes(buf, p.replicaNodes)
	buf = writeNodes(buf, p.isrNodes)
	buf = writeNodes(buf, p.eligibleLeaderReplicas)
	buf = writeNodes(buf, p.lastKnownELR)
	buf = writeNodes(buf, p.offlineReplicas)
	// tag buffer
	buf = append(buf, 0x00)
	n, err := w.Write(buf)
	return int64(n), err
}

func writeNodes(buf []byte, nodes []int32) []byte {
	buf = binary.AppendVarint(buf, int64(len(nodes)+1))
	for _, node := range nodes {
		buf = binary.BigEndian.AppendUint32(buf, uint32(node))
	}
	return buf
}

type topicResponse struct {
	errorCode                 int16
	name                      compactString
	topicID                   uuid.UUID
	isInternal                bool
	partitions                []partition
	topicAuthorizedOperations int32
	tagBuffer                 taggedFields
}

func (t topicResponse) WriteTo(w io.Writer) (int64, error) {
	bw, ok := w.(*bufio.Writer)
	if !ok {
		bw = bufio.NewWriter(w)
	}
	if err := binary.Write(bw, binary.BigEndian, t.errorCode); err != nil {
		return int64(bw.Buffered()), fmt.Errorf("write errorCode: %w", err)
	}
	if _, err := t.name.WriteTo(bw); err != nil {
		return int64(bw.Buffered()), fmt.Errorf("write topicID: %w", err)
	}
	if err := binary.Write(bw, binary.BigEndian, t.topicID); err != nil {
		return int64(bw.Buffered()), fmt.Errorf("write topicID: %w", err)
	}
	if err := binary.Write(bw, binary.BigEndian, t.isInternal); err != nil {
		return int64(bw.Buffered()), fmt.Errorf("write isInternal: %w", err)
	}
	// partitions
	partitions := make(compactArrayResp[partition], 0, len(t.partitions))
	for _, p := range t.partitions {
		partitions = append(partitions, p)
	}
	if _, err := partitions.WriteTo(bw); err != nil {
		return int64(bw.Buffered()), fmt.Errorf("write partitions: %w", err)
	}
	// authorized operations
	if err := binary.Write(w, binary.BigEndian, t.topicAuthorizedOperations); err != nil {
		return int64(bw.Buffered()), fmt.Errorf("write authorized operations: %w", err)
	}

	if _, err := t.tagBuffer.WriteTo(bw); err != nil {
		return int64(bw.Buffered()), fmt.Errorf("write tag buffer: %w", err)
	}

	return int64(bw.Buffered()), nil
}

type cursor struct {
	topicName      compactString
	partitionIndex int32
}

func (c *cursor) WriteTo(w io.Writer) (int64, error) {
	// TODO: Implement me
	n, err := w.Write([]byte{0xff})
	if err != nil {
		return 0, fmt.Errorf("write null byte: %w", err)
	}
	return int64(n), nil
}

type describeTopicPartitionsResponse struct {
	throttleTimeMS int32
	topics         []topicResponse
	nextCursor     cursor
	tagBuffer      taggedFields
}

func (d describeTopicPartitionsResponse) WriteTo(w io.Writer) (int64, error) {
	bw, ok := w.(*bufio.Writer)
	if !ok {
		bw = bufio.NewWriter(w)
	}
	err := binary.Write(bw, binary.BigEndian, d.throttleTimeMS)
	if err != nil {
		return int64(bw.Buffered()), fmt.Errorf("write throttleTimeMS: %w", err)
	}
	topics := compactArrayResp[topicResponse]{}

	for _, t := range d.topics {
		topics = append(topics, t)
	}
	if _, err = topics.WriteTo(bw); err != nil {
		return int64(bw.Buffered()), fmt.Errorf("write topics: %w", err)
	}

	if _, err := d.nextCursor.WriteTo(bw); err != nil {
		return int64(bw.Buffered()), fmt.Errorf("write next cursor: %w", err)
	}

	if _, err := d.tagBuffer.WriteTo(bw); err != nil {
		return int64(bw.Buffered()), fmt.Errorf("write tag buffer: %w", err)
	}

	if err := bw.Flush(); err != nil {
		return int64(bw.Buffered()), fmt.Errorf("flushing buffer: %w", err)
	}
	return int64(bw.Buffered()), nil
}

const (
	// READ (bit index 3 from the right)
	aclOpRead = 1 << 3
	// WRITE (bit index 4 from the right)
	aclOpWrite = 1 << 4
	// CREATE (bit index 5 from the right)
	aclOpCreate = 1 << 5
	// DELETE (bit index 6 from the right)
	aclOpDelete = 1 << 6
	// ALTER (bit index 7 from the right)
	aclOpAlter = 1 << 7
	// DESCRIBE (bit index 8 from the right)
	aclOpDescribe = 1 << 8
	// DESCRIBE_CONFIGS (bit index 10 from the right)
	aclOpDescribeConfigs = 1 << 10
	// ALTER_CONFIGS (bit index 11 from the right)
	aclOpAlterConfigs = 1 << 11
)

// compactString contains a 32-bit unsigned varint representing the
// string's length + 1, followed by the string bytes.
type compactString string

func (c *compactString) ReadFrom(r io.Reader) (int64, error) {
	rdr, ok := r.(*bufio.Reader)
	if !ok {
		rdr = bufio.NewReader(r)
	}

	strLenPlus1, err := binary.ReadUvarint(rdr)
	if err != nil {
		return int64(1), fmt.Errorf("reading string length: %w", err)
	}

	buf := make([]byte, strLenPlus1-1)

	n, err := io.ReadFull(rdr, buf)
	if err != nil {
		return 1, fmt.Errorf("reading string contents: %w", err)
	}
	*c = compactString(buf)
	return int64(n + 1), nil
}

func (c *compactString) WriteTo(w io.Writer) (int64, error) {
	buf := make([]byte, 0, binary.MaxVarintLen32+len(*c))

	var length uint64
	if c != nil {
		length = uint64(len(*c) + 1)
	}
	buf = binary.AppendUvarint(buf, length)
	buf = append(buf, *c...)

	n, err := w.Write(buf)
	return int64(n), err
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
