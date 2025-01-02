package metadata

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

type LogFile struct {
	rdr      bufio.Reader // Reader to read the underlying log file.
	err      error        // The current error, if any.
	curBatch RecordBatch  // The current batch pulled from the log file.
}

// NewLogFile returns a new [LogFile] that reads record batches from r.
func NewLogFile(r io.Reader) *LogFile {
	return &LogFile{rdr: *bufio.NewReader(r)}
}

// Batch returns the most recent record batch read from the log file.
func (l *LogFile) Batch() (int, RecordBatch) {
	return int(l.curBatch.Offset), l.curBatch
}

// Next advances to the next record batch in the log file. If there is an error reading the next batch, Next() returns false, and [LogFile].Err() will be non-nil.
func (l *LogFile) Next() bool {
	rb := RecordBatch{file: l}
	_, err := rb.ReadFrom(&l.rdr)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			l.err = fmt.Errorf("read record batch: %w", err)
		}
		return false
	}

	l.curBatch = rb
	return true
}

func (l *LogFile) Err() error {
	return l.err
}

// RecordBatch represents the on-disk format that Kafka uses to store multiple records.
type RecordBatch struct {
	file      *LogFile // Reference to containing log file.
	err       error    // Error from reading the record from the record batch's log file.
	curRecord *Record  // The last read record from the batch
	hasNext   bool     // Whether or not there is another record in the batch.

	Offset               int64 // Indicates the offset of the first record in this batch. Ex `0` is the first record, `1` the 2nd...
	Length               int32 // Length of the entire record batch in bytes.
	PartitionLeaderEpoch int32 // Indicates the epoch of the leader for this partition. It is a monotonically increasing number that is incremented by 1 whenever the partition leader changes. This value is used to detect out of order writes.
	MagicByte            byte  // The version of the record batch.
	CRC                  int32 // CRC32-C checksum of the record batch. The CRC is computed over the data following the CRC field to the end of the record batch. The CRC32-C (Castagnoli) polynomial is used for the computation.

	// TODO:
	// Attributes is a 2-byte big-endian integer indicating the Attributes of the record batch.
	//
	// Attributes is a bitmask of the following flags:
	//
	// bit 0-2:
	//   0: no compression
	//   1: gzip
	//   2: snappy
	//   3: lz4
	//   4: zstd
	// bit 3:
	//   timestampType
	// bit 4:
	//   isTransactional (0 means not transactional)
	// bit 5:
	//  isControlBatch (0 means not a control batch)
	// bit 6:
	//   hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
	// bit 7~15:
	//   unused
	// In this case, the value is 0x00, which is 0 in decimal.
	Attributes      int16
	LastOffsetDelta int32     // The difference between the last offset of this record batch and the base offset. Ex: `0`` indicates 1 record in the batch. `2` indicates 3 records in the batch.
	BaseTimeStamp   time.Time // Base Timestamp is a 8-byte big-endian integer indicating the timestamp of the first record in this batch.
	MaxTimeStamp    time.Time // Max Timestamp is a 8-byte big-endian integer indicating the maximum timestamp of the records in this batch.
	ProducerID      int64     // Producer ID is a 8-byte big-endian integer indicating the ID of the producer that produced the records in this batch.
	ProducerEpoch   int16     // Producer Epoch is a 2-byte big-endian integer indicating the epoch of the producer that produced the records in this batch.
	BaseSequence    int32     // Base Sequence is a 4-byte big-endian integer indicating the sequence number of the first record in a batch. It is used to ensure the correct ordering and deduplication of messages produced by a Kafka producer.
	RecordsLength   int32     // Records Length is a 4-byte big-endian integer indicating the number of records in this batch.
	Records         []Record
}

func (rb *RecordBatch) ReadFrom(r io.Reader) (int64, error) {
	header := make([]byte, 61)
	n, err := io.ReadFull(r, header)
	if err != nil {
		return int64(n), fmt.Errorf("read header: %w", err)
	}
	nRead := int64(len(header))
	rb.hasNext = true
	rb.Offset = int64(binary.BigEndian.Uint64(header[0:8]))
	rb.Length = int32(binary.BigEndian.Uint32(header[8:12]))
	rb.PartitionLeaderEpoch = int32(binary.BigEndian.Uint32(header[12:16]))
	rb.MagicByte = header[16]
	rb.CRC = int32(binary.BigEndian.Uint32(header[17:21]))
	rb.Attributes = int16(binary.BigEndian.Uint16(header[21:23]))
	rb.LastOffsetDelta = int32(binary.BigEndian.Uint32(header[23:27]))

	baseTSMS := int64(binary.BigEndian.Uint64(header[27:35]))
	rb.BaseTimeStamp = time.UnixMilli(baseTSMS).UTC()

	maxTSMS := int64(binary.BigEndian.Uint64(header[35:43]))
	rb.MaxTimeStamp = time.UnixMilli(maxTSMS).UTC()

	rb.ProducerID = int64(binary.BigEndian.Uint64(header[43:51]))
	rb.ProducerEpoch = int16(binary.BigEndian.Uint16(header[51:53]))
	rb.BaseSequence = int32(binary.BigEndian.Uint32(header[53:57]))
	rb.RecordsLength = int32(binary.BigEndian.Uint32(header[57:61]))

	rb.Records = make([]Record, rb.RecordsLength)
	for i, rec := range rb.Records {
		n, err := rec.ReadFrom(r)
		nRead += n
		if err != nil {
			return nRead, fmt.Errorf("read record :%w", err)
		}
		rb.Records[i] = rec
	}
	return nRead, nil
}

func (rb *RecordBatch) NextRecord() bool {
	if !rb.hasNext {
		return false
	}

	record := Record{}
	_, err := record.ReadFrom(&rb.file.rdr)
	if err != nil {
		rb.err = err
		return false
	}
	rb.err = nil
	rb.curRecord = &record

	rb.hasNext = record.OffsetDelta < int64(rb.RecordsLength)-1
	return true
}

func (rb *RecordBatch) Cur() *Record {
	return rb.curRecord
}

func (rb *RecordBatch) Err() error {
	return rb.err
}

type RecordType int

const (
	TypeTopic        RecordType = 0x02
	TypePartition    RecordType = 0x03
	TypeFeatureLevel RecordType = 0x0c
)

type Record struct {
	Length            int64      // Length is a signed variable size integer indicating the length of the record, the length is calculated from the attributes field to the end of the record.
	Attributes        int8       // Attributes is a 1-byte big-endian integer indicating the attributes of the record. Currently, this field is unused in the protocol.
	TimestampDelta    int64      // Timestamp Delta is a signed variable size integer indicating the difference between the timestamp of the record and the base timestamp of the record batch.
	OffsetDelta       int64      // Offset Delta is a signed variable size integer indicating the difference between the offset of the record and the base offset of the record batch.
	Type              RecordType // Value type for the record.
	KeyLength         int64      // Key Length is a signed variable size integer indicating the length of the key of the record.
	Key               []byte     // Key is a byte array indicating the key of the record.
	ValueLength       int64      // Value Length is a signed variable size integer indicating the length of the value of the record.
	Value             []byte     // Value is a byte array indicating the value of the record.
	HeadersArrayCount uint       // Header array count is an unsigned variable size integer indicating the number of headers present.
}

func (rec *Record) ReadFrom(r io.Reader) (int64, error) {
	var err error
	var nRead int
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}

	length, n, err := readVarInt(br)
	nRead += n
	if err != nil {
		return int64(nRead), fmt.Errorf("read length: %w", err)
	}

	rec.Length = length
	buf := make([]byte, rec.Length)

	n, err = br.Read(buf)
	nRead += n
	if err != nil {
		return int64(nRead), fmt.Errorf("read record: %w", err)
	}

	var cursor int
	rec.Attributes = int8(buf[cursor])
	rec.TimestampDelta, n = binary.Varint(buf[1:])
	if err := checkN(n); err != nil {
		return int64(nRead), fmt.Errorf("read timestamp delta %w", err)
	}
	return int64(nRead), nil
}

func readVarInt(r *bufio.Reader) (val int64, n int, err error) {
	buf, err := r.Peek(binary.MaxVarintLen64)
	if err != nil {
		return 0, 0, err
	}
	val, n = binary.Varint(buf)
	if err := checkN(n); err != nil {
		return val, n, fmt.Errorf("read varint: %w", err)
	}
	// Read off how ever many bytes were needed for the variable int
	discarded, err := r.Discard(n)
	if err != nil {
		return val, n, fmt.Errorf("discard: %w", err)
	}
	if discarded != n {
		return val, n, fmt.Errorf("expected to discard %d bytes, but only dropped %d", n, discarded)
	}
	return val, n, nil
}

func checkN(n int) error {
	if n == 0 {
		return io.ErrUnexpectedEOF
	}
	if n < 0 {
		return errors.New("overflow")
	}
	return nil
}
