package metadata

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

type LogFile struct {
	rdr        bufio.Reader // Reader to read the underlying log file.
	err        error        // The current error, if any.
	curBatch   RecordBatch  // The current batch pulled from the log file.
	nextOffset int          // nextOffset is the byte offset to the next record batch on the reader.
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
	// read off the diff from current head and full batch length
	// before moving to the next batch...aka janky Seek()
	nDiscarded, err := l.rdr.Discard(int(l.nextOffset))
	if err != nil {
		l.err = fmt.Errorf("discard bytes before next batch: %w", err)
		return false
	}
	if l.nextOffset != nDiscarded {
		l.err = fmt.Errorf("expected to discard %d bytes, but actually discarded %d", l.nextOffset, nDiscarded)
		return false
	}

	rb := RecordBatch{}

	n, err := rb.ReadFrom(&l.rdr)
	if err != nil {
		l.err = fmt.Errorf("read record batch: %w", err)
		return false
	}

	l.curBatch = rb

	l.nextOffset = int(int64(rb.Length) - n)
	return rb.Offset < int64(rb.LastOffsetDelta)
}

func (l *LogFile) Err() error {
	return l.err
}

// RecordBatch represents the on-disk format that Kafka uses to store multiple records.
type RecordBatch struct {
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
}

type Record struct{}

func (rb *RecordBatch) ReadFrom(r io.Reader) (int64, error) {
	header := make([]byte, 61)
	n, err := io.ReadFull(r, header)
	if err != nil {
		return int64(n), fmt.Errorf("read header: %w", err)
	}
	nRead := int64(len(header))
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

	return nRead, nil
}
