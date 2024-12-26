package main

import (
	"bufio"
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadRecordBatch(t *testing.T) {
	testCases := []struct {
		desc  string
		input io.Reader
		want  RecordBatch
	}{
		{
			desc:  "bin spec test data",
			input: bytes.NewReader(exampleLogFile()),
			want: RecordBatch{
				offset:               0,
				length:               79,
				partitionLeaderEpoch: 1,
				magicByte:            2,
				crc:                  -1335278212,
				// attributes
				lastOffsetDelta: 0,
				baseTimeStamp:   mustParse(t, time.RFC3339Nano, "2024-09-11T09:12:23.832Z"),
				maxTimeStamp:    mustParse(t, time.RFC3339Nano, "2024-09-11T09:12:23.832Z"),
				producerID:      -1,
				producerEpoch:   -1,
				baseSequence:    -1,
				recordsLength:   1,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			got := RecordBatch{}
			_, err := got.ReadFrom(tc.input)
			require.NoError(t, err)

			require.Equal(t, tc.want.offset, got.offset)
			require.Equal(t, tc.want.length, got.length)
			require.Equal(t, tc.want.partitionLeaderEpoch, got.partitionLeaderEpoch)
			require.Equal(t, tc.want.magicByte, got.magicByte)
			require.Equal(t, tc.want.crc, got.crc)
			require.Equal(t, tc.want.lastOffsetDelta, got.lastOffsetDelta)
			require.Equal(t, tc.want.baseTimeStamp, got.baseTimeStamp)
			require.Equal(t, tc.want.maxTimeStamp, got.maxTimeStamp)
			require.Equal(t, tc.want.producerID, got.producerID)
			require.Equal(t, tc.want.producerEpoch, got.producerEpoch)
			require.Equal(t, tc.want.baseSequence, got.baseSequence)
			require.Equal(t, tc.want.recordsLength, got.recordsLength)
		})
	}
}

func TestLogFileIter(t *testing.T) {
	wantBatches := []RecordBatch{
		{
			offset:               0,
			length:               79,
			partitionLeaderEpoch: 1,
			magicByte:            2,
			crc:                  -1335278212,
			// attributes
			lastOffsetDelta: 0,
			baseTimeStamp:   mustParse(t, time.RFC3339Nano, "2024-09-11T09:12:23.832Z"),
			maxTimeStamp:    mustParse(t, time.RFC3339Nano, "2024-09-11T09:12:23.832Z"),
			producerID:      -1,
			producerEpoch:   -1,
			baseSequence:    -1,
			recordsLength:   1,
		},
		{
			offset:               1,
			length:               228,
			partitionLeaderEpoch: 1,
			magicByte:            2,
			crc:                  618336989,
			// attributes
			lastOffsetDelta: 2,
			baseTimeStamp:   mustParse(t, time.RFC3339Nano, "2024-09-11T09:12:37.397Z"),
			maxTimeStamp:    mustParse(t, time.RFC3339Nano, "2024-09-11T09:12:37.397Z"),
			producerID:      -1,
			producerEpoch:   -1,
			baseSequence:    -1,
			recordsLength:   3,
		},
	}

	t.Run("iterates over the file, reading the batches, skipping the records", func(t *testing.T) {
		logFile := LogFile{
			rdr: *bufio.NewReader(bytes.NewReader(exampleLogFile())),
		}
		for i, got := range logFile.BatchRecords() {
			want := wantBatches[i]
			require.Equal(t, want.offset, got.offset)
			require.Equal(t, want.length, got.length)
			require.Equal(t, want.partitionLeaderEpoch, got.partitionLeaderEpoch)
			require.Equal(t, want.magicByte, got.magicByte)
			require.Equal(t, want.crc, got.crc)
			require.Equal(t, want.lastOffsetDelta, got.lastOffsetDelta)
			require.Equal(t, want.baseTimeStamp, got.baseTimeStamp)
			require.Equal(t, want.maxTimeStamp, got.maxTimeStamp)
			require.Equal(t, want.producerID, got.producerID)
			require.Equal(t, want.producerEpoch, got.producerEpoch)
			require.Equal(t, want.baseSequence, got.baseSequence)
			require.Equal(t, want.recordsLength, got.recordsLength)
		}

		require.NoError(t, logFile.Err())
	})
}

func exampleLogFile() []byte {
	return []byte{
		// RecordBatch[0]
		// Base Offset (8 bytes, 0x00 in hex, 0 in decimal)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, // Batch Length (4 bytes, 0x4f in hex, 79 in decimal)
		0x00, 0x00, 0x4f,
		0x00, // Partition Leader Epoch (4 bytes, 0x01 in hex, 1 in decimal)
		0x00, 0x00, 0x01,
		0x02, // Magic Byte (1 byte, 0x02 in hex, 2 in decimal)
		0xb0, // CRC (4 bytes, 0xb069457c in hex, -1335278212 in decimal)
		0x69, 0x45, 0x7c,
		0x00,       // Attributes (2 bytes, 0x00 in hex, 0 in decimal)
		0x00, 0x00, // Last Offset Delta (4 bytes, 0x03 in hex, 3 in decimal)
		0x00, 0x00, 0x00,
		0x00, // Base Timestamp (8 bytes, 0x00000191e05af818 in hex, 1726045943832 in decimal)
		0x00, 0x01, 0x91, 0xe0, 0x5a, 0xf8, 0x18,
		0x00, // Max Timestamp (8 bytes, 0x00000191e05af818 in hex, 1726045943832 in decimal)
		0x00, 0x01, 0x91, 0xe0, 0x5a, 0xf8, 0x18,
		0xff, // Producer ID (8 bytes, 0xffffffffffffffff in hex, -1 in decimal)
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, // Producer Epoch (2 bytes, 0xffff in hex, -1 in decimal)
		0xff,
		0xff, // Base Sequence (4 bytes, 0xffffffff in hex, -1 in decimal)
		0xff, 0xff, 0xff,
		0x00, // Records Length (4 bytes, 0x01 in hex, 1 in decimal)
		0x00, 0x00, 0x01,

		// Record[0]
		0x3a, // Record Length (1 byte, 0x3a in hex, 29 in decimal (as signed varint)) (Length from attributes to the end of the record)
		0x00, // Attributes (1 byte, 0x00 in hex, 0 in decimal)
		0x00, // Timestamp Delta (1 byte, 0x00 in hex, 0 in decimal)
		0x00, // Offset Delta (1 byte, 0x00 in hex, 0 in decimal)
		0x01, // Key Length (1 byte, 0x01 in hex, -1 in decimal (as signed varint, using zigzag encoding, refer to: https://protobuf.dev/programming-guides/encoding/#signed-ints))
		// As key length is -1, the key value is empty
		0x2e, // Value Length (1 byte, 0x2e in hex, 23 in decimal (as signed varint))
		// Payload: Feature Level Record
		0x01, // Frame Version (1 byte, 0x01 in hex, 1 in decimal)
		0x0c, // Type (1 byte, 0x0c in hex, 12 in decimal)
		0x00, // Version (1 byte, 0x00 in hex, 0 in decimal)
		0x11, // Name Length (1 byte, 0x11 in hex, 17 in decimal (as unsigned varint))
		0x6d, // Name (Compact String (Length = 17 - 1), parsed as "metadata.version")
		0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x2e, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f,
		0x6e,
		0x00, // Feature Level (2 bytes, 0x14 in hex, 20 in decimal)
		0x14,
		0x00, // Tagged Field Count (1 byte, 0x00 in hex, 0 in decimal) (unsigned varint)
		0x00, // Headers array Count (1 byte, 0x00 in hex, 0 in decimal) (unsigned varint)

		// RecordBatch[1]
		0x00, // Base Offset (8 bytes, 0x01 in hex, 1 in decimal)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
		0x00, // Batch Length (4 bytes, 0xe4 in hex, 228 in decimal)
		0x00, 0x00, 0xe4,
		0x00, // Partition Leader Epoch (4 bytes, 0x01 in hex, 1 in decimal)
		0x00, 0x00, 0x01,
		0x02, // Magic Byte (1 byte, 0x02 in hex, 2 in decimal)
		0x24, // CRC (4 bytes, 0x24db12dd in hex, 618336989 in decimal)
		0xdb, 0x12, 0xdd,
		0x00, // Attributes (2 bytes, 0x00 in hex, 0 in decimal)
		0x00,
		0x00, // Last Offset Delta (4 bytes, 0x01 in hex, 1 in decimal)
		0x00, 0x00, 0x02,
		0x00, // Base Timestamp (8 bytes, 0x00000191e05b2d15 in hex, 1726045957397 in decimal)
		0x00,
		0x01,
		0x91,
		0xe0,
		0x5b,
		0x2d,
		0x15,
		0x00, // Max Timestamp (8 bytes, 0x00000191e05b2d15 in hex, 1726045957397 in decimal)
		0x00,
		0x01,
		0x91,
		0xe0,
		0x5b,
		0x2d,
		0x15,
		0xff, // Producer ID (8 bytes, 0xffffffffffffffff in hex, -1 in decimal)
		0xff,
		0xff,
		0xff,
		0xff,
		0xff,
		0xff,
		0xff,
		0xff, // Producer Epoch (2 bytes, 0xffff in hex, -1 in decimal)
		0xff,
		0xff, // Base Sequence (4 bytes, 0xffffffff in hex, -1 in decimal)
		0xff,
		0xff,
		0xff,
		0x00, // Records Length (4 bytes, 0x03 in hex, 3 in decimal)
		0x00,
		0x00,
		0x03,

		// Record[0]
		0x3c, // Record Length (1 byte, 0x3c in hex, 30 in decimal (as signed varint)) (Length from attributes to the end of the record)
		0x00, // Attributes (1 byte, 0x00 in hex, 0 in decimal)
		0x00, // Timestamp Delta (1 byte, 0x00 in hex, 0 in decimal)
		0x00, // Offset Delta (1 byte, 0x00 in hex, 0 in decimal)
		0x01, // Key Length (1 byte, 0x01 in hex, -1 in decimal (as signed varint, using zigzag encoding, refer to: https://protobuf.dev/programming-guides/encoding/#signed-ints))
		// As key length is -1, the key value is empty
		0x30, // Value Length (1 byte, 0x30 in hex, 24 in decimal (as signed varint))
		// Payload: Topic Record
		0x01, // Frame Version (1 byte, 0x01 in hex, 1 in decimal)
		0x02, // Type (1 byte, 0x02 in hex, 2 in decimal)
		0x00, // Version (1 byte, 0x00 in hex, 0 in decimal)
		0x04, // Name Length (1 byte, 0x04 in hex, 4 in decimal (as unsigned varint))
		0x73, // Topic Name (Compact String (Length = 4 - 1), parsed as "saz")
		0x61,
		0x7a,
		0x00, // Topic UUID (16 raw bytes, 00000000-0000-4000-8000-000000000091 after parsing)
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x40,
		0x00,
		0x80,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x91,
		0x00, // Tagged Field Count (1 byte, 0x00 in hex, 0 in decimal) (unsigned varint)
		0x00, // Headers array Count (1 byte, 0x00 in hex, 0 in decimal) (unsigned varint)

		// Record[1]
		0x90, // Record Length (1 byte, 0x9001 in hex, 72 in decimal (as signed varint)) (Length from attributes to the end of the record)
		0x01,
		0x00, // Attributes (1 byte, 0x00 in hex, 0 in decimal)
		0x00, // Timestamp Delta (1 byte, 0x00 in hex, 0 in decimal)
		0x02, // Offset Delta (1 byte, 0x02 in hex, 1 in decimal (as signed varint))
		0x01, // Key Length (1 byte, 0x01 in hex, -1 in decimal (as signed varint, using zigzag encoding, refer to: https://protobuf.dev/programming-guides/encoding/#signed-ints))
		// As key length is -1, the key value is empty
		0x82, // Value Length (2 bytes, 0x8201 in hex, 65 in decimal (as signed varint))
		0x01,
		// Payload: Partition Record
		0x01, // Frame Version (1 byte, 0x01 in hex, 1 in decimal)
		0x03, // Type (1 byte, 0x03 in hex, 3 in decimal)
		0x01, // Version (1 byte, 0x01 in hex, 1 in decimal)
		0x00, // Partition ID (4 bytes, 0x00 in hex, 0 in decimal)
		0x00,
		0x00,
		0x00,
		0x00, // Topic UUID (16 raw bytes, 00000000-0000-4000-8000-000000000091 after parsing)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x91,
		0x02, // Length of Replica array (1 byte, 0x02 in hex, 2 in decimal)
		0x00, // Replica array (1 element, length = (2-1), each element is 4 bytes)
		0x00, 0x00, 0x01,
		0x02, // Length of In Sync Replica array (1 byte, 0x02 in hex, 2 in decimal)
		0x00, // In Sync Replica array (1 element, length = (2-1), each element is 4 bytes)
		0x00, 0x00, 0x01,
		0x01, // Length of Removing Replicas array (1 byte, 0x01 in hex, 1 in decimal, actual length = (1 - 1 = 0))
		0x01, // Length of Adding Replicas array (1 byte, 0x01 in hex, 1 in decimal, actual length = (1 - 1 = 0))
		0x00, // Leader (4 bytes, 0x01 in hex, 1 in decimal)
		0x00,
		0x00,
		0x01,
		0x00, // Leader Epoch (4 bytes, 0x00 in hex, 0 in decimal)
		0x00,
		0x00,
		0x00,
		0x00, // Partition Epoch (4 bytes, 0x00 in hex, 0 in decimal)
		0x00,
		0x00,
		0x00,
		0x02, // Compact Array Length (1 byte, 0x02 in hex, 1 in decimal (parsed as an unsigned varint))
		0x10, // Directory UUID (16 raw bytes, 10000000-0000-4000-8000-000000000001 after parsing)
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x40,
		0x00,
		0x80,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x01,
		0x00, // Tagged Field Count (1 byte, 0x00 in hex, 0 in decimal) (unsigned varint)
		0x00, // Headers array Count (1 byte, 0x00 in hex, 0 in decimal) (unsigned varint)

		// Record[2]
		0x90, // Record Length (1 byte, 0x9001 in hex, 72 in decimal (as signed varint)) (Length from attributes to the end of the record)
		0x01,
		0x00, // Attributes (1 byte, 0x00 in hex, 0 in decimal)
		0x00, // Timestamp Delta (1 byte, 0x00 in hex, 0 in decimal)
		0x04, // Offset Delta (1 byte, 0x04 in hex, 2 in decimal (as signed varint))
		0x01, // Key Length (1 byte, 0x01 in hex, -1 in decimal (as signed varint, using zigzag encoding, refer to: https://protobuf.dev/programming-guides/encoding/#signed-ints))
		// As key length is -1, the key value is empty
		0x82, // Value Length (2 bytes, 0x8201 in hex, 65 in decimal (as signed varint))
		0x01,

		// Payload: Partition Record
		0x01, // Frame Version (1 byte, 0x01 in hex, 1 in decimal)
		0x03, // Type (1 byte, 0x03 in hex, 3 in decimal)
		0x01, // Version (1 byte, 0x01 in hex, 1 in decimal)
		0x00, // Partition ID (4 bytes, 0x01 in hex, 1 in decimal)
		0x00,
		0x00,
		0x01,
		0x00, // Topic UUID (16 raw bytes, 00000000-0000-4000-8000-000000000091 after parsing)
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x40,
		0x00,
		0x80,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x91,
		0x02, // Length of Replica array (1 byte, 0x02 in hex, 2 in decimal)
		0x00, // Replica array (1 element, length = (2-1), each element is 4 bytes)
		0x00,
		0x00,
		0x01,
		0x02, // Length of In Sync Replica array (1 byte, 0x02 in hex, 2 in decimal)
		0x00, // In Sync Replica array (1 element, length = (2-1), each element is 4 bytes)
		0x00,
		0x00,
		0x01,
		0x01, // Length of Removing Replicas array (1 byte, 0x01 in hex, 1 in decimal, actual length = (1 - 1 = 0))
		0x01, // Length of Adding Replicas array (1 byte, 0x01 in hex, 1 in decimal, actual length = (1 - 1 = 0))
		0x00, // Leader (4 bytes, 0x01 in hex, 1 in decimal)
		0x00,
		0x00,
		0x01,
		0x00, // Leader Epoch (4 bytes, 0x00 in hex, 0 in decimal)
		0x00,
		0x00,
		0x00,
		0x00, // Partition Epoch (4 bytes, 0x00 in hex, 0 in decimal)
		0x00,
		0x00,
		0x00,
		0x02, // Compact Array Length (1 byte, 0x02 in hex, 1 in decimal (parsed as an unsigned varint))
		0x10, // Directory UUID (16 raw bytes, 10000000-0000-4000-8000-000000000001 after parsing)
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x40,
		0x00,
		0x80,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x01,
		0x00, // Tagged Field Count (1 byte, 0x00 in hex, 0 in decimal) (unsigned varint)
		0x00, // Number of Headers (1 byte, 0x00 in hex, 0 in decimal)
	}
}

func mustParse(t *testing.T, layout string, v string) time.Time {
	t.Helper()
	res, err := time.Parse(layout, v)
	require.NoError(t, err)
	return res
}
