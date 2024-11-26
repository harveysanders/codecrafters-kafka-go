package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestUnmarshalBinary(t *testing.T) {
	testCases := []struct {
		desc    string
		data    []byte
		wantReq request
	}{
		{
			desc: "sample request message",
			data: []byte{
				0x00, 0x00, 0x00, 0x23, // message_size:        35
				0x00, 0x12, //             request_api_key:     18
				0x00, 0x04, //             request_api_version: 4
				0x6f, 0x7f, 0xc6, 0x61, // correlation_id:      1870644833
			},
			wantReq: request{
				msgSize: 35,
				header: requestHeader{
					requestAPIKey:     18,
					requestAPIVersion: 4,
					correlationID:     1870644833,
				},
			},
		},
	}

	for _, tc := range testCases {
		req := &request{}
		err := req.UnmarshalBinary(tc.data)
		require.NoError(t, err)

		require.Equal(t, req.msgSize, tc.wantReq.msgSize)
		require.Equal(t, req.header.requestAPIKey, tc.wantReq.header.requestAPIKey)
		require.Equal(t, req.header.requestAPIVersion, tc.wantReq.header.requestAPIVersion)
		require.Equal(t, req.header.correlationID, tc.wantReq.header.correlationID)
	}
}

func TestAPIVersionsResponse(t *testing.T) {
	resp := response{
		header: responseHeader{correlationID: 12345},
		body: apiVersionsResponse{
			apiKeys: []apiKey{
				{index: APIKeyApiVersions, minVersion: 4, maxVersion: 4},
			},
		},
	}
	var gotBuf bytes.Buffer

	n, err := resp.WriteTo(&gotBuf)
	require.NoError(t, err)
	// ApiVersions Response (Version: 4) => error_code [api_keys] throttle_time_ms TAG_BUFFER
	// error_code => INT16
	// api_keys => api_key min_version max_version TAG_BUFFER
	//   api_key => INT16
	//   min_version => INT16
	//   max_version => INT16
	// throttle_time_ms => INT32

	// message_size:        4 bytes
	// ____________________________
	// correlation_id:      4 bytes
	// error_code: 					2 bytes
	// api_keys ___					1 byte for len (Compact Array)
	// api_key							2 bytes
	// min_version					2 bytes
	// max_version					2 bytes
	// tagged_fields				1 byte
	// api_keys ___
	// throttle_time_ms			4 bytes
	// tagged_fields				1 byte
	// total 								19 bytes
	wantLen := 19
	require.Equal(t, int64(wantLen), n)

	var gotMsgSize int32
	err = binary.Read(&gotBuf, binary.BigEndian, &gotMsgSize)
	require.NoError(t, err)
	require.Equal(t, int32(wantLen), gotMsgSize)
}

func TestCompactString(t *testing.T) {
	t.Run("can be read from a reader", func(t *testing.T) {
		testCases := []struct {
			input    []byte
			want     string
			wantRead int64
		}{
			{input: []byte{0x04, 0x66, 0x6f, 0x6f, 00},
				want:     "foo",
				wantRead: 5, // 1 for length, 3+1 for string itself
			},
		}

		for _, tc := range testCases {
			t.Run(fmt.Sprintf("decode: %q", tc.want), func(t *testing.T) {
				var got compactString
				n, err := got.ReadFrom(bytes.NewReader(tc.input))
				require.NoError(t, err)
				require.Equal(t, int64(5), n)
			})
		}

	})
}

func TestCompactArray(t *testing.T) {
	t.Run("can be read from a reader", func(t *testing.T) {
		input := []byte{
			0x02,                       // number of items + 1
			0x04, 0x66, 0x6f, 0x6f, 00, // topic: "foo" (compactString)
		}
		var got compactArrayReq[*topic]
		n, err := got.ReadFrom(bytes.NewReader(input))
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, int64(6), n)
	})
}
