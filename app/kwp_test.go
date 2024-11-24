package main

import (
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
