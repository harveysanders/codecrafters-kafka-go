package main

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServer(t *testing.T) {
	srv := server{}

	go func(t *testing.T) {
		err := srv.ListenAndServe()
		require.NoError(t, err)
	}(t)

	t.Run("'ApiVersions' request", func(t *testing.T) {
		request := []byte{
			0x00, 0x00, 0x00, 0x23, // message_size: 35
			0x00, 0x12, // request_api_key: 18
			0x00, 0x04, // request_api_version: v4
			0x17, 0x5c, 0x18, 0x2a, // correlation_id: 391911466
			// client_software_name
			// client_software_version
			0x00, 0x09, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, 0x00, 0x0a, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, 0x04, 0x30, 0x2e, 0x31, 0x00,
		}

		// wait for server to start
		time.Sleep(20 * time.Millisecond)

		client, err := net.Dial("tcp", "127.0.0.1:9092")
		require.NoError(t, err)

		nW, err := client.Write(request)
		require.NoError(t, err)
		require.Equal(t, len(request), nW)

		// ApiVersions Response
		// message_size:        4 bytes
		// correlation_id:      4 bytes
		// error_code: 					2 bytes
		// []api_keys ___
		// 	api_key							2 bytes
		// 	min_version					2 bytes
		// 	max_version					2 bytes
		// 	tagged_fields				1 byte
		// end api_keys ___
		// throttle_time_ms			4 bytes
		// tagged_fields				1 byte
		//
		// total 								22 bytes
		expectedReqLen := int32(22)
		var msgSize int32
		err = binary.Read(client, binary.BigEndian, &msgSize)
		require.NoError(t, err)

		require.Equal(t, expectedReqLen, msgSize)

		respBuf := make([]byte, 0, msgSize-4)
		_, err = io.ReadFull(client, respBuf)
		require.NoError(t, err)
	})

}
