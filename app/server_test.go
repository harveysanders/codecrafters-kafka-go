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
	app := &app{
		supportedAPIs: supportedAPIs{
			APIKeyApiVersions: {
				minVersion: 3, maxVersion: 4,
			},
		},
	}
	srv := server{app}

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
		// ____________________________
		// correlation_id:      4 bytes
		// error_code: 					2 bytes
		// []api_keys ___				1 byte (len)
		// 2x
		// 	api_key							2 bytes
		// 	min_version					2 bytes
		// 	max_version					2 bytes
		// 	tagged_fields				1 byte
		// end api_keys ___
		// throttle_time_ms			4 bytes
		// tagged_fields				1 byte
		//
		// total 								19 bytes (not including msg size bytes)
		expectedReqLen := int32(19)
		var msgSize int32
		err = binary.Read(client, binary.BigEndian, &msgSize)
		require.NoError(t, err)

		require.Equal(t, expectedReqLen, msgSize)

		respBuf := make([]byte, msgSize) // skip 4 bytes from msgLen
		_, err = io.ReadFull(client, respBuf)
		require.NoError(t, err)

		// Check all the fields
		//  - .ResponseHeader
		//  	- .correlation_id (391911466)
		require.Equal(t, []byte{0x17, 0x5c, 0x18, 0x2a}, respBuf[0:4])
		//  - .ResponseBody
		//  	- .error_code (0)
		require.Equal(t, []byte{0x0, 0x0}, respBuf[4:6])
		//  	- .num_api_keys (1)
		require.Equal(t, []byte{0x2}, respBuf[6:7])
		//  	- .ApiKeys[0]
		//  		- .api_key (18)
		require.Equal(t, []byte{0x0, 0x12}, respBuf[7:9])
		//  		- .min_version (4)
		require.Equal(t, []byte{0x0, 0x3}, respBuf[9:11])
		//  		- .max_version (4)
		require.Equal(t, []byte{0x0, 0x4}, respBuf[11:13])
		//  		- .TAG_BUFFER
		require.Equal(t, []byte{0x0}, respBuf[13:14])
		//  	- .throttle_time_ms (0)
		require.Equal(t, []byte{0x0, 0x0, 0x0, 0x0}, respBuf[14:18])
		//  	- .TAG_BUFFER
		require.Equal(t, []byte{0x0}, respBuf[18:19])
	})

}
