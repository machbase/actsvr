package siotsvr

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteRecptnPacketData(t *testing.T) {
	router := httpTestServer.Router()

	tests := []struct {
		name           string
		queryParams    map[string]string
		expectedStatus int
		expectedBody   string
	}{
		{
			name: "Valid_request_with_all_parameters",
			queryParams: map[string]string{
				"PACKET_SEQ":           "123456789",
				"TRNSMIT_SERVER_NO":    "123",
				"DATA_NO":              "1",
				"PK_SEQ":               "987654321",
				"AREA_CODE":            "AREA01",
				"MODL_SERIAL":          "SERIAL123",
				"PACKET":               "PACKET_DATA",
				"PACKET_STTUS_CODE":    "1234",
				"RECPTN_RESULT_CODE":   "1234567890",
				"RECPTN_RESULT_MSSAGE": "Data received successfully",
				"PARS_SE_CODE":         "1234",
				"REGIST_DE":            "20240715",
				"REGIST_TIME":          "1030",
				"REGIST_DT":            "2024-07-15 10:30:45",
			},
			expectedStatus: http.StatusOK,
			expectedBody:   "RecptnPacketData written successfully!",
		},
		{
			name: "Request_with_special_characters_in_string_fields",
			queryParams: map[string]string{
				"PACKET_SEQ":           "123456789",
				"TRNSMIT_SERVER_NO":    "123",
				"DATA_NO":              "1",
				"PK_SEQ":               "987654321",
				"AREA_CODE":            "AREA@01#",
				"MODL_SERIAL":          "SERIAL/123\\456",
				"PACKET":               "DATA%20WITH%20SPACES",
				"PACKET_STTUS_CODE":    "1234",
				"RECPTN_RESULT_CODE":   "1234567890",
				"RECPTN_RESULT_MSSAGE": "Success with spaces and symbols!@#",
				"PARS_SE_CODE":         "1234",
				"REGIST_DE":            "20240715",
				"REGIST_TIME":          "1030",
				"REGIST_DT":            "2024-07-15 10:30:45",
			},
			expectedStatus: http.StatusOK,
			expectedBody:   "RecptnPacketData written successfully!",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build query string
			values := url.Values{}
			for key, value := range tt.queryParams {
				values.Add(key, value)
			}

			// Create request
			reqURL := "/db/write/RecptnPacketData?" + values.Encode()
			req := httptest.NewRequest(http.MethodGet, reqURL, nil)

			// Create response recorder
			w := httptest.NewRecorder()

			// Perform request
			router.ServeHTTP(w, req)

			// Assertions
			assert.Equal(t, tt.expectedStatus, w.Code, "Status code should match")
			assert.Equal(t, tt.expectedBody, w.Body.String(), "Response body should match")
		})
	}
}
