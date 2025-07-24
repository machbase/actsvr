package siotsvr

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWritePacketParsData(t *testing.T) {
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
				"PACKET_PARS_SEQ":   "123456789",
				"PACKET_SEQ":        "987654321",
				"TRNSMIT_SERVER_NO": "123",
				"DATA_NO":           "1",
				"REGIST_DT":         "2024-07-15 10:30:45",
				"REGIST_DE":         "20240715",
				"SERVICE_SEQ":       "1",
				"AREA_CODE":         "AREA01",
				"MODL_SERIAL":       "SERIAL123",
				"COLUMN0":           "Value0",
				"COLUMN10":          "Value10",
				"COLUMN2":           "Value2",
				"COLUMN63":          "Value63",
			},
			expectedStatus: http.StatusOK,
			expectedBody:   "PacketParsData written successfully!",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build query string
			values := url.Values{}
			for k, v := range tt.queryParams {
				values.Set(k, v)
			}

			// Create request
			reqURL := "/db/write/PacketParsData?" + values.Encode()
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
