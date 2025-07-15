package siotsvr

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/gin-gonic/gin"
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
			name: "Valid_request_with_minimal_parameters",
			queryParams: map[string]string{
				"PACKET_SEQ":        "123",
				"TRNSMIT_SERVER_NO": "100",
				"DATA_NO":           "1",
			},
			expectedStatus: http.StatusOK,
			expectedBody:   "RecptnPacketData written successfully!",
		},
		{
			name: "Valid_request_with_numeric_string_values",
			queryParams: map[string]string{
				"TRNSMIT_SERVER_NO": "-1",
				"PACKET_SEQ":        "999999999",
				"DATA_NO":           "999",
				"PK_SEQ":            "888888888",
			},
			expectedStatus: http.StatusBadRequest,
			expectedBody:   "Validation failed: invalid TRNSMIT_SERVER_NO: -1, must be non-negative",
		},
		{
			name: "Request_with_invalid_DATA_NO_(non-numeric)",
			queryParams: map[string]string{
				"PACKET_SEQ":        "123",
				"TRNSMIT_SERVER_NO": "SVR1",
				"DATA_NO":           "invalid",
			},
			expectedStatus: http.StatusBadRequest,
			expectedBody:   "Invalid query parameters",
		},
		{
			name: "Request with invalid PACKET_SEQ (non-numeric)",
			queryParams: map[string]string{
				"PACKET_SEQ":        "invalid",
				"TRNSMIT_SERVER_NO": "SVR1",
				"DATA_NO":           "1",
			},
			expectedStatus: http.StatusBadRequest,
			expectedBody:   "Invalid query parameters",
		},
		{
			name: "Request_with_invalid_PK_SEQ_(non-numeric)",
			queryParams: map[string]string{
				"PACKET_SEQ":        "123",
				"TRNSMIT_SERVER_NO": "SVR1",
				"DATA_NO":           "1",
				"PK_SEQ":            "invalid",
			},
			expectedStatus: http.StatusBadRequest,
			expectedBody:   "Invalid query parameters",
		},
		{
			name:           "Empty_request_(no parameters)",
			queryParams:    map[string]string{},
			expectedStatus: http.StatusBadRequest,
			expectedBody:   "Validation failed: invalid PACKET_SEQ: 0, must be greater than 0",
		},
		{
			name: "Request_with_special_characters_in_string_fields",
			queryParams: map[string]string{
				"PACKET_SEQ":           "123456789",
				"TRNSMIT_SERVER_NO":    "123",
				"AREA_CODE":            "AREA@01#",
				"MODL_SERIAL":          "SERIAL/123\\456",
				"PACKET":               "DATA%20WITH%20SPACES",
				"RECPTN_RESULT_MSSAGE": "Success with spaces and symbols!@#",
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
			req := httptest.NewRequest(http.MethodGet, "/db/write/RecptnPacketData?"+values.Encode(), nil)

			// Create response recorder
			w := httptest.NewRecorder()

			// Perform request
			router.ServeHTTP(w, req)

			// Assertions
			assert.Equal(t, tt.expectedStatus, w.Code, "Status code should match")
			assert.Equal(t, tt.expectedBody, w.Body.String(), "Response body should match")
		})
		t.Run(tt.name, func(t *testing.T) {
			// Build query string
			data, err := json.Marshal(tt.queryParams)
			if err != nil {
				t.Fatalf("Failed to marshal query parameters: %v", err)
			}

			// Create request
			req := httptest.NewRequest(http.MethodPost, "/db/write/RecptnPacketData", bytes.NewBuffer(data))

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

func TestWriteRecptnPacketDataStructBinding(t *testing.T) {
	// Set Gin to test mode
	gin.SetMode(gin.TestMode)

	// Create a mock HttpServer
	httpServer := &HttpServer{}

	// Create a Gin router and add the route
	router := gin.New()
	router.GET("/db/write/RecptnPacketData", httpServer.writeRecptnPacketData)

	// Test data binding by checking if the struct fields are properly parsed
	t.Run("Test struct field binding", func(t *testing.T) {
		queryParams := map[string]string{
			"PACKET_SEQ":           "123456789",
			"TRNSMIT_SERVER_NO":    "SERVER001",
			"DATA_NO":              "42",
			"PK_SEQ":               "987654321",
			"AREA_CODE":            "AREA01",
			"MODL_SERIAL":          "SERIAL123",
			"PACKET":               "PACKET_DATA",
			"PACKET_STTUS_CODE":    "SUCCESS",
			"RECPTN_RESULT_CODE":   "OK",
			"RECPTN_RESULT_MSSAGE": "Data received successfully",
			"PARS_SE_CODE":         "PARSED",
			"REGIST_DE":            "2024-07-15",
			"REGIST_TIME":          "10:30:45",
			"REGIST_DT":            "2024-07-15 10:30:45",
		}

		// Build query string
		values := url.Values{}
		for key, value := range queryParams {
			values.Add(key, value)
		}

		// Create request
		req := httptest.NewRequest(http.MethodGet, "/db/write/RecptnPacketData?"+values.Encode(), nil)

		// Create response recorder
		w := httptest.NewRecorder()

		// Perform request
		router.ServeHTTP(w, req)

		// Should succeed if binding works correctly
		assert.Equal(t, http.StatusOK, w.Code, "Request should succeed with valid parameters")
		assert.Equal(t, "RecptnPacketData written successfully!", w.Body.String())
	})
}

func TestWriteRecptnPacketDataEdgeCases(t *testing.T) {
	// Set Gin to test mode
	gin.SetMode(gin.TestMode)

	// Create a mock HttpServer
	httpServer := &HttpServer{}

	// Create a Gin router and add the route
	router := gin.New()
	router.GET("/db/write/RecptnPacketData", httpServer.writeRecptnPacketData)

	tests := []struct {
		name           string
		queryString    string
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "Request with empty values",
			queryString:    "PACKET_SEQ=&TRNSMIT_SERVER_NO=&DATA_NO=&PK_SEQ=",
			expectedStatus: http.StatusBadRequest,
			expectedBody:   "Invalid query parameters",
		},
		{
			name:           "Request with only DATA_NO as zero",
			queryString:    "DATA_NO=0",
			expectedStatus: http.StatusOK,
			expectedBody:   "RecptnPacketData written successfully!",
		},
		{
			name:           "Request with negative numbers",
			queryString:    "PACKET_SEQ=-123&DATA_NO=-1&PK_SEQ=-999",
			expectedStatus: http.StatusOK,
			expectedBody:   "RecptnPacketData written successfully!",
		},
		{
			name:           "Request with very large numbers",
			queryString:    "PACKET_SEQ=9223372036854775807&DATA_NO=2147483647&PK_SEQ=9223372036854775807",
			expectedStatus: http.StatusOK,
			expectedBody:   "RecptnPacketData written successfully!",
		},
		{
			name:           "Request with URL encoded values",
			queryString:    "TRNSMIT_SERVER_NO=SERVER%20001&RECPTN_RESULT_MSSAGE=Success%20with%20encoded%20spaces",
			expectedStatus: http.StatusOK,
			expectedBody:   "RecptnPacketData written successfully!",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request
			req := httptest.NewRequest(http.MethodGet, "/db/write/RecptnPacketData?"+tt.queryString, nil)

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
