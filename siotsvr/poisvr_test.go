package siotsvr

import (
	"context"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestPoiServer(t *testing.T) {
	// Initialize the PoiServer
	poiSvr := &PoiServer{}
	if err := poiSvr.Start(context.Background(), nil); err != nil {
		t.Fatalf("Failed to start PoiServer: %v", err)
	}
	defer poiSvr.Stop(context.Background())

	// Create a Gin router group for the PoiServer
	router := gin.Default()
	group := router.Group("/poi")
	poiSvr.Router(group)

	// Test the /db/poi/debug
	w := performRequest(router, "POST", "/poi/debug", nil)
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 OK, got %d\n%s", w.Code, w.Body.String())
	}

	// Test the /db/poi/nearby
	// 종로구: 37.5990998, 126.9861493
	w = performRequest(router, "GET", "/poi/nearby?la=37.5990998&lo=126.9861493&maxDist=10000&maxN=3", nil)
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 OK, got %d\n%s", w.Code, w.Body.String())
	}
	require.JSONEq(t, `[
		{"area_code":"11110", "area_nm":"종로구", "dist":0, "la":37.5991, "lo":126.986149},
		{"area_code":"11290", "area_nm":"성북구", "dist":3381, "la":37.606991, "lo":127.023218},
		{"area_code":"11140", "area_nm":"중구", "dist":4630, "la":37.557945, "lo":126.99419}
	]`, w.Body.String())
}
