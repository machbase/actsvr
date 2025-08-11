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
	if err := poiSvr.Start(context.Background()); err != nil {
		t.Fatalf("Failed to start PoiServer: %v", err)
	}
	defer poiSvr.Stop(context.Background())

	// Create a Gin router group for the PoiServer
	router := gin.Default()
	group := router.Group("/db/poi")
	poiSvr.Router(group)

	// Test the /db/poi/reload
	w := performRequest(router, "POST", "/db/poi/reload", nil)
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 OK, got %d\n%s", w.Code, w.Body.String())
	}

	// Test the /db/poi/nearby
	// 종로구: 37.5990998, 126.9861493
	w = performRequest(router, "GET", "/db/poi/nearby?la=37.5990998&lo=126.9861493&n=3", nil)
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 OK, got %d\n%s", w.Code, w.Body.String())
	}
	require.JSONEq(t, `[
		{"area_code":"11110", "area_nm":"종로구", "dist":0, "la":37.5991, "lo":126.986149},
		{"adres":"서울특별시 종로구 평창동 산6-17", "adres_detail":"", "buld_nm":"홍제천 (신영교앞) 상", "data_no":1, "dist":532, "instl_floor":null, "instl_ho_no":null, "la":37.601808, "lo":126.98117, "modl_serial":"OC3CL200154", "trnsmit_server_no":31},
		{"adres":"서울특별시 종로구 평창동 산6-17", "adres_detail":"", "buld_nm":"홍제천 (신영교앞) 상", "data_no":1, "dist":532, "instl_floor":null, "instl_ho_no":null, "la":37.601808, "lo":126.98117, "modl_serial":"OC3CL200154", "trnsmit_server_no":84}
	]`, w.Body.String())

	// Test the /db/poi/nearby with area code
	w = performRequest(router, "GET", "/db/poi/nearby?area_code=11110&n=5", nil)
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 OK, got %d\n%s", w.Code, w.Body.String())
	}
	require.JSONEq(t, `[
		{"area_code":"11110", "area_nm":"종로구", "dist":0, "la":37.5991, "lo":126.986149},
		{"adres":"서울특별시 종로구 평창동 산6-17", "adres_detail":"", "buld_nm":"홍제천 (신영교앞) 상", "data_no":1, "dist":532, "instl_floor":null, "instl_ho_no":null, "la":37.601808, "lo":126.98117, "modl_serial":"OC3CL200154", "trnsmit_server_no":31},
		{"adres":"서울특별시 종로구 평창동 산6-17", "adres_detail":"", "buld_nm":"홍제천 (신영교앞) 상", "data_no":1, "dist":532, "instl_floor":null, "instl_ho_no":null, "la":37.601808, "lo":126.98117, "modl_serial":"OC3CL200154", "trnsmit_server_no":84},
		{"adres":"서울특별시 종로구 평창동 산6-17", "adres_detail":"", "buld_nm":"홍제천 (신영교앞) 상", "data_no":1, "dist":532, "instl_floor":null, "instl_ho_no":null, "la":37.601808, "lo":126.98117, "modl_serial":"OC3CL200154", "trnsmit_server_no":49},
		{"adres":"서울특별시 종로구 평창동 산6-17", "adres_detail":"", "buld_nm":"홍제천 (신영교앞) 상", "data_no":1, "dist":532, "instl_floor":null, "instl_ho_no":null, "la":37.601808, "lo":126.98117, "modl_serial":"OC3CL200154", "trnsmit_server_no":48}
	]`, w.Body.String())

	// Test the /db/poi/nearby with modl_serial
	w = performRequest(router, "GET", "/db/poi/nearby?modl_serial=0000000032&trnsmit_server_no=3&data_no=1&n=5", nil)
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 OK, got %d\n%s", w.Code, w.Body.String())
	}
	require.JSONEq(t, `[
		{"adres":"강동구 천호동 571", "adres_detail":"천호 이스트센트럴타워 근생용", "buld_nm":"천호 이스트센트럴타워 근생용", "data_no":1, "dist":0, "instl_floor":0, "instl_ho_no":0, "la":37.537959, "lo":127.131604, "modl_serial":"0000000032", "trnsmit_server_no":3},
		{"adres":"강동구 천호동 571", "adres_detail":"천호 이스트센트럴타워 업무시설", "buld_nm":"천호 이스트센트럴타워 업무시설", "data_no":1, "dist":42, "instl_floor":0, "instl_ho_no":0, "la":37.537687, "lo":127.131948, "modl_serial":"0000000236", "trnsmit_server_no":3},
		{"adres":"강동구 천호동 571", "adres_detail":"강동 팰리스아파트(래미안) 상가용", "buld_nm":"강동 팰리스아파트(래미안) 상가용", "data_no":1, "dist":50, "instl_floor":0, "instl_ho_no":0, "la":37.538164, "lo":127.132119, "modl_serial":"0000000206", "trnsmit_server_no":3},
		{"adres":"서울특별시 강동구 천호대로 1077", "adres_detail":"101동 1층(천호동, 래미안강동팰리스)", "buld_nm":"구립꿈마루어린이집", "data_no":1, "dist":73, "instl_floor":1, "instl_ho_no":1, "la":37.537296, "lo":127.131626, "modl_serial":"A1E33C002809", "trnsmit_server_no":123},
		{"adres":"강동구 천호동 571", "adres_detail":"강동 팰리스아파트(래미안)", "buld_nm":"강동 팰리스아파트(래미안)", "data_no":1, "dist":64, "instl_floor":0, "instl_ho_no":0, "la":37.538164, "lo":127.132291, "modl_serial":"0000000223", "trnsmit_server_no":3}
	]`, w.Body.String())
}
