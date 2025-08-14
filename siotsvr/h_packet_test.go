package siotsvr

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPacketSend(t *testing.T) {
	defaultLog = DefaultLog()
	defaultLog.SetVerbose(2)
	nowFunc = func() time.Time {
		return time.Unix(0, 1755129018971253000)
	}
	if err := reloadCertkey(); err != nil {
		t.Fatalf("Failed to reload cert key: %v", err)
	}
	if err := reloadPacketDefinition(); err != nil {
		t.Fatalf("Failed to reload packet definition: %v", err)
	}
	if err := reloadModelAreaCode(); err != nil {
		t.Fatalf("Failed to reload model area code: %v", err)
	}
	s := NewHttpServer()
	go func() {
		d := <-s.rawPacketCh
		data, err := json.Marshal(d)
		require.NoError(t, err, "Failed to marshal raw packet data")
		expect := `{
			"PacketSeq":1,
			"TrnsmitServerNo":43,
			"DataNo":1,
			"PkSeq":202506010000030000,
			"AreaCode":"11200",
			"ModlSerial":"SCMS-SEOUL-SD-0006",
			"DqmCrrOp":0,
			"Packet":"SCMS-SEOUL-SDSCMS-SEOUL-SD-00060004",
			"PacketSttusCode":"S",
			"RecptnResultCode":"SUCC-000",
			"RecptnResultMssage":"수신 완료",
			"ParsSeCode":"A",
			"RegistDe":"20250814",
			"RegistTime":"0850",
			"RegistDt":1755129018971253000
		}`
		require.JSONEq(t, expect, string(data))
		parsed := s.parseRawPacket(d)
		s.parsPacketCh <- parsed
	}()
	defer close(s.rawPacketCh)

	var parseWg sync.WaitGroup
	parseWg.Add(1)
	go func() {
		d := <-s.parsPacketCh
		defer parseWg.Done()
		data, err := json.Marshal(d)
		require.NoError(t, err, "Failed to marshal parsed packet data")
		expect := `{
		"PacketParsSeq":1, 
		"PacketSeq":1, 
		"TrnsmitServerNo":43, 
		"DataNo":1, 
		"ServiceSeq":0, 
		"AreaCode":"11200", 
		"ModlSerial":"SCMS-SEOUL-SD-0006", 
		"DqmCrrOp":0, 
		"RegistDt":1755129018971253000, 
		"RegistDe":"20250814", 
		"Values":["SCMS-SEOUL-SD", "SCMS-SEOUL-SD-0006", "0004"]}`
		require.JSONEq(t, expect, string(data))
	}()
	defer close(s.parsPacketCh)
	// Create a Gin router group for the PoiServer
	router := s.Router()

	certKey := "2db21d56bfb44f1d9b56"
	pkSeq := "202506010000030000"
	modelSerial := "SCMS-SEOUL-SD-0006"
	// 모델명 : 13
	// 시리얼: 18
	// 보행자수: 4
	// SCMS-SEOUL-SDSCMS-SEOUL-SD-00060004
	data := "SCMS-SEOUL-SDSCMS-SEOUL-SD-00060004"
	path := fmt.Sprintf("/n/api/send/%s/1/%s/%s/%s", certKey, pkSeq, modelSerial, data)
	w := performRequest(router, "GET", path, nil)
	if w.Code != http.StatusOK {
		t.Errorf("Expected status OK, got %v", w.Code)
	}
	require.JSONEq(t, `{"resultStats":{"resultCode":"SUCC-000", "resultMsg":"수신 완료"}}`, w.Body.String())
	// Wait for the parsing goroutine to finish
	parseWg.Wait()
}
