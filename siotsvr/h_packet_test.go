package siotsvr

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPacketSend(t *testing.T) {
	defaultLog = DefaultLog()
	defaultLog.SetVerbose(2)
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
		fmt.Println("Received raw packet data:", d)
		s.parseRawPacket(d)
	}()
	defer close(s.rawPacketCh)

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
}
