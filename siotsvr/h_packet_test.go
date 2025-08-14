package siotsvr

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
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

func TestPacketSendCases(t *testing.T) {
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

	tests := []struct {
		name string
		// inputs
		certKey     string
		pkSeq       int64
		modelSerial string
		data        string
		now         int64
		// expects
		packetMasterSeq int64
		tsn             int64
		areaCode        string
		values          []string
	}{
		{
			name:            "tc_55",
			certKey:         "2db21d56bfb44f1d9b56",
			pkSeq:           202506010000030000,
			modelSerial:     "SCMS-SEOUL-SD-0006",
			data:            "SCMS-SEOUL-SDSCMS-SEOUL-SD-00060004",
			now:             1755129018971253000,
			packetMasterSeq: 55,
			tsn:             43,
			areaCode:        "11200",
			values:          []string{"SCMS-SEOUL-SD", "SCMS-SEOUL-SD-0006", "0004"},
		},
		{
			name:            "tc_36",
			certKey:         "a42398c0e429415ab9e6",
			pkSeq:           202506302359940343,
			modelSerial:     "V02Q1940343",
			data:            "SDOT001V02Q19403430001000200029.1075xxxxxxxxxxxxxxxxxxxxxx00000000000.0440000.030000.020001.030000.080000.190001.09xxxxxxx0001000220250630235900000.0xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
			now:             1751295599105542439,
			packetMasterSeq: 36,
			tsn:             30,
			areaCode:        "11380",
			values: []string{
				"SDOT001", "V02Q1940343", "0001", "0002", "00029.1", "075", "", "", "", "",
				"000000", "00000.0", "44", "0000.03", "0000.02", "0001.03", "0000.08", "0000.19", "0001.09",
				"", "0001", "0002", "202506302359", "00000.0", "", "", "", "", "", "", ""},
		},
		{
			name:            "tc_142",
			certKey:         "285ddf7fa1f64b2b8ac9",
			pkSeq:           202506300000035144,
			modelSerial:     "c8c3a7e5abea7",
			data:            "IMC_1000Ac8c3a7e5abea7025071000400030092702131",
			now:             1751295541832625228,
			packetMasterSeq: 142,
			tsn:             111,
			areaCode:        "11545",
			values:          []string{"IMC_1000A", "c8c3a7e5abea7", "025", "071", "0004", "0003", "00927", "02131"},
		},
		// Add more test cases as needed
	}
	s := NewHttpServer()
	defer close(s.rawPacketCh)
	defer close(s.parsPacketCh)
	for tcIdx, tc := range tests {
		nowFunc = func() time.Time { return time.Unix(0, tc.now) }
		go func() {
			d := <-s.rawPacketCh
			require.NotNil(t, d, "RawPacketData should not be nil")
			require.Equal(t, int64(tcIdx+1), d.PacketSeq, "PacketSeq should be "+strconv.Itoa(tcIdx+1))
			require.Equal(t, tc.tsn, d.TrnsmitServerNo, "TrnsmitServerNo should be "+strconv.FormatInt(tc.tsn, 10))
			require.Equal(t, tc.pkSeq, d.PkSeq, "PkSeq should be "+strconv.FormatInt(tc.pkSeq, 10))
			require.Equal(t, tc.areaCode, d.AreaCode, "AreaCode should be "+tc.areaCode)
			require.Equal(t, tc.modelSerial, d.ModlSerial, "ModlSerial should be "+tc.modelSerial)
			require.Equal(t, 0, d.DqmCrrOp, "DqmCrrOp should be 0")
			require.Equal(t, tc.data, d.Packet, "Packet should be "+tc.data)
			require.Equal(t, PacketStatusOK, d.PacketSttusCode, "PacketSttusCode should be S")
			require.Equal(t, "SUCC-000", d.RecptnResultCode, "RecptnResultCode should be SUCC-000")
			require.Equal(t, "수신 완료", d.RecptnResultMssage, "RecptnResultMssage should be 수신 완료")
			require.Equal(t, PacketParseAuto, d.ParsSeCode, "ParsSeCode should be A")
			require.Equal(t, time.Unix(0, tc.now).Format("20060102"), d.RegistDe, "RegistDe")
			require.Equal(t, time.Unix(0, tc.now).Format("1504"), d.RegistTime, "RegistTime")
			require.Equal(t, tc.now, d.RegistDt, "RegistDt should be "+strconv.FormatInt(tc.now, 10))
			parsed := s.parseRawPacket(d)
			s.parsPacketCh <- parsed
		}()

		var parseWg sync.WaitGroup
		parseWg.Add(1)
		go func() {
			defer parseWg.Done()
			d := <-s.parsPacketCh
			require.NotNil(t, d, "ParsedPacketData should not be nil")
			require.Equal(t, int64(tcIdx+1), d.PacketParsSeq, "PacketParsSeq should be "+strconv.FormatInt(int64(tcIdx+1), 10))
			require.Equal(t, int64(tcIdx+1), d.PacketSeq, "PacketSeq should be "+strconv.FormatInt(int64(tcIdx+1), 10))
			require.Equal(t, tc.tsn, d.TrnsmitServerNo, "TrnsmitServerNo should be "+strconv.FormatInt(tc.tsn, 10))
			require.Equal(t, 1, d.DataNo, "DataNo should be 1")
			require.Equal(t, tc.packetMasterSeq, d.ServiceSeq, "ServiceSeq should be "+strconv.FormatInt(tc.packetMasterSeq, 10))
			require.Equal(t, tc.areaCode, d.AreaCode, "AreaCode should be "+tc.areaCode)
			require.Equal(t, tc.modelSerial, d.ModlSerial, "ModlSerial should be "+tc.modelSerial)
			require.Equal(t, 0, d.DqmCrrOp, "DqmCrrOp should be 0")
			require.Equal(t, time.Unix(0, tc.now).Format("20060102"), d.RegistDe, "RegistDe")
			require.Equal(t, tc.now, d.RegistDt, "RegistDt should be "+strconv.FormatInt(tc.now, 10))
			require.EqualValues(t, tc.values, d.Values, "Values should match expected values")
		}()
		// Create a Gin router group for the PoiServer
		router := s.Router()

		path := fmt.Sprintf("/n/api/send/%s/1/%d/%s/%s", tc.certKey, tc.pkSeq, tc.modelSerial, tc.data)
		w := performRequest(router, "GET", path, nil)
		if w.Code != http.StatusOK {
			t.Errorf("Expected status OK, got %v", w.Code)
		}
		require.JSONEq(t, `{"resultStats":{"resultCode":"SUCC-000", "resultMsg":"수신 완료"}}`, w.Body.String())
		// Wait for the parsing goroutine to finish
		parseWg.Wait()
	}
}
