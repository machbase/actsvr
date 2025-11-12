package siotsvr

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPkSeq(t *testing.T) {
	pkSeqStr := "2025-11-12_17:07:00940123"
	pkSeqStr = strings.Map(func(r rune) rune {
		if r == '-' || r == '_' || r == ':' {
			return -1
		}
		return r
	}, pkSeqStr)
	maxPkSeqLen := 18
	if len(pkSeqStr) > maxPkSeqLen {
		pkSeqStr = pkSeqStr[:maxPkSeqLen]
	}
	pkSeq, err := strconv.ParseInt(pkSeqStr, 10, 64)
	require.NoError(t, err, len(pkSeqStr))
	require.True(t, pkSeq > 0, "pkSeq should be greater than 0")
}

func TestPacketSend(t *testing.T) {
	defaultLog = DefaultLog()
	defaultLog.SetVerbose(2)
	nowFunc = func() time.Time {
		return time.Unix(0, 1755129018971253000)
	}
	if err := reloadCertKey(); err != nil {
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
			"DqmCrrOp":"",
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
		parsed, err := s.parseRawPacket(d)
		require.NoError(t, err, "Failed to parse raw packet data")
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
		"ServiceSeq":55, 
		"AreaCode":"11200", 
		"ModlSerial":"SCMS-SEOUL-SD-0006", 
		"DqmCrrOp":"", 
		"RegistDt":1755129018971253000, 
		"RegistDe":"20250814", 
		"Values":["SCMS-SEOUL-SD", "SCMS-SEOUL-SD-0006", "4"]}`
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
	if err := reloadCertKey(); err != nil {
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
			values:          []string{"SCMS-SEOUL-SD", "SCMS-SEOUL-SD-0006", "4"},
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
				"SDOT001", "V02Q1940343", "1", "2", "29.1", "75", "", "", "", "",
				"0", "0.0", "44", "0.03", "0.02", "1.03", "0.08", "0.19", "1.09",
				"", "1", "2", "202506302359", "0.0", "", "", "", "", "", "", ""},
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
			values:          []string{"IMC_1000A", "c8c3a7e5abea7", "25", "71", "4", "3", "927", "2131"},
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
			require.Equal(t, "", d.DqmCrrOp, "DqmCrrOp should be 0")
			require.Equal(t, tc.data, d.Packet, "Packet should be "+tc.data)
			require.Equal(t, PacketStatusOK, d.PacketSttusCode, "PacketSttusCode should be S")
			require.Equal(t, "SUCC-000", d.RecptnResultCode, "RecptnResultCode should be SUCC-000")
			require.Equal(t, "수신 완료", d.RecptnResultMssage, "RecptnResultMssage should be 수신 완료")
			require.Equal(t, PacketParseAuto, d.ParsSeCode, "ParsSeCode should be A")
			require.Equal(t, time.Unix(0, tc.now).Format("20060102"), d.RegistDe, "RegistDe")
			require.Equal(t, time.Unix(0, tc.now).Format("1504"), d.RegistTime, "RegistTime")
			require.Equal(t, tc.now, d.RegistDt, "RegistDt should be "+strconv.FormatInt(tc.now, 10))
			parsed, err := s.parseRawPacket(d)
			require.NoError(t, err, "Failed to parse raw packet data")
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
			require.Equal(t, "", d.DqmCrrOp, "DqmCrrOp should be 0")
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

func TestRemoveLeadingZeros(t *testing.T) {
	tests := []struct {
		input    string
		expected string
		desc     string
	}{
		// 기본 숫자 처리
		{"00123", "123", "기본 앞의 0 제거"},
		{"0000", "0", "모든 0은 단일 0으로"},
		{"00", "0", "두 개의 0은 단일 0으로"},
		{"0", "0", "단일 0은 그대로"},

		// 소수점 처리
		{"0.123", "0.123", "소수점 앞의 0은 유지"},
		{"000.123", "0.123", "소수점 앞의 여러 0은 단일 0으로"},
		{"216.0", "216.0", "일반 소수점은 그대로"},
		{"127.", "127.", "소수점 뒤에 0이 없는 경우는 그대로"},
		{"123.456", "123.456", "일반 소수점은 그대로"},
		{"000123.456", "123.456", "소수점 앞의 정수부 0 제거"},
		{"0000.0000", "0.0000", "소수점 앞의 0들을 단일 0으로"},

		// 음수 처리
		{"-00123", "-123", "음수의 앞의 0 제거"},
		{"-000", "-0", "음수 0 처리"},
		{"-0.123", "-0.123", "음수 소수점 처리"},
		{"-000123.456", "-123.456", "음수 소수점의 앞의 0 제거"},

		// 양수 부호 처리
		{"+00123", "+123", "양수 부호의 앞의 0 제거"},
		{"+000", "+0", "양수 부호 0 처리"},
		{"+0.123", "+0.123", "양수 부호 소수점 처리"},

		// 특수 문자 처리 (패딩용)
		{"xxx", "", "x 패딩 문자는 빈 문자열로"},
		{"XXX", "", "X 패딩 문자는 빈 문자열로"},
		{"xxXXxx", "xxXXxx", "혼합 패딩 문자는 그대로"},
		{"x123", "x123", "x가 포함된 일반 문자열은 그대로"},

		// 문자열 처리 (숫자가 아닌 경우)
		{"00ABC", "ABC", "숫자가 아닌 문자열은 0으로 패딩"},
		{"ABC123", "ABC123", "문자가 포함된 문자열은 그대로"},
		{"12AB34", "12AB34", "중간에 문자가 있으면 그대로"},
		{"0000000main_street", "main_street", "0으로 패딩된 문자열"},

		// 빈 문자열
		{"", "", "빈 문자열은 그대로"},

		// 에지 케이스
		{".123", "0.123", "소수점으로 시작하는 숫자는 0을 앞에 붙여 정규화"},
		{"000.000", "0.000", "소수점 양쪽 모두 0인 경우"},
		{"000000000", "0", "많은 0들은 단일 0으로"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			result := removeLeadingZeros(tt.input)
			require.Equal(t, tt.expected, result, "입력: %q, 기대값: %q, 실제값: %q", tt.input, tt.expected, result)
		})
	}
}
