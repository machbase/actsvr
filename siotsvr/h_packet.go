package siotsvr

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func (s *HttpServer) handleSendPacket(c *gin.Context) {
	tick := time.Now()
	packetSeq := int64(-1)
	defer func() {
		// Request의 패킷 정보를 로깅
		req := c.Request.URL.Path
		if c.Request.URL.RawQuery != "" {
			req += "?" + c.Request.URL.RawQuery
		}
		defaultLog.Info(packetSeq, " ", c.Writer.Status(), " ", time.Since(tick), " ", req)
	}()
	// Path params
	certkey := c.Param("certkey")        // string e.g. a2a3a4a5testauthkey9
	pkSeqStr := c.Param("pk_seq")        // integer e.g. 202008030000000301
	modelSerial := c.Param("serial_num") // string e.g. 3A4A50D
	packet := c.Param("packet")          // string data
	// Query params
	dqmcrrOpStr := c.Query("DQMCRR_OP") // 100 | 200
	dataNo := 1

	// Validate required parameters
	if certkey == "" || pkSeqStr == "" || modelSerial == "" || packet == "" {
		c.JSON(http.StatusOK, ApiErrorInvalidParameters)
		return
	}
	// pkSeq should be a valid integer
	pkSeq, err := strconv.ParseInt(pkSeqStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusOK, ApiErrorInvalidParameters)
		return
	}

	// find transmit server number from certificate key
	tsn, err := s.VerifyCertkey(certkey)
	if err != nil {
		c.JSON(http.StatusOK, ApiErrorInvalidCertkey)
		return
	}

	// find packet definition
	definition := getPacketDefinition(tsn, dataNo)
	if definition == nil {
		defaultLog.Errorf("No packet definition found for transmit server number: %d", tsn)
		c.JSON(http.StatusOK, ApiErrorServer)
		return
	}

	// get current time
	now := nowFunc().In(DefaultLocation)

	// Build RawPacketData
	packetSeq = nextPacketSeq()
	data := RawPacketData{
		PacketSeq:          packetSeq,
		TrnsmitServerNo:    tsn,
		DataNo:             dataNo,
		PkSeq:              pkSeq,
		ModlSerial:         modelSerial,
		Packet:             packet,
		PacketSttusCode:    PacketStatusOK,
		RecptnResultCode:   ApiReceiveSuccess.ResultStats.ResultCode,
		RecptnResultMssage: ApiReceiveSuccess.ResultStats.ResultMsg,
		ParsSeCode:         PacketParseAuto,
		RegistDe:           now.Format("20060102"),
		RegistTime:         now.Format("1504"),
		RegistDt:           now.UnixNano(),
	}

	// set DQMCCR_OP if it exists
	if dqmcrrOpStr != "" {
		if dqmcrrOp, err := strconv.Atoi(dqmcrrOpStr); err == nil {
			data.DqmCrrOp = fmt.Sprintf("%d", dqmcrrOp)
		}
	}

	// find AreaCode of the model by serial number
	if code, err := getModelAreaCode(modelSerial, tsn, dataNo); err != nil {
		defaultLog.Errorf("Failed to get area code for model_serial:%s, tsn:%d, dataNo:%d", modelSerial, tsn, dataNo)
	} else {
		data.AreaCode = code
	}

	// response message
	var ret = ApiReceiveSuccess

	// check packet length
	if len(packet) != definition.PacketSize {
		ret = ApiErrorInvalidPacketLength
		ret.ResultStats.ResultMsg = fmt.Sprintf("%s 패킷정의길이:%d(H:%d/T:%d),수신패킷길이:%d",
			ret.ResultStats.ResultMsg,
			definition.PacketSize,
			definition.HeaderSize,
			definition.DataSize,
			len(packet))
		data.PacketSttusCode = PacketStatusErr
	}
	// recptResult
	data.RecptnResultCode = ret.ResultStats.ResultCode
	data.RecptnResultMssage = ret.ResultStats.ResultMsg

	// insert packet data into database
	s.rawPacketCh <- &data
	c.JSON(http.StatusOK, ret)
}

// 앞의 0 패딩을 제거하는 정규표현식 (숫자 패턴 검증 및 변환을 위한)
var numericRegex = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)$`)

// removeLeadingZeros 함수: 문자열에서 앞의 0 패딩을 제거
// - 빈 문자열이나 "x", "X"만 있는 경우: 빈 문자열 반환
// - 숫자가 아닌 문자열: 원본 반환
// - 숫자 문자열: 앞의 0 제거 후 반환 (음수, 소수점 처리 포함)
func removeLeadingZeros(s string) string {
	// 빈 문자열 처리
	if s == "" {
		return ""
	}

	// "x" 또는 "X"만 있는 경우 (패딩 문자로 사용되는 경우)
	if len(s) > 0 && (s[0] == 'x' || s[0] == 'X') {
		trimmed := strings.Trim(s, "xX")
		if trimmed == "" {
			return ""
		}
	}

	// 숫자형 문자열이 아닐 경우 그대로 반환
	if !numericRegex.MatchString(s) {
		return s
	}

	// 부호 분리 (음수/양수 처리를 위해)
	sign := ""
	numStr := s
	if strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-") {
		sign = s[:1]
		numStr = s[1:]
	}

	// 소수점이 있는 경우
	if strings.Contains(numStr, ".") {
		parts := strings.Split(numStr, ".")
		intPart := strings.TrimLeft(parts[0], "0")
		if intPart == "" {
			intPart = "0"
		}
		return sign + intPart + "." + parts[1]
	}

	// 정수인 경우
	trimmed := strings.TrimLeft(numStr, "0")
	if trimmed == "" {
		// 0인 경우, 부호가 +/-인 경우에는 부호를 유지하지만,
		// 실제로는 0이므로 부호를 제거하는 것이 일반적
		// 하지만 테스트 요구사항에 맞춰 부호를 유지
		if sign == "+" || sign == "-" {
			return sign + "0"
		}
		return "0"
	}
	return sign + trimmed
}

func (s *HttpServer) parseRawPacket(data *RawPacketData) *ParsedPacketData {
	definition := getPacketDefinition(data.TrnsmitServerNo, data.DataNo)
	if definition == nil {
		defaultLog.Errorf("No packet definition found for transmit server number: %d", data.TrnsmitServerNo)
		return nil
	}
	packet := data.Packet
	// split packet into values
	values := make([]string, len(definition.Fields))
	for i, field := range definition.Fields {
		val := strings.TrimSpace(packet[0:field.PacketByte])
		// remove padding
		values[i] = removeLeadingZeros(val)
		packet = packet[field.PacketByte:]
	}

	// log packet parsing
	if defaultLog.DebugEnabled() {
		for i, field := range definition.Fields {
			defaultLog.Debugf("%d [%d] %s %s: %s", data.PacketSeq, i, field.PacketSeCode, field.PacketName, values[i])
		}
	}

	parsed := &ParsedPacketData{
		PacketParsSeq:   nextPacketParseSeq(),
		PacketSeq:       data.PacketSeq,
		TrnsmitServerNo: data.TrnsmitServerNo,
		DataNo:          data.DataNo,
		RegistDt:        data.RegistDt,
		RegistDe:        data.RegistDe,
		ServiceSeq:      definition.PacketMasterSeq,
		AreaCode:        data.AreaCode,
		ModlSerial:      data.ModlSerial,
		DqmCrrOp:        data.DqmCrrOp,
		Values:          values,
	}
	return parsed
}

func (s *HttpServer) handleServerStat(c *gin.Context) {
	certkey := c.Param("certkey")

	ret := struct {
		ResultStats struct {
			ResultCode string `json:"resultCode"`
			ResultMsg  string `json:"resultMsg"`
		} `json:"resultStats"`
	}{}

	if cacheCertKeys == nil {
		ret.ResultStats.ResultCode = "ERROR-900"
		ret.ResultStats.ResultMsg = "System Error"
	}

	cacheCertKeysMutex.RLock()
	key, exists := cacheCertKeys[certkey]
	cacheCertKeysMutex.RUnlock()
	if !exists {
		ret.ResultStats.ResultCode = "ERROR-200"
		ret.ResultStats.ResultMsg = "인증키가 올바르지 않습니다."
		c.JSON(http.StatusOK, ret)
		return
	}

	now := nowFunc()
	if now.Before(key.BeginValidDe) || now.After(key.EndValidDe) {
		ret.ResultStats.ResultCode = "ERROR-200"
		ret.ResultStats.ResultMsg = "인증키가 올바르지 않습니다."
		c.JSON(http.StatusOK, ret)
		return
	}
	ret.ResultStats.ResultCode = "READY-000"
	ret.ResultStats.ResultMsg = "수신 가능"
	c.JSON(http.StatusOK, ret)
}

const (
	PacketStatusOK    = "S" // 정상
	PacketStatusErr   = "E" // 오류
	PacketParseAuto   = "A" // 자동
	PacketParseManual = "M" // 수동
)

type RawPacketData struct {
	PacketSeq          int64
	TrnsmitServerNo    int64
	DataNo             int
	PkSeq              int64
	AreaCode           string
	ModlSerial         string
	DqmCrrOp           string
	Packet             string
	PacketSttusCode    string
	RecptnResultCode   string
	RecptnResultMssage string
	ParsSeCode         string
	RegistDe           string
	RegistTime         string
	RegistDt           int64 // Use int64 for timestamp
}

type ParsedPacketData struct {
	PacketParsSeq   int64
	PacketSeq       int64
	TrnsmitServerNo int64
	DataNo          int
	ServiceSeq      int64
	AreaCode        string
	ModlSerial      string
	DqmCrrOp        string
	RegistDt        int64  // Use int64 for timestamp
	RegistDe        string // 20060102
	Values          []string
}
