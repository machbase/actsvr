package siotsvr

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

func (s *HttpServer) handleSendPacket(c *gin.Context) {
	// Request의 패킷 정보를 로깅
	if c.Request.URL.RawQuery != "" {
		defaultLog.Info(c.Request.URL.Path + "?" + c.Request.URL.RawQuery)
	} else {
		defaultLog.Info(c.Request.URL.Path)
	}
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
	definition := getPacketDefinition(tsn)
	if definition == nil {
		defaultLog.Errorf("No packet definition found for transmit server number: %d", tsn)
		c.JSON(http.StatusOK, ApiErrorServer)
		return
	}

	// get current time
	now := nowFunc().In(DefaultLocation)

	// Build RawPacketData
	data := RawPacketData{
		PacketSeq:          nextPacketSeq(),
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
			data.DqmCrrOp = dqmcrrOp
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

func (s *HttpServer) parseRawPacket(data *RawPacketData) *ParsedPacketData {
	definition := getPacketDefinition(data.TrnsmitServerNo)
	if definition == nil {
		defaultLog.Errorf("No packet definition found for transmit server number: %d", data.TrnsmitServerNo)
		return nil
	}
	packet := data.Packet
	// split packet into values
	values := make([]string, len(definition.Fields))
	for i, field := range definition.Fields {
		values[i] = strings.TrimSpace(packet[0:field.PacketByte])
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
	DqmCrrOp           int
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
	DqmCrrOp        int
	RegistDt        int64  // Use int64 for timestamp
	RegistDe        string // 20060102
	Values          []string
}
