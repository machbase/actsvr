package siotsvr

import (
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func (s *HttpServer) handleSendPacket(c *gin.Context) {
	tick := time.Now()
	packetSeq := int64(-1)
	var requestErr string
	defer func() {
		// Request의 패킷 정보를 로깅
		req := c.Request.URL.Path
		if c.Request.URL.RawQuery != "" {
			req += "?" + c.Request.URL.RawQuery
		}
		if packetSeq != -1 {
			defaultLog.Info(packetSeq, " ", c.Writer.Status(), " ", time.Since(tick), " ", req)
		} else {
			defaultLog.Warn(requestErr, " ", c.Writer.Status(), " ", time.Since(tick), " ", req)
		}
	}()
	// Path params
	certkey := c.Param("certkey") // string e.g. a2a3a4a5testauthkey9
	dataNoStr := c.Param("data_no")
	pkSeqStr := c.Param("pk_seq")        // integer e.g. 202008030000000301
	modelSerial := c.Param("serial_num") // string e.g. 3A4A50D
	packet := c.Param("packet")          // string data
	// Query params
	dqmcrrOpStr := c.Query("DQMCRR_OP") // 100 | 200

	// Validate required parameters
	if certkey == "" || pkSeqStr == "" || modelSerial == "" || packet == "" || dataNoStr == "" {
		requestErr = "empty_params"
		c.JSON(http.StatusOK, ApiErrorInvalidParameters)
		return
	}
	// data_no should be a valid integer
	var dataNo int
	if no, err := strconv.Atoi(dataNoStr); err != nil {
		requestErr = "invalid_data_no"
		c.JSON(http.StatusOK, ApiErrorInvalidParameters)
		return
	} else {
		dataNo = no
	}
	// pkSeq should be a valid integer
	pkSeq, err := strconv.ParseInt(pkSeqStr, 10, 64)
	if err != nil {
		requestErr = "invalid_pk_seq"
		c.JSON(http.StatusOK, ApiErrorInvalidParameters)
		return
	}

	// find transmit server number from certificate key
	tsn, err := s.VerifyCertkey(certkey)
	if err != nil {
		requestErr = "wrong_certkey"
		c.JSON(http.StatusOK, ApiErrorInvalidCertkey)
		return
	}

	// find packet definition
	searchDataNo := 1 // always use data_no = 1 for model data
	definition := getPacketDefinition(tsn, searchDataNo)
	if definition == nil {
		requestErr = "no_modl_detail"
		defaultLog.Errorf("No packet definition found for tsn: %d, data_no:%d", tsn, searchDataNo)
		c.JSON(http.StatusOK, ApiErrorServer)
		return
	}

	// get current time
	now := nowFunc().In(DefaultTZ)

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
	if code, err := getModelAreaCode(modelSerial, tsn, searchDataNo); err != nil {
		defaultLog.Errorf("%d Failed to get area code for model_serial:%s, tsn:%d, dataNo:%d", packetSeq, modelSerial, tsn, searchDataNo)
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
	if data.RecptnResultCode != ApiReceiveSuccess.ResultStats.ResultCode {
		s.errPacketCh <- &data
	}
	c.JSON(http.StatusOK, ret)
}

// Regular expression for removing leading zero padding
// (for numeric pattern validation and conversion)
var numericRegex = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)$`)

// removeLeadingZeros function: removes leading zero padding from a string
// - If the s is empty or contains only "x" or "X": returns an empty string
// - If the s is not numeric: returns the original string
// - If the s is numeric: removes leading zeros and returns the result (handles negative numbers and decimals)
func removeLeadingZeros(s string) string {
	// empty string
	if s == "" {
		return ""
	}

	// If the string contains only "x" or "X" (used as padding character)
	if len(s) > 0 && (s[0] == 'x' || s[0] == 'X') {
		trimmed := strings.Trim(s, "xX")
		if trimmed == "" {
			return ""
		}
	}

	// If the string is not numeric, return it as is
	if !numericRegex.MatchString(s) {
		return s
	}

	// Separate sign (for handling negative/positive numbers)
	sign := ""
	numStr := s
	if strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-") {
		sign = s[:1]
		numStr = s[1:]
	}

	// If there is a decimal point
	if strings.Contains(numStr, ".") {
		parts := strings.Split(numStr, ".")
		intPart := strings.TrimLeft(parts[0], "0")
		if intPart == "" {
			intPart = "0"
		}
		return sign + intPart + "." + parts[1]
	}

	// For integer values
	trimmed := strings.TrimLeft(numStr, "0")
	if trimmed == "" {
		// If the value is 0, normally the sign (+/-) is removed,
		// but to meet test requirements, keep the sign if present.
		if sign == "+" || sign == "-" {
			return sign + "0"
		}
		return "0"
	}
	return sign + trimmed
}

type ValidateError struct {
	TransmitServerNo int64
	FieldName        string
	RuleType         string
	Rule             string
	FailedValue      string
}

func (pe *ValidateError) Error() string {
	return fmt.Sprintf("packet validation error, tsn:%d, field:%s, value:%s, rule:%s(%s)",
		pe.TransmitServerNo, pe.FieldName, pe.FailedValue, pe.RuleType, pe.Rule)
}

var _ error = (*ValidateError)(nil)

func (s *HttpServer) parseRawPacket(data *RawPacketData) (*ParsedPacketData, error) {
	// Get packet definition by data_no = 1 instead of data.DataNo
	searchDataNo := 1
	definition := getPacketDefinition(data.TrnsmitServerNo, searchDataNo)
	if definition == nil {
		return nil, fmt.Errorf("no packet definition found for transmit server number: %d", data.TrnsmitServerNo)
	}
	packet := data.Packet
	// split packet into values
	values := make([]string, len(definition.Fields))
	for i, field := range definition.Fields {
		val := strings.TrimSpace(packet[0:field.PacketByte])
		// remove padding
		val = removeLeadingZeros(val)
		// validation
		switch field.RuleType {
		case "VAL_ITV":
			v, err := strconv.ParseFloat(val, 64) // just check if it's numeric
			if err != nil {
				return nil, &ValidateError{
					TransmitServerNo: data.TrnsmitServerNo,
					FieldName:        field.PacketName,
					RuleType:         field.RuleType,
					Rule:             "not a numeric value",
					FailedValue:      val,
				}
			}
			if v < field.MinValue || v > field.MaxValue {
				return nil, &ValidateError{
					TransmitServerNo: data.TrnsmitServerNo,
					FieldName:        field.PacketName,
					RuleType:         field.RuleType,
					Rule:             fmt.Sprintf("%v~%v", field.MinValue, field.MaxValue),
					FailedValue:      val,
				}
			}
		case "VAL_ARR":
			arr := strings.Split(field.ArrValue, ",")
			if !slices.Contains(arr, val) {
				return nil, &ValidateError{
					TransmitServerNo: data.TrnsmitServerNo,
					FieldName:        field.PacketName,
					RuleType:         field.RuleType,
					Rule:             field.ArrValue,
					FailedValue:      val,
				}
			}
		}
		values[i] = val
		packet = packet[field.PacketByte:]
	}

	// log packet parsing
	if defaultLog.DebugEnabled() {
		for i, field := range definition.Fields {
			dc := ""
			if field.DC != "" {
				dc = fmt.Sprintf(" / %s", field.DC)
			}
			defaultLog.Debugf("%d [%d] %s %s%s: %s",
				data.PacketSeq, i, field.PacketSeCode, field.PacketName, dc, values[i])
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
	return parsed, nil
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
