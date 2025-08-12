package siotsvr

import (
	"embed"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/tochemey/goakt/v3/log"
)

//go:embed static
var htmlFS embed.FS

func (s *HttpServer) Router() *gin.Engine {
	if s.router == nil {
		s.router = s.buildRouter()
	}
	return s.router
}

func (s *HttpServer) buildRouter() *gin.Engine {
	if s.log != nil && s.log.LogLevel() == log.DebugLevel {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	r.GET("/", func(c *gin.Context) { c.Redirect(http.StatusFound, "/static/") })
	r.GET("/static/", gin.WrapF(http.FileServerFS(htmlFS).ServeHTTP))
	r.GET("/db/write/RecptnPacketData", s.writeRecptnPacketData)
	r.POST("/db/write/RecptnPacketData", s.writeRecptnPacketData)
	r.GET("/db/write/PacketParsData", s.writePacketParsData)
	r.POST("/db/write/PacketParsData", s.writePacketParsData)
	r.GET("/n/api/serverstat/:certkey", s.handleServerStat)
	r.GET("/n/api/send/:certkey/1/:pk_seq/:serial_num/:packet", s.handleSendPacket)
	r.NoRoute(func(c *gin.Context) {
		c.String(http.StatusNotFound, "404 Not Found")
	})
	return r
}

func (s *HttpServer) handleSendPacket(c *gin.Context) {
	certkey := c.Param("certkey")      // string e.g. a2a3a4a5testauthkey9
	pkSeq := c.Param("pk_seq")         // integer e.g. 202008030000000301
	serialNum := c.Param("serial_num") // string e.g. 3A4A50D
	packet := c.Param("packet")        // string data
	dqmcrrOp := c.Query("dqmcrr_op")   // 100 | 200

	if certkey == "" || pkSeq == "" || serialNum == "" || packet == "" {
		c.JSON(http.StatusOK, ApiErrorInvalidParameters)
		return
	}

	_ = dqmcrrOp // Ignore dqmcrr_op for now

	if false { // If package length is wrong
		ret := ApiErrorInvalidPacketLength
		ret.ResultStats.ResultMsg = fmt.Sprintf("%s 패킷정의길이:%d(H:15/T7),수신패킷길이:%d",
			ret.ResultStats.ResultMsg, 0, 1)
		c.JSON(http.StatusOK, ret)
		return
	}
	valid, err := s.VerifyCertkey(certkey, 1) // Assuming 1 is the transmit server number
	if err != nil || !valid {
		c.JSON(http.StatusOK, ApiErrorInvalidCertkey)
		return
	}

	c.JSON(http.StatusOK, ApiReceiveSuccess)
}

func (s *HttpServer) handleServerStat(c *gin.Context) {
	certkey := c.Param("certkey")

	ret := struct {
		ResultStats struct {
			ResultCode string `json:"resultCode"`
			ResultMsg  string `json:"resultMsg"`
		} `json:"resultStats"`
	}{}

	if s.certKeys == nil {
		ret.ResultStats.ResultCode = "ERROR-900"
		ret.ResultStats.ResultMsg = "System Error"
	}

	s.certKeysMutex.RLock()
	key, exists := s.certKeys[certkey]
	s.certKeysMutex.RUnlock()
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
	ret.ResultStats.ResultMsg = "수신가능"
	c.JSON(http.StatusOK, ret)
}

func (s *HttpServer) VerifyCertkey(certkey string, tsn int64) (bool, error) {
	s.certKeysMutex.RLock()
	defer s.certKeysMutex.RUnlock()
	if s.certKeys == nil {
		return false, errors.New("certificate keys not loaded")
	}
	key, exists := s.certKeys[certkey]
	if !exists {
		return false, fmt.Errorf("unknown certificate key: %q", certkey)
	}
	now := nowFunc()
	if now.Before(key.BeginValidDe) || now.After(key.EndValidDe) {
		return false, fmt.Errorf("certificate key %q is not valid", certkey)
	}
	if !key.TrnsmitServerNo.Valid || key.TrnsmitServerNo.Int64 != tsn {
		return false, fmt.Errorf("certificate key %q is not valid for transmit server %d", certkey, tsn)
	}
	return true, nil
}

type ApiResult struct {
	ResultStats ApiResultStats `json:"resultStats"`
}
type ApiResultStats struct {
	ResultCode string `json:"resultCode"`
	ResultMsg  string `json:"resultMsg"`
}

var (
	ApiReceiveSuccess = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "SUCCESS-000",
			ResultMsg:  "수신완료",
		},
	}
	ApiErrorInvalidCertkey = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-200",
			ResultMsg:  "인증키가 올바르지 않습니다.",
		},
	}
	ApiErrorInvalidPacketLength = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-300",
			ResultMsg:  "패킷의 길이가 정의서와 다릅니다.",
		},
	}
	ApiErrorUnknownDevice = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-600",
			ResultMsg:  "미등록 기기입니다.",
		},
	}
	ApiErrorIntervalExceeded = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-610",
			ResultMsg:  "수신간격초과입니다.",
		},
	}
	ApiErrorLegend = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-620",
			ResultMsg:  "범례 오류 입니다.",
		},
	}
	ApiErrorInvalidPacket = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-630",
			ResultMsg:  "패킷 구조 오류입니다.",
		},
	}
	ApiErrorTrafficLimitExceeded = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-640",
			ResultMsg:  "일일 전송량 초과입니다.",
		},
	}
	ApiErrorInvalidParameters = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-700",
			ResultMsg:  "잘못된 파라미터입니다.",
		},
	}
	ApiErrorSystemError = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-900",
			ResultMsg:  "시스템 오류입니다.",
		},
	}
)
