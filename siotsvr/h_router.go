package siotsvr

import (
	"embed"
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
	r.GET("/db/poi/nearby", s.handlePoiNearby)
	r.POST("/db/poi/reload", s.handlePoiReload)
	r.GET("/n/api/serverstat/:certkey", s.handleServerStat)
	r.GET("/n/api/send/:certkey/1/:pk_seq/:serial_num/:packet", s.handleSendPacket)
	r.NoRoute(func(c *gin.Context) {
		c.String(http.StatusNotFound, "404 Not Found")
	})
	return r
}

func (s *HttpServer) VerifyCertkey(certkey string) (int64, error) {
	key, err := getCertKey(certkey)
	if err != nil {
		return 0, err
	}
	now := nowFunc()
	if now.Before(key.BeginValidDe) || now.After(key.EndValidDe) {
		return 0, fmt.Errorf("certificate key %q is not valid", certkey)
	}
	if !key.TrnsmitServerNo.Valid {
		return 0, fmt.Errorf("certificate key %q has invalid tsn", certkey)
	}
	return key.TrnsmitServerNo.Int64, nil
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
			ResultCode: "SUCC-000",
			ResultMsg:  "수신 완료",
		},
	}
	ApiReady = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "READY-000",
			ResultMsg:  "수신 가능",
		},
	}
	ApiErrorWrongCertkey = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-200",
			ResultMsg:  "인증키가 올바르지 않습니다.",
		},
	}
	ApiErrorExpiredCertkey = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-210",
			ResultMsg:  "인증키 유효기관을 확인 바립니다.",
		},
	}
	ApiErrorInvalidCertkey = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-210",
			ResultMsg:  "인증키가 유효하지 않습니다.",
		},
	}

	ApiErrorInvalidPacketLength = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-300",
			ResultMsg:  "패킷의 길이가 정의서와 다릅니다.",
		},
	}
	ApiErrorInvalidPacketLegend = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-310",
			ResultMsg:  "패킷 데이터의 범주가 정의서와 다릅니다.",
		},
	}
	ApiErrorServer = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-500",
			ResultMsg:  "서버 오류입니다. 지속적으로 발생시 담당기관의 문의 바랍니다.",
		},
	}
	ApiErrorUnknownDevice = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-600",
			ResultMsg:  "미등록 기기의 패킷 데이터 입니다.",
		},
	}
	ApiErrorIntervalExceeded = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-610",
			ResultMsg:  "수신 간격 초과 패킷 데이터 입니다.",
		},
	}
	ApiErrorLegend = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-620",
			ResultMsg:  "범례 초과 패킷 데이터 입니다.",
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
	ApiErrorOther = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-900",
			ResultMsg:  "기타 에러",
		},
	}
)
