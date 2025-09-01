package siotsvr

import (
	"actsvr/util"
	"context"
	"embed"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/machbase/neo-server/v8/api/machcli"
	"github.com/tochemey/goakt/v3/log"
)

type HttpServer struct {
	Host      string
	Port      int
	KeepAlive int // seconds
	DataDir   string

	machCli    *machcli.Database
	log        *util.Log
	httpServer *http.Server
	router     *gin.Engine

	rawPacketCh  chan *RawPacketData
	errPacketCh  chan *RawPacketData
	parsPacketCh chan *ParsedPacketData

	replicaAlive bool
	replicaWg    sync.WaitGroup
}

func NewHttpServer() *HttpServer {
	return &HttpServer{
		Host:         "0.0.0.0",
		Port:         8888,
		DataDir:      "/tmp",
		rawPacketCh:  make(chan *RawPacketData),
		errPacketCh:  make(chan *RawPacketData),
		parsPacketCh: make(chan *ParsedPacketData),
		replicaAlive: true,
	}
}

func (s *HttpServer) Start(ctx context.Context) error {
	s.log = DefaultLog()
	if err := reloadPoiData(); err != nil {
		return err
	}
	if err := reloadCertkey(); err != nil {
		return err
	}
	if err := reloadPacketDefinition(); err != nil {
		return err
	}
	if err := reloadModelAreaCode(); err != nil {
		return err
	}
	if err := s.openDatabase(); err != nil {
		return err
	}
	if err := s.reloadPacketSeq(); err != nil {
		return err
	}
	if err := s.reloadPacketParseSeq(); err != nil {
		return err
	}
	go s.loopRawPacket()
	go s.loopErrPacket()
	go s.loopParsPacket()
	go s.loopReplicaRawPacket()
	go s.loopReplicaParsPacket()

	s.httpServer = &http.Server{
		ConnContext: s.httpContext,
		Handler:     s.Router(),
	}
	lsnr, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.Host, s.Port))
	if err != nil {
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}
	go s.httpServer.Serve(lsnr)
	s.log.Infof("Starting HTTP server on %s:%d", s.Host, s.Port)
	return nil
}

func (s *HttpServer) Stop(ctx context.Context) (err error) {
	if s.httpServer != nil {
		err = s.httpServer.Shutdown(ctx)
	}
	s.replicaAlive = false
	s.replicaWg.Wait()
	s.closeDatabase()
	close(s.parsPacketCh)
	close(s.rawPacketCh)
	s.log.Infof("Stopping HTTP server on %s:%d", s.Host, s.Port)
	return
}

func (s *HttpServer) httpContext(ctx context.Context, c net.Conn) context.Context {
	if tcpCon, ok := c.(*net.TCPConn); ok && tcpCon != nil {
		tcpCon.SetNoDelay(true)
		if s.KeepAlive > 0 {
			tcpCon.SetKeepAlive(true)
			tcpCon.SetKeepAlivePeriod(time.Duration(s.KeepAlive) * time.Second)
		}
		tcpCon.SetLinger(0)
	}
	return ctx
}

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
	r.Use(gin.Recovery())
	r.GET("/", func(c *gin.Context) { c.Redirect(http.StatusFound, "/static/") })
	r.GET("/static/", gin.WrapF(http.FileServerFS(htmlFS).ServeHTTP))
	r.GET("/db/admin/log", s.handleAdminLog)
	r.GET("/db/admin/reload", s.handleAdminReload)
	r.GET("/db/admin/replica", s.handleAdminReplica)
	r.Any("/debug/pprof/*path", gin.WrapF(pprof.Index))
	r.Use(CollectorMiddleware)
	r.GET("/db/poi/nearby", s.handlePoiNearby)
	r.GET("/n/api/serverstat/:certkey", s.handleServerStat)
	r.GET("/n/api/send/:certkey/:data_no/:pk_seq/:serial_num/:packet", s.handleSendPacket)
	r.GET("/n/api/servers/:tsn/data/:data_no", s.handleData)
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

func (s *HttpServer) handleAdminLog(c *gin.Context) {
	verbose := c.Query("verbose")
	switch verbose {
	case "1":
		defaultLog.SetVerbose(1)
	case "2":
		defaultLog.SetVerbose(2)
	default:
		defaultLog.SetVerbose(0)
	}
	c.String(http.StatusOK, fmt.Sprintf("set log verbose: %d", defaultLog.LogLevel()))
}

func (s *HttpServer) handleAdminReload(c *gin.Context) {
	var err error
	target := c.Query("target")
	switch strings.ToLower(target) {
	case "poi":
		err = reloadPoiData()
	case "certkey":
		err = reloadCertkey()
	case "model":
		err = reloadPacketDefinition()
	case "model_areacode":
		err = reloadModelAreaCode()
	case "packet_seq":
		err = s.reloadPacketSeq()
	case "packet_parse_seq":
		err = s.reloadPacketParseSeq()
	case "last_packet":
		packetDataArrivalTime.Lock()
		newVal := c.Query("new_value")
		if newVal == "" {
			packetDataArrivalTime.Load()
		} else if ts, err2 := time.ParseInLocation("2006-01-02 15:04:05.000000000", newVal, DefaultTZ); err2 == nil {
			packetDataArrivalTime.Time = ts
			packetDataArrivalTime.Save()
		} else {
			err = err2
		}
		packetDataArrivalTime.Unlock()
	case "last_pars":
		parsDataArrivalTime.Lock()
		newVal := c.Query("new_value")
		if newVal == "" {
			parsDataArrivalTime.Load()
		} else if ts, err2 := time.ParseInLocation("2006-01-02 15:04:05.000000000", newVal, DefaultTZ); err2 == nil {
			parsDataArrivalTime.Time = ts
			parsDataArrivalTime.Save()
		} else {
			err = err2
		}
		parsDataArrivalTime.Unlock()
	default:
		c.String(http.StatusNotFound, "Unknown reload target: %s", target)
	}
	if err != nil {
		c.String(http.StatusInternalServerError, "Failed to reload %s: %v", target, err)
	} else {
		c.String(http.StatusOK, "Reloaded %s successfully", target)
	}
}

func (s *HttpServer) handleAdminReplica(c *gin.Context) {
	nrows := c.Query("rows")
	if nrows != "" {
		// If replicaRowsPerRun is set to zero, replication processes will pause.
		if n, err := strconv.Atoi(nrows); err != nil {
			c.String(http.StatusBadRequest, "Invalid rows parameter: %v", err)
			return
		} else {
			replicaRowsPerRun = n
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"rows": replicaRowsPerRun,
	})
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
			ResultMsg:  "인증키 유효기간을 확인 바립니다.",
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
