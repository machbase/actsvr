package siotsvr

import (
	"actsvr/util"
	"context"
	"embed"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/machbase/neo-server/v8/api"
	"github.com/machbase/neo-server/v8/api/machcli"
	"github.com/machbase/neo-server/v8/mods/util/metric"
	"github.com/machbase/neo-server/v8/mods/util/tailer"
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
	statCh       chan *StatDatum

	replicaAlive bool
	replicaWg    sync.WaitGroup

	collector *metric.Collector
}

func NewHttpServer() *HttpServer {
	return &HttpServer{
		Host:         "0.0.0.0",
		Port:         8888,
		DataDir:      "", // content directory for /data/*, if not set, /data/* is not served
		rawPacketCh:  make(chan *RawPacketData),
		errPacketCh:  make(chan *RawPacketData),
		parsPacketCh: make(chan *ParsedPacketData),
		statCh:       make(chan *StatDatum),
		replicaAlive: true,
	}
}

func (s *HttpServer) Start(ctx context.Context) error {
	s.log = DefaultLog()
	if err := reloadPoiData(); err != nil {
		return err
	}
	if err := reloadCertKey(); err != nil {
		return err
	}
	if err := reloadOrgKey(); err != nil {
		return err
	}
	if err := reloadPacketDefinition(); err != nil {
		return err
	}
	if err := reloadModelAreaCode(); err != nil {
		return err
	}
	if err := reloadModelDqmInfo(); err != nil {
		return err
	}
	if err := reloadModelOrgnPublic(); err != nil {
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
	s.collector = Collector(s.onProduct)
	s.collector.Start()

	go s.loopRawPacket()
	go s.loopErrPacket()
	go s.loopParsPacket()
	go s.loopStatData()
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
		// shutdown any background tailer tasks
		tailer.Shutdown()
		err = s.httpServer.Shutdown(ctx)
	}
	s.collector.Stop()
	s.replicaAlive = false
	s.replicaWg.Wait()
	s.closeDatabase()
	close(s.parsPacketCh)
	close(s.rawPacketCh)
	close(s.statCh)
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

	rootPath := "/static/"
	if s.DataDir != "" {
		rootPath = "/data/"
	}

	r.GET("/", func(c *gin.Context) { c.Redirect(http.StatusFound, rootPath) })
	r.GET("/static/*path", gin.WrapF(http.FileServerFS(htmlFS).ServeHTTP))
	if s.DataDir != "" {
		r.GET("/data/*path", gin.WrapF(http.FileServer(http.Dir(s.DataDir)).ServeHTTP))
	}
	r.GET("/db/admin/log", s.handleAdminLog)
	r.GET("/db/admin/reload", s.handleAdminReload)
	r.GET("/db/admin/stat/:begin_date/:end_date", s.handleAdminStat)
	r.GET("/db/admin/replica", s.handleAdminReplica)
	r.Any("/debug/pprof/*path", gin.WrapF(pprof.Index))
	r.GET("/debug/dashboard", gin.WrapH(CollectorHandler()))
	if lf := logConfig.Filename; lf != "" && lf != "-" {
		h := tailer.NewHandler("/debug/logs/", lf,
			tailer.WithLastN(50),
			tailer.WithPlugins(tailer.NewColoring("default")),
		)
		h.TerminalOpts.FontSize = 11
		r.GET("/debug/logs/*path", gin.WrapH(h))
	}
	if trcLogfile != "" {
		h := tailer.NewHandler("/debug/trc/", trcLogfile,
			tailer.WithLastN(50),
			tailer.WithPlugins(tailer.NewColoring("default")),
		)
		h.TerminalOpts.FontSize = 11
		r.GET("/debug/trc/*path", gin.WrapH(h))
	}
	r.Use(CollectorMiddleware)
	r.GET("/db/poi/nearby", s.handlePoiNearby)
	r.GET("/n/api/serverstat/:certkey", s.handleServerStat)
	r.GET("/n/api/send/:certkey/:data_no/:pk_seq/:serial_num/:packet", s.handleSendPacket)
	r.GET("/api/send/:certkey/:data_no/:pk_seq/:serial_num/:packet", s.handleSendPacket) // legacy sensors
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
		err = reloadCertKey()
	case "orgkey":
		err = reloadOrgKey()
	case "model":
		err = reloadPacketDefinition()
	case "model_areacode":
		err = reloadModelAreaCode()
	case "model_dqm":
		err = reloadModelDqmInfo()
	case "orgnpublic":
		err = reloadModelOrgnPublic()
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

func (s *HttpServer) loadStatNames(ctx context.Context) ([]string, error) {
	conn, err := s.openConn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	sqlText := fmt.Sprintf("SELECT NAME FROM _%s_META WHERE NAME LIKE 'stat:nrow:%%' ORDER BY NAME", statTagTable)
	rows, err := conn.Query(ctx, sqlText)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

func (s *HttpServer) handleAdminStat(c *gin.Context) {
	beginDateStr := c.Param("begin_date") // inclusive
	endDateStr := c.Param("end_date")     // exclusive

	beginTime, err := time.ParseInLocation("20060102", beginDateStr, DefaultTZ)
	if err != nil {
		defaultLog.Errorf("Invalid begin_date parameter: %v", err)
		c.JSON(http.StatusBadRequest, ApiErrorInvalidParameters)
		return
	}
	endTime, err := time.ParseInLocation("20060102", endDateStr, DefaultTZ)
	if err != nil {
		defaultLog.Errorf("Invalid end_date parameter: %v", err)
		c.JSON(http.StatusBadRequest, ApiErrorInvalidParameters)
		return
	}
	endTime = endTime.Add(24 * time.Hour) // Make end date inclusive by adding one day

	names, err := s.loadStatNames(c)
	if err != nil {
		defaultLog.Errorf("Failed to load stat names: %v", err)
		c.String(http.StatusInternalServerError, "Failed to load stat names: %v", err)
		return
	}
	conn, err := s.openConn(c)
	if err != nil {
		defaultLog.Errorf("Failed to open DB connection: %v", err)
		c.String(http.StatusInternalServerError, "Failed to open DB connection: %v", err)
		return
	}
	defer conn.Close()

	c.Header("Content-Type", "text/csv")
	c.Writer.WriteString("DATE,ORG,TSN,COUNT,RECORDS\n")
	for _, name := range names {
		if err := fetchStatRows(c, conn, beginTime, endTime, name, c.Writer); err != nil {
			defaultLog.Errorf("Failed to query stat %q: %v", name, err)
			c.String(http.StatusInternalServerError, "Failed to execute query: %v", err)
			return
		}
	}
	c.Writer.WriteString("\n")
}

func fetchStatRows(ctx context.Context, conn api.Conn, beginTime time.Time, endTime time.Time, name string, w io.Writer) error {
	sqlText := fmt.Sprintf(`SELECT TO_CHAR(DATE_TRUNC('day', TIME, 1), 'YYYYMMDD') DATE, COUNT(*) CNT, SUM(VALUE) RECS
FROM (
    SELECT
        TIME, VALUE
    FROM
        %s
    WHERE TIME >= ? AND TIME < ? AND NAME = ?
	ORDER BY TIME
)
GROUP BY DATE`, statTagTable)

	nameParts := strings.SplitN(strings.TrimPrefix(name, "stat:nrow:"), ":", 2)
	if len(nameParts) != 2 {
		return fmt.Errorf("invalid stat name format: %s", name)
	}

	rows, err := conn.Query(ctx, sqlText, beginTime.UnixNano(), endTime.UnixNano(), name)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var date string
		var count int64
		var value string
		if err := rows.Scan(&date, &count, &value); err != nil {
			return err
		}
		fmt.Fprintf(w, "%s,%s,%s,%d,%s\n", date, nameParts[0], nameParts[1], count, value)
	}
	return nil
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
	ApiErrorNonPublic = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-403",
			ResultMsg:  "비공개 데이터입니다.",
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
	ApiErrorDqmNonPublic = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-650",
			ResultMsg:  "비공개 데이터입니다.",
		},
	}
	ApiErrorOrgnNonRetrive = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-660",
			ResultMsg:  "비공개 데이터입니다.",
		},
	}
	ApiErrorInvalidValue = ApiResult{
		ResultStats: ApiResultStats{
			ResultCode: "ERROR-670",
			ResultMsg:  "측정값 이상",
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
