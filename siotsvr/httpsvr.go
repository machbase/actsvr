package siotsvr

import (
	"actsvr/util"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/machbase/neo-server/v8/api"
	"github.com/machbase/neo-server/v8/api/machcli"
)

type HttpServer struct {
	Host      string
	Port      int
	KeepAlive int // seconds
	TempDir   string

	machCli    *machcli.Database
	log        *util.Log
	httpServer *http.Server
	router     *gin.Engine

	rawPacketCh  chan *RawPacketData
	parsPacketCh chan *ParsedPacketData
}

func NewHttpServer() *HttpServer {
	return &HttpServer{
		Host:         "0.0.0.0",
		Port:         8888,
		TempDir:      "/tmp",
		rawPacketCh:  make(chan *RawPacketData),
		parsPacketCh: make(chan *ParsedPacketData),
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
	go s.loopParsPacket()

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

func (s *HttpServer) Stop(ctx context.Context) error {
	if s.httpServer != nil {
		s.httpServer.Shutdown(ctx)
	}
	s.closeDatabase()
	close(s.parsPacketCh)
	close(s.rawPacketCh)
	s.log.Infof("Stopping HTTP server on %s:%d", s.Host, s.Port)
	return nil
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

func (s *HttpServer) openDatabase() error {
	if s.machCli == nil {
		db, err := machcli.NewDatabase(&machcli.Config{
			Host:         machConfig.dbHost,
			Port:         machConfig.dbPort,
			TrustUsers:   map[string]string{machConfig.dbUser: machConfig.dbPass},
			MaxOpenConn:  -1,
			MaxOpenQuery: -1,
		})
		if err != nil {
			return err
		}
		s.machCli = db
	}
	return nil
}

func (s *HttpServer) closeDatabase() {
	if s.machCli != nil {
		s.machCli.Close()
		s.machCli = nil
	}
}

func (s *HttpServer) openConn(ctx context.Context) (api.Conn, error) {
	if err := s.openDatabase(); err != nil {
		return nil, err
	}
	conn, err := s.machCli.Connect(ctx, api.WithPassword(machConfig.dbUser, machConfig.dbPass))
	if err != nil {
		return nil, err
	}
	return conn, nil
}

var globalPacketSeq int64 = 0 // Global packet sequence number
func nextPacketSeq() int64 {
	return atomic.AddInt64(&globalPacketSeq, 1)
}

func (s *HttpServer) reloadPacketSeq() error {
	ctx := context.Background()
	conn, err := s.openConn(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	row := conn.QueryRow(ctx, "SELECT MAX(PACKET_SEQ) FROM TB_RECPTN_PACKET_DATA")
	if err := row.Err(); err != nil {
		return err
	}
	seq := int64(0)
	if err := row.Scan(&seq); err != nil {
		return fmt.Errorf("failed to scan packet sequence: %w", err)
	}
	atomic.StoreInt64(&globalPacketSeq, seq+1)
	return nil
}

var globalPacketParseSeq int64 = 0 // Global packet parse sequence number
func nextPacketParseSeq() int64 {
	return atomic.AddInt64(&globalPacketParseSeq, 1)
}

func (s *HttpServer) reloadPacketParseSeq() error {
	ctx := context.Background()
	conn, err := s.openConn(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	row := conn.QueryRow(ctx, "SELECT MAX(PACKET_PARS_SEQ) FROM TB_PACKET_PARS_DATA")
	if err := row.Err(); err != nil {
		return err
	}
	seq := int64(0)
	if err := row.Scan(&seq); err != nil {
		return fmt.Errorf("failed to scan packet parse sequence: %w", err)
	}
	atomic.StoreInt64(&globalPacketParseSeq, seq+1)
	return nil
}

func (s *HttpServer) loopRawPacket() {
	ctx := context.Background()
	conn, err := s.openConn(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	sqlText := strings.Join([]string{
		"INSERT INTO TB_RECPTN_PACKET_DATA(",
		"PACKET_SEQ,",
		"TRNSMIT_SERVER_NO,",
		"DATA_NO,",
		"PK_SEQ,",
		"AREA_CODE,",
		"MODL_SERIAL,",
		"DQMCRR_OP,",
		"PACKET,",
		"PACKET_STTUS_CODE,",
		"RECPTN_RESULT_CODE,",
		"RECPTN_RESULT_MSSAGE,",
		"PARS_SE_CODE,",
		"REGIST_DE,",
		"REGIST_TIME,",
		"REGIST_DT",
		") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
	}, "")
	for data := range s.rawPacketCh {
		if data == nil {
			break
		}
		result := conn.Exec(ctx, sqlText,
			data.PacketSeq, data.TrnsmitServerNo, data.DataNo,
			data.PkSeq, data.AreaCode, data.ModlSerial, data.DqmCrrOp, data.Packet,
			data.PacketSttusCode, data.RecptnResultCode, data.RecptnResultMssage,
			data.ParsSeCode, data.RegistDe, data.RegistTime, data.RegistDt)
		if err := result.Err(); err != nil {
			s.log.Errorf("Failed to insert RecptnPacketData: %v, data: %#v", err, data)
			continue
		}
		parsed := s.parseRawPacket(data)
		s.parsPacketCh <- parsed
	}
}

func (s *HttpServer) loopParsPacket() {
	ctx := context.Background()
	conn, err := s.openConn(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	for data := range s.parsPacketCh {
		if data == nil {
			break
		}
		sqlBuilder := strings.Builder{}
		sqlBuilder.WriteString(`INSERT INTO TB_PACKET_PARS_DATA(`)
		sqlBuilder.WriteString(`PACKET_PARS_SEQ,`)
		sqlBuilder.WriteString(`PACKET_SEQ,`)
		sqlBuilder.WriteString(`TRNSMIT_SERVER_NO,`)
		sqlBuilder.WriteString(`DATA_NO,`)
		sqlBuilder.WriteString(`REGIST_DT,`)
		sqlBuilder.WriteString(`REGIST_DE,`)
		sqlBuilder.WriteString(`SERVICE_SEQ,`)
		sqlBuilder.WriteString(`AREA_CODE,`)
		sqlBuilder.WriteString(`MODL_SERIAL,`)
		sqlBuilder.WriteString(`DQMCRR_OP`)
		for i := range data.Values {
			sqlBuilder.WriteString(fmt.Sprintf(",COLUMN%d", i))
		}
		sqlBuilder.WriteString(`) VALUES(?,?,?,?,?,?,?,?,?,?`)
		for range data.Values {
			sqlBuilder.WriteString(",?")
		}
		sqlBuilder.WriteString(`)`)

		values := []interface{}{
			data.PacketParsSeq,
			data.PacketSeq,
			data.TrnsmitServerNo,
			data.DataNo,
			data.RegistDt,
			data.RegistDe,
			data.ServiceSeq,
			data.AreaCode,
			data.ModlSerial,
			data.DqmCrrOp,
		}
		for _, val := range data.Values {
			values = append(values, val)
		}
		result := conn.Exec(ctx, sqlBuilder.String(), values...)
		if result.Err() != nil {
			s.log.Error("Failed to insert PacketParsData:", result.Err())
		}
	}
}
