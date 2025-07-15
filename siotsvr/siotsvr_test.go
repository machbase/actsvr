package siotsvr

import (
	"actsvr/util"
	"context"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/machbase/neo-server/v8/api"
	"github.com/machbase/neo-server/v8/api/testsuite"
)

// Create a mock HttpServer
var httpTestServer *HttpServer
var dbTestServer *testsuite.Server

func TestMain(m *testing.M) {
	dbTestServer = testsuite.NewServer("./test_data/tmp/db")
	dbTestServer.StartServer(m)
	time.Sleep(3 * time.Second)
	createTables()

	// Set Gin to test mode
	gin.SetMode(gin.TestMode)

	httpTestServer = &HttpServer{
		log:    util.NewLog(util.LogConfig{}),
		dbHost: "127.0.0.1",
		dbPort: dbTestServer.MachPort(),
		dbUser: "sys",
		dbPass: "manager",
	}

	// Run the tests
	code := m.Run()

	dbTestServer.StopServer(m)
	os.Exit(code)
}

func createTables() {
	ctx := context.Background()
	db := dbTestServer.DatabaseSVR()
	conn, err := db.Connect(ctx, api.WithTrustUser("sys"))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	result := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS TB_RECPTN_PACKET_DATA (
			PACKET_SEQ             long,
			TRNSMIT_SERVER_NO      int,
			DATA_NO                int,
			PK_SEQ                 long,
			MODL_SERIAL            varchar(20),
			PACKET                 varchar(1000),
			PACKET_STTUS_CODE      varchar(4),
			RECPTN_RESULT_CODE     varchar(10),
			RECPTN_RESULT_MSSAGE   varchar(200),
			PARS_SE_CODE           varchar(4),
			PARS_DT                datetime,
			REGIST_DE              varchar(8),
			REGIST_TIME            varchar(4),
			REGIST_DT              datetime,
			AREA_CODE              varchar(10)
		)`)
	if err := result.Err(); err != nil {
		panic(err)
	}
}
