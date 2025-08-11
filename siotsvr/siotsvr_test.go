package siotsvr

import (
	"actsvr/util"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/machbase/neo-server/v8/api"
	"github.com/machbase/neo-server/v8/api/testsuite"
)

func performRequest(r http.Handler, method, path string, body interface{}) *httptest.ResponseRecorder {
	_ = body // Ignore body for this test
	req, _ := http.NewRequest(method, path, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// Create a mock HttpServer
var httpTestServer *HttpServer
var dbTestServer *testsuite.Server

func XXTestMain_disable(m *testing.M) {
	dbTestServer = testsuite.NewServer("./test_data/tmp/db")
	dbTestServer.StartServer(m)
	time.Sleep(3 * time.Second)
	createTables()

	machConfig = MachConfig{
		dbHost: "127.0.0.1",
		dbPort: dbTestServer.MachPort(),
		dbUser: "sys",
		dbPass: "manager",
	}

	// Set Gin to test mode
	gin.SetMode(gin.TestMode)

	httpTestServer = &HttpServer{
		log: util.NewLog(util.LogConfig{}),
	}

	// Run the tests
	code := m.Run()

	dbTestServer.StopServer(m)
	os.Exit(code)
}

func createTables() {
	createRecptnPacketDataTable()
	createPacketParsDataTable()
}

func createRecptnPacketDataTable() {
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

func createPacketParsDataTable() {
	ctx := context.Background()
	db := dbTestServer.DatabaseSVR()
	conn, err := db.Connect(ctx, api.WithTrustUser("sys"))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	result := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS TB_PACKET_PARS_DATA (
			PACKET_PARS_SEQ   long,
			PACKET_SEQ        long,
			TRNSMIT_SERVER_NO int,
			DATA_NO           int,
			REGIST_DT         datetime,
			REGIST_DE         varchar(8),
			SERVICE_SEQ       int,
			AREA_CODE         varchar(10),
			MODL_SERIAL       varchar(20),
			COLUMN0           varchar(50),
			COLUMN1           varchar(50),
			COLUMN2           varchar(50),
			COLUMN3           varchar(50),
			COLUMN4           varchar(50),
			COLUMN5           varchar(50),
			COLUMN6           varchar(50),
			COLUMN7           varchar(50),
			COLUMN8           varchar(50),
			COLUMN9           varchar(50),
			COLUMN10          varchar(50),
			COLUMN11          varchar(50),
			COLUMN12          varchar(50),
			COLUMN13          varchar(50),
			COLUMN14          varchar(50),
			COLUMN15          varchar(50),
			COLUMN16          varchar(50),
			COLUMN17          varchar(50),
			COLUMN18          varchar(50),
			COLUMN19          varchar(50),
			COLUMN20          varchar(50),
			COLUMN21          varchar(50),
			COLUMN22          varchar(50),
			COLUMN23          varchar(50),
			COLUMN24          varchar(50),
			COLUMN25          varchar(50),
			COLUMN26          varchar(50),
			COLUMN27          varchar(50),
			COLUMN28          varchar(50),
			COLUMN29          varchar(50),
			COLUMN30          varchar(50),
			COLUMN31          varchar(50),
			COLUMN32          varchar(50),
			COLUMN33          varchar(50),
			COLUMN34          varchar(50),
			COLUMN35          varchar(50),
			COLUMN36          varchar(50),
			COLUMN37          varchar(50),
			COLUMN38          varchar(50),
			COLUMN39          varchar(50),
			COLUMN40          varchar(50),
			COLUMN41          varchar(50),
			COLUMN42          varchar(50),
			COLUMN43          varchar(50),
			COLUMN44          varchar(50),
			COLUMN45          varchar(50),
			COLUMN46          varchar(50),
			COLUMN47          varchar(50),
			COLUMN48          varchar(50),
			COLUMN49          varchar(50),
			COLUMN50          varchar(50),
			COLUMN51          varchar(50),
			COLUMN52          varchar(50),
			COLUMN53          varchar(50),
			COLUMN54          varchar(50),
			COLUMN55          varchar(50),
			COLUMN56          varchar(50),
			COLUMN57          varchar(50),
			COLUMN58          varchar(50),
			COLUMN59          varchar(50),
			COLUMN60          varchar(50),
			COLUMN61          varchar(10),
			COLUMN62          varchar(10),
			COLUMN63          varchar(10)
		)`)
	if err := result.Err(); err != nil {
		panic(err)
	}
}
