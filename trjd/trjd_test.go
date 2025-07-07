package trjd

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/machbase/neo-server/v8/api"
	"github.com/machbase/neo-server/v8/api/testsuite"
)

var testServer *testsuite.Server

func TestMain(m *testing.M) {
	testServer = testsuite.NewServer("./test_data/tmp/db")
	testServer.StartServer(m)
	time.Sleep(3 * time.Second)
	createTables()
	code := m.Run()
	testServer.StopServer(m)
	os.Exit(code)
}

func createTables() {
	ctx := context.Background()
	db := testServer.DatabaseSVR()
	conn, err := db.Connect(ctx, api.WithTrustUser("sys"))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	result := conn.Exec(ctx, `
		CREATE TAG TABLE IF NOT EXISTS TRIP (
			ID VARCHAR(200) PRIMARY KEY,
			TIME DATETIME BASETIME,
			VALUE DOUBLE,
			DATA JSON
		)`)
	if err := result.Err(); err != nil {
		panic(err)
	}
}
