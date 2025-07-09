package loader

import (
	"context"
	"os"
	"strconv"
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
		CREATE TAG TABLE IF NOT EXISTS TAG (
			NAME VARCHAR(200) PRIMARY KEY,
			TIME DATETIME BASETIME,
			VALUE DOUBLE,
			SVAL VARCHAR(200),
			IVAL INT64,
			DVAL DOUBLE
		)`)
	if err := result.Err(); err != nil {
		panic(err)
	}
}

func TestLoader(t *testing.T) {
	t.Run("import_data", func(t *testing.T) {
		timeformat := "2006-01-02 15:04:05"
		tz := "Asia/Seoul"
		os.Args = []string{
			"loader",
			"-db-host", "127.0.0.1",
			"-db-port", strconv.Itoa(testServer.MachPort()),
			"-db-user", "sys",
			"-db-pass", "manager",
			"-db-table", "tag",
			"-skip-header",
			"-timeformat", timeformat,
			"-tz", tz,
			"-log-filename", "./test_data/tmp/loader.log",
			"./test_data/sample.csv",
		}

		Main()

		ctx := context.Background()
		db := testServer.DatabaseCLI()
		conn, err := db.Connect(ctx, api.WithTrustUser("sys"))
		if err != nil {
			t.Fatalf("Failed to connect to database: %v", err)
		}
		defer conn.Close()
		rows, err := conn.Query(ctx, "SELECT * FROM tag")
		if err != nil {
			t.Fatalf("Failed to query data: %v", err)
		}
		defer rows.Close()

		location, err := time.LoadLocation(tz)
		if err != nil {
			t.Fatalf("Failed to load time zone %s: %v", tz, err)
		}
		for rows.Next() {
			var name, sval string
			var ts time.Time
			var value, dval float64
			var ival int64
			if err := rows.Scan(&name, &ts, &value, &sval, &ival, &dval); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			t.Logf("Row: NAME=%s, TIME=%s, VALUE=%f, SVAL=%s, IVAL=%d, DVAL=%f",
				name, ts.In(location).Format(timeformat), value, sval, ival, dval)
		}

		conn.Exec(ctx, "DROP TABLE tag")
	})
}
