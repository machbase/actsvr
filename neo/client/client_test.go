package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/machbase/neo-server/v8/api"
	"github.com/stretchr/testify/require"
)

func TestMachCli(t *testing.T) {
	cfg := Config{
		Host: "192.168.0.207",
		Port: 5656,
	}
	client, err := NewClient(cfg)
	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	ctx := context.Background()
	conn, err := client.DB.Connect(ctx, api.WithPassword("sys", "manager"))
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	rows, err := conn.Query(ctx, "SELECT * FROM demo LIMIT ?", 3)
	require.NoError(t, err)
	require.NotNil(t, rows)
	defer rows.Close()

	for rows.Next() {
		var rec Record

		err = rows.Scan(&rec.Name, &rec.Time, &rec.Value, &rec.Lat, &rec.Lon)
		require.NoError(t, err)
		t.Log("Row:", rec.String())
	}
}

type Record struct {
	Name  string    `json:"name"`
	Time  time.Time `json:"time"`
	Value float64   `json:"value"`
	Lat   float64   `json:"lat"`
	Lon   float64   `json:"lon"`
}

func (r *Record) String() string {
	return r.Name + " " + r.Time.Format(time.DateTime) + " " +
		"Value: " + fmt.Sprintf("%f", r.Value) + " " +
		"Lat: " + fmt.Sprintf("%f", r.Lat) + " " +
		"Lon: " + fmt.Sprintf("%f", r.Lon)
}
