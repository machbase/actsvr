package rest_test

import (
	"actsvr/neo/rest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	cfg := rest.Config{
		Host: "127.0.0.1",
		Port: 5654,
	}
	client, err := rest.NewClient(cfg)
	require.NoError(t, err)
	require.NotNil(t, client)

	req := &rest.QueryRequest{Q: "SELECT * FROM demo limit 3", Format: "json", Transpose: true}
	rsp := client.Do(req)
	require.NoError(t, rsp.Err)
	require.NotNil(t, rsp)

	jsonData, err := rsp.JSON()
	require.NoError(t, err)
	require.NotNil(t, jsonData)

	err = rsp.Close()
	require.NoError(t, err)

	t.Logf("%+v", jsonData)
}
