package rest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type Config struct {
	Host string
	Port int
}

type Client struct {
	Conf Config
	ht   http.Client
}

func NewClient(cfg Config) (*Client, error) {
	return &Client{
		Conf: cfg,
		ht: http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: true,
			},
			Timeout: 30 * time.Second, // 30 seconds
		},
	}, nil
}

func (c *Client) makeUrl(r any) string {
	baseAddr := "http://" + c.Conf.Host + ":" + fmt.Sprint(c.Conf.Port)
	switch v := r.(type) {
	case string:
		return url.QueryEscape(v)
	case *QueryRequest:
		ret := baseAddr + "/db/query?q=" + url.QueryEscape(v.Q)
		if v.Format != "" {
			ret += "&format=" + url.QueryEscape(v.Format)
		}
		if v.Transpose {
			ret += "&transpose=true"
		} else if v.RowsFlatten {
			ret += "&rowsFlatten=true"
		}
		return ret
	default:
		return ""
	}
}

func (c *Client) Do(r *QueryRequest) *QueryResponse {
	rsp, err := c.ht.Get(c.makeUrl(r))
	if err != nil {
		return &QueryResponse{Err: err}
	}

	ret := &QueryResponse{}
	ret.StatusCode = rsp.StatusCode
	ret.Status = rsp.Status
	ret.ContentType = rsp.Header.Get("Content-Type")
	ret.ContentEncoding = rsp.Header.Get("Content-Encoding")
	ret.Body = rsp.Body
	return ret
}

type QueryRequest struct {
	Q           string `json:"q"`
	Format      string `json:"format,omitempty"`      // e.g., "json", "csv"
	Transpose   bool   `json:"transpose,omitempty"`   // whether to transpose the result
	RowsFlatten bool   `json:"rowsFlatten,omitempty"` // whether to flatten rows
}

type QueryResponse struct {
	Err             error
	StatusCode      int    // e.g., 200, 404, 500
	Status          string // e.g., "200 OK", "404 Not Found"
	ContentType     string
	ContentEncoding string
	Body            io.ReadCloser
}

func (r *QueryResponse) Close() (err error) {
	if r.Body != nil {
		err = r.Body.Close()
		r.Body = nil
	}
	return
}

func (r *QueryResponse) JSON() (*JsonData, error) {
	if r.Err != nil {
		return nil, r.Err
	}

	if r.ContentType != "application/json" {
		return nil, fmt.Errorf("unexpected content type: %s", r.ContentType)
	}

	var data = &JsonData{}
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(data)
	return data, err
}

type JsonData struct {
	Success bool          `json:"success"`
	Reason  string        `json:"reason,omitempty"`
	Elapse  time.Duration `json:"elapsed,omitempty"`
	Data    struct {
		Types   []string `json:"types,omitempty"`
		Columns []string `json:"columns,omitempty"`
		Rows    any      `json:"rows,omitempty"`
		Cols    any      `json:"cols,omitempty"`
	} `json:"data"`
}
