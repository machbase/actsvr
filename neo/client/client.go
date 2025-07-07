package client

import (
	"github.com/machbase/neo-server/v8/api/machcli"
)

type Config struct {
	Host string
	Port int
}

type Client struct {
	Conf Config
	DB   *machcli.Database
}

func NewClient(cfg Config) (*Client, error) {
	conf := &machcli.Config{
		Host: cfg.Host,
		Port: cfg.Port,
	}

	db, err := machcli.NewDatabase(conf)
	if err != nil {
		return nil, err
	}
	return &Client{
		Conf: cfg,
		DB:   db,
	}, nil
}

func (c *Client) Close() (err error) {
	if c.DB != nil {
		err = c.DB.Close()
		c.DB = nil
	}
	return
}
