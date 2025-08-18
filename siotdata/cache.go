package siotdata

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func SelectCertkey(db *sql.DB, callback func(*Certkey) bool) error {
	rows, err := db.Query(`SELECT
		CERTKEY_SEQ,
		TRNSMIT_SERVER_NO,
		CRTFC_KEY,
		BEGIN_VALID_DE,
		END_VALID_DE,
		STTUS_CODE,
		REGISTER_NO,
		REGIST_DT,
		UPDUSR_NO,
		UPDT_DT
	FROM TB_CERTKEY`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var ck Certkey
		var BeginValidDe sql.NullString
		var EndValidDe sql.NullString
		err := rows.Scan(
			&ck.CertkeySeq,
			&ck.TrnsmitServerNo,
			&ck.CrtfcKey,
			&BeginValidDe,
			&EndValidDe,
			&ck.SttusCode,
			&ck.RegisterNo,
			&ck.RegistDt,
			&ck.UpdusrNo,
			&ck.UpdtDt)
		if err != nil {
			return err
		}
		if !BeginValidDe.Valid || !EndValidDe.Valid {
			fmt.Printf("CERTKEY invalid date range for CertkeySeq %d: BeginValidDe %v, EndValidDe %v", ck.CertkeySeq, BeginValidDe, EndValidDe)
			continue
		}
		ck.BeginValidDe, err = stringToDate(BeginValidDe.String)
		if err != nil {
			fmt.Printf("CERTKEY invalid BeginValidDe: %q, for CertkeySeq %d", BeginValidDe.String, ck.CertkeySeq)
			continue
		}
		ck.EndValidDe, err = stringToDate(EndValidDe.String)
		if err != nil {
			fmt.Printf("CERTKEY invalid EndValidDe: %q, for CertkeySeq %d", EndValidDe.String, ck.CertkeySeq)
			continue
		}
		if !callback(&ck) {
			break
		}
	}
	return nil
}

type Certkey struct {
	CertkeySeq      int64          `sql:"certkey_seq"` // primary key
	TrnsmitServerNo sql.NullInt64  `sql:"trnsmit_server_no"`
	CrtfcKey        sql.NullString `sql:"crtfc_key"`
	BeginValidDe    time.Time      `sql:"begin_valid_de"`
	EndValidDe      time.Time      `sql:"end_valid_de"`
	SttusCode       sql.NullString `sql:"sttus_code"`
	RegisterNo      sql.NullString `sql:"register_no"`
	RegistDt        sql.NullTime   `sql:"regist_dt"`
	UpdusrNo        sql.NullString `sql:"updusr_no"`
	UpdtDt          sql.NullTime   `sql:"updt_dt"`
}

func stringToDate(dateStr string) (time.Time, error) {
	layout := "20060102"
	return time.Parse(layout, dateStr)
}

var certKeys map[string]*Certkey

var rdbConfig = RDBConfig{
	host: "siot.redirectme.net",
	port: 3306,
	user: "iotdb",
	pass: "iotmanager",
	db:   "iotdb",
}

type RDBConfig struct {
	host string
	port int
	user string
	pass string
	db   string
}

func (c RDBConfig) Connect() (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		c.user, c.pass, c.host, c.port, c.db)
	rdb, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return rdb, nil
}
