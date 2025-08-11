package siotsvr

import "database/sql"

type AreaCode struct {
	AreaSeq    int            `sql:"area_seq"`
	AreaCode   sql.NullString `sql:"area_code"`
	AreaNm     sql.NullString `sql:"area_nm"`
	La         float64        `sql:"la"`
	Lo         float64        `sql:"lo"`
	RegisterNo sql.NullString `sql:"register_no"`
	RegistDt   sql.NullTime   `sql:"regist_dt"`
	UpdUsrNo   sql.NullString `sql:"upd_usr_no"`
	UpdtDt     sql.NullTime   `sql:"updt_dt"`
}

func selectAreaCode(db *sql.DB, callback func(*AreaCode) bool) error {
	rows, err := db.Query(`SELECT
		AREA_SEQ,
		AREA_CODE,
		AREA_NM,
		LA,
		LO,
		REGISTER_NO,
		REGIST_DT,
		UPDUSR_NO,
		UPDT_DT
	FROM TB_AREA_CODE`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var ac AreaCode
		err := rows.Scan(
			&ac.AreaSeq,
			&ac.AreaCode,
			&ac.AreaNm,
			&ac.La,
			&ac.Lo,
			&ac.RegisterNo,
			&ac.RegistDt,
			&ac.UpdUsrNo,
			&ac.UpdtDt)
		if err != nil {
			return err
		}
		if !callback(&ac) {
			break
		}
	}
	return nil
}
