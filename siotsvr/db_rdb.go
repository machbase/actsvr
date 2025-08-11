package siotsvr

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

type AreaCode struct {
	AreaSeq    int64          `sql:"area_seq"`
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

func selectModlInstlInfo(db *sql.DB, callback func(*ModelInstallInfo, error) bool) error {
	rows, err := db.Query(`SELECT
		MODL_SERIAL,
		TRNSMIT_SERVER_NO,
		DATA_NO,
		AREA_SEQ,
		POST_NO,
		ADRES,
		ADRES_DETAIL,
		CRDNT_CODE,
		LA,
		LO,
		ANTCTY,
		BULD_NM,
		INSTL_FLOOR,
		INSTL_HO_NO,
		RM,
		USE_YN,
		SORT_ORDR,
		LAST_RECPTN_DT,
		REGISTER_NO,
		REGIST_DT,
		UPDUSR_NO,
		UPDT_DT
	FROM TB_MODL_INSTL_INFO`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var mi ModelInstallInfo
		var la, lo sql.NullString
		err := rows.Scan(
			&mi.ModlSerial,
			&mi.TrnsmitServerNo,
			&mi.DataNo,
			&mi.AreaSeq,
			&mi.PostNo,
			&mi.Adres,
			&mi.AdresDetail,
			&mi.CrdntCode,
			&la,
			&lo,
			&mi.AntCty,
			&mi.BuldNm,
			&mi.InstlFloor,
			&mi.InstlHoNo,
			&mi.Rm,
			&mi.UseYn,
			&mi.SortOrdr,
			&mi.LastRecptnDt,
			&mi.RegisterNo,
			&mi.RegistDt,
			&mi.UpdusrNo,
			&mi.UpdtDt)

		if err != nil {
			return err
		}

		key := fmt.Sprintf("%s_%d_%d", mi.ModlSerial, mi.TrnsmitServerNo, mi.DataNo)
		if la.Valid {
			mi.La, err = strconv.ParseFloat(strings.TrimSpace(la.String), 64)
			if err != nil {
				if !callback(&mi, fmt.Errorf("invalid latitude: %q, of %s", la.String, key)) {
					break
				}
				continue
			}
		} else {
			if !callback(&mi, fmt.Errorf("latitude is null for %s", key)) {
				break
			}
			continue
		}
		if lo.Valid {
			mi.Lo, err = strconv.ParseFloat(strings.TrimSpace(lo.String), 64)
			if err != nil {
				if !callback(&mi, fmt.Errorf("invalid longitude: %q, of %s", lo.String, mi.ModlSerial)) {
					break
				}
				continue
			}
		} else {
			if !callback(&mi, fmt.Errorf("longitude is null for %s", mi.ModlSerial)) {
				break
			}
			continue
		}
		if !callback(&mi, nil) {
			break
		}
	}
	return nil
}

type ModelInstallInfo struct {
	ModlSerial      string         `json:"modl_serial"`       // not null
	TrnsmitServerNo int64          `json:"trnsmit_server_no"` // not null
	DataNo          int64          `json:"data_no"`           // not null
	AreaSeq         sql.NullInt64  `json:"area_seq"`
	PostNo          sql.NullString `json:"post_no"`
	Adres           sql.NullString `json:"adres"`
	AdresDetail     sql.NullString `json:"adres_detail"`
	CrdntCode       sql.NullString `json:"crdnt_code"`
	La              float64        `json:"la"`
	Lo              float64        `json:"lo"`
	AntCty          sql.NullInt64  `json:"antcty"`
	BuldNm          sql.NullString `json:"buld_nm"`
	InstlFloor      sql.NullInt64  `json:"instl_floor"`
	InstlHoNo       sql.NullInt64  `json:"instl_ho_no"`
	Rm              sql.NullString `json:"rm"`
	UseYn           sql.NullString `json:"use_yn"`
	SortOrdr        sql.NullInt64  `json:"sort_ordr"`
	LastRecptnDt    sql.NullTime   `json:"last_recptn_dt"`
	RegisterNo      sql.NullString `json:"register_no"`
	RegistDt        sql.NullTime   `json:"regist_dt"`
	UpdusrNo        sql.NullString `json:"updusr_no"`
	UpdtDt          sql.NullTime   `json:"updt_dt"`
}

func nullString(v sql.NullString) any {
	if v.Valid {
		return v.String
	}
	return nil
}

func nullInt64(v sql.NullInt64) any {
	if v.Valid {
		return v.Int64
	}
	return nil
}
