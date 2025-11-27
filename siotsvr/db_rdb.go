package siotsvr

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type ModelDqmInfo struct {
	TrnsmitServerNo int64 `sql:"trnsmit_server_no"`
	Masking         bool  `sql:"maskng_yn"`
	Public          bool  `sql:"public_yn"`
}

func SelectModelDqmInfo(db *sql.DB, callback func(*ModelDqmInfo) bool) error {
	sqlText := strings.Join([]string{
		"SELECT",
		"TRNSMIT_SERVER_NO,",
		"MASKNG_YN,",
		"PUBLIC_YN",
		"FROM",
		tableName("TB_MODL_DQM_INFO"),
	}, " ")
	rows, err := db.Query(sqlText)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var md ModelDqmInfo
		var public sql.NullString
		var masking sql.NullString
		err := rows.Scan(
			&md.TrnsmitServerNo,
			&masking,
			&public)
		if err != nil {
			return err
		}
		md.Public = public.Valid && public.String == "Y"
		md.Masking = masking.Valid && masking.String == "Y"
		if !callback(&md) {
			break
		}
	}
	return nil
}

type ModelPacketMaster struct {
	PacketMasterSeq int64          `sql:"packet_master_seq"` // primary key
	TrnsmitServerNo sql.NullInt64  `sql:"trnsmit_server_no"`
	DataNo          sql.NullInt64  `sql:"data_no"`
	PacketSize      sql.NullInt64  `sql:"packet_size"`
	HderSize        sql.NullInt64  `sql:"hder_size"`
	DataSize        sql.NullInt64  `sql:"data_size"`
	RegisterNo      sql.NullString `sql:"register_no"`
	RegistDt        sql.NullTime   `sql:"regist_dt"`
	UpdusrNo        sql.NullString `sql:"updusr_no"`
	UpdtDt          sql.NullTime   `sql:"updt_dt"`
}

func SelectModelPacketMaster(db *sql.DB, callback func(*ModelPacketMaster) bool) error {
	sqlText := strings.Join([]string{
		"SELECT",
		"PACKET_MASTR_SEQ,",
		"TRNSMIT_SERVER_NO,",
		"DATA_NO,",
		"PACKET_SIZE,",
		"HDER_SIZE,",
		"DATA_SIZE,",
		"REGISTER_NO,",
		"REGIST_DT,",
		"UPDUSR_NO,",
		"UPDT_DT",
		"FROM",
		"TB_MODL_PACKET_MASTR",
	}, " ")
	rows, err := db.Query(sqlText)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var mp ModelPacketMaster
		err := rows.Scan(
			&mp.PacketMasterSeq,
			&mp.TrnsmitServerNo,
			&mp.DataNo,
			&mp.PacketSize,
			&mp.HderSize,
			&mp.DataSize,
			&mp.RegisterNo,
			&mp.RegistDt,
			&mp.UpdusrNo,
			&mp.UpdtDt)
		if err != nil {
			return err
		}
		if !callback(&mp) {
			break
		}
	}
	return nil
}

type ModelPacketDetail struct {
	PacketDetailSeq int64           `sql:"packet_detail_seq"` // primary key
	PacketMastrSeq  sql.NullInt64   `sql:"packet_master_seq"`
	PacketSeCode    sql.NullString  `sql:"packet_se_code"`
	PacketNm        sql.NullString  `sql:"packet_nm"`
	DataCd          sql.NullString  `sql:"data_cd"`
	PacketByte      sql.NullInt64   `sql:"packet_byte"`
	PacketUnit      sql.NullString  `sql:"packet_unit"`
	ValdRuleType    sql.NullString  `sql:"vald_rule_type"`
	MaxValue        sql.NullFloat64 `sql:"max_value"`
	MinValue        sql.NullFloat64 `sql:"min_value"`
	ArrValue        sql.NullString  `sql:"arr_value"`
	PacketCtgry     sql.NullString  `sql:"packet_ctgry"`
	DC              sql.NullString  `sql:"dc"`
	PublicYn        sql.NullString  `sql:"public_yn"`
	DqmYn           sql.NullString  `sql:"dqm_yn"`
	GraphYn         sql.NullString  `sql:"graph_yn"`
	LimitValue      sql.NullString  `sql:"limit_value"`
	CeckPttrn       sql.NullString  `sql:"ceck_pttrn"`
	SortOrdr        sql.NullInt64   `sql:"sort_ordr"`
	RegisterNo      sql.NullString  `sql:"register_no"`
	RegistDt        sql.NullTime    `sql:"regist_dt"`
	UpdusrNo        sql.NullString  `sql:"updusr_no"`
	UpdtDt          sql.NullTime    `sql:"updt_dt"`
}

func SelectModelPacketDetail(db *sql.DB, masterSeq int64, callback func(*ModelPacketDetail) bool) error {
	sqlText := strings.Join([]string{
		"SELECT",
		"PACKET_DETAIL_SEQ,",
		"PACKET_MASTR_SEQ,",
		"PACKET_SE_CODE,",
		"PACKET_NM,",
		"DATA_CD,",
		"PACKET_BYTE,",
		"PACKET_UNIT,",
		"VALD_RULE_TYPE,",
		"MAX_VALUE,",
		"MIN_VALUE,",
		"ARR_VALUE,",
		"PACKET_CTGRY,",
		"DC,",
		"PUBLIC_YN,",
		"DQM_YN,",
		"GRAPH_YN,",
		"LIMIT_VALUE,",
		"CECK_PTTRN,",
		"SORT_ORDR,",
		"REGISTER_NO,",
		"REGIST_DT,",
		"UPDUSR_NO,",
		"UPDT_DT",
		"FROM",
		tableName("TB_MODL_PACKET_DETAIL"),
		"WHERE",
		"PACKET_MASTR_SEQ = ?",
		"ORDER BY",
		"PACKET_DETAIL_SEQ",
	}, " ")
	rows, err := db.Query(sqlText, masterSeq)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var mpd ModelPacketDetail
		err := rows.Scan(
			&mpd.PacketDetailSeq,
			&mpd.PacketMastrSeq,
			&mpd.PacketSeCode,
			&mpd.PacketNm,
			&mpd.DataCd,
			&mpd.PacketByte,
			&mpd.PacketUnit,
			&mpd.ValdRuleType,
			&mpd.MaxValue,
			&mpd.MinValue,
			&mpd.ArrValue,
			&mpd.PacketCtgry,
			&mpd.DC,
			&mpd.PublicYn,
			&mpd.DqmYn,
			&mpd.GraphYn,
			&mpd.LimitValue,
			&mpd.CeckPttrn,
			&mpd.SortOrdr,
			&mpd.RegisterNo,
			&mpd.RegistDt,
			&mpd.UpdusrNo,
			&mpd.UpdtDt)

		if err != nil {
			return err
		}

		if !callback(&mpd) {
			break
		}
	}
	return nil
}

type CertKey struct {
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

func SelectCertKey(db *sql.DB, callback func(*CertKey) bool) error {
	sqlText := strings.Join([]string{
		"SELECT",
		"CERTKEY_SEQ,",
		"TRNSMIT_SERVER_NO,",
		"CRTFC_KEY,",
		"BEGIN_VALID_DE,",
		"END_VALID_DE,",
		"STTUS_CODE,",
		"REGISTER_NO,",
		"REGIST_DT,",
		"UPDUSR_NO,",
		"UPDT_DT",
		"FROM",
		"TB_CERTKEY",
	}, " ")
	rows, err := db.Query(sqlText)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var ck CertKey
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
			defaultLog.Warnf("CERTKEY invalid date range for CertkeySeq %d: BeginValidDe %v, EndValidDe %v", ck.CertkeySeq, BeginValidDe, EndValidDe)
			continue
		}
		ck.BeginValidDe, err = stringToDate(BeginValidDe.String)
		if err != nil {
			defaultLog.Warnf("CERTKEY invalid BeginValidDe: %q, for CertkeySeq %d", BeginValidDe.String, ck.CertkeySeq)
			continue
		}
		ck.EndValidDe, err = stringToDate(EndValidDe.String)
		if err != nil {
			defaultLog.Warnf("CERTKEY invalid EndValidDe: %q, for CertkeySeq %d", EndValidDe.String, ck.CertkeySeq)
			continue
		}
		if !callback(&ck) {
			break
		}
	}
	return nil
}

type OrgKey struct {
	CertkeySeq   int64          `sql:"certkey_seq"` // primary key
	CrtfcKey     sql.NullString `sql:"crtfc_key"`
	OrgName      string         `sql:"organ_nm"`
	OrgCName     string         `sql:"organ_cn"`
	BeginValidDe time.Time      `sql:"begin_valid_de"`
	EndValidDe   time.Time      `sql:"end_valid_de"`
	SttusCode    sql.NullString `sql:"sttus_code"`
	RegisterNo   sql.NullString `sql:"register_no"`
	RegistDt     sql.NullTime   `sql:"regist_dt"`
	UpdusrNo     sql.NullString `sql:"updusr_no"`
	UpdtDt       sql.NullTime   `sql:"updt_dt"`
}

func SelectOrgKey(db *sql.DB, callback func(*OrgKey) bool) error {
	sqlText := strings.Join([]string{
		"SELECT",
		"CERTKEY_SEQ,",
		"CRTFC_KEY,",
		"ORGAN_NM,",
		"ORGAN_CN,",
		"BEGIN_VALID_DE,",
		"END_VALID_DE,",
		"STTUS_CODE,",
		"REGISTER_NO,",
		"REGIST_DT,",
		"UPDUSR_NO,",
		"UPDT_DT",
		"FROM",
		"TB_CERTKEY_INTEGRATION",
	}, " ")
	rows, err := db.Query(sqlText)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var ck OrgKey
		var BeginValidDe sql.NullString
		var EndValidDe sql.NullString
		var OrgNm sql.NullString
		var OrgCn sql.NullString
		err := rows.Scan(
			&ck.CertkeySeq,
			&ck.CrtfcKey,
			&OrgNm,
			&OrgCn,
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
			defaultLog.Warnf("CERTKEY invalid date range for CertkeySeq %d: BeginValidDe %v, EndValidDe %v", ck.CertkeySeq, BeginValidDe, EndValidDe)
			continue
		}
		ck.BeginValidDe, err = stringToDate(BeginValidDe.String)
		if err != nil {
			defaultLog.Warnf("CERTKEY invalid BeginValidDe: %q, for CertkeySeq %d", BeginValidDe.String, ck.CertkeySeq)
			continue
		}
		ck.EndValidDe, err = stringToDate(EndValidDe.String)
		if err != nil {
			defaultLog.Warnf("CERTKEY invalid EndValidDe: %q, for CertkeySeq %d", EndValidDe.String, ck.CertkeySeq)
			continue
		}
		if OrgNm.Valid {
			ck.OrgName = strings.TrimSpace(OrgNm.String)
		}
		if OrgCn.Valid {
			ck.OrgCName = strings.TrimSpace(OrgCn.String)
		}
		if !callback(&ck) {
			break
		}
	}
	return nil
}

type AreaCode struct {
	AreaSeq    int64          `sql:"area_seq"`
	AreaCode   sql.NullString `sql:"area_code"`
	AreaNm     sql.NullString `sql:"area_nm"`
	La         float64        `sql:"la"`
	Lo         float64        `sql:"lo"`
	RegisterNo sql.NullString `sql:"register_no"`
	RegistDt   sql.NullTime   `sql:"regist_dt"`
	UpdusrNo   sql.NullString `sql:"updusr_no"`
	UpdtDt     sql.NullTime   `sql:"updt_dt"`
}

func SelectAreaCode(db *sql.DB, callback func(*AreaCode) bool) error {
	sqlText := strings.Join([]string{
		"SELECT",
		"AREA_SEQ,",
		"AREA_CODE,",
		"AREA_NM,",
		"LA,",
		"LO,",
		"REGISTER_NO,",
		"REGIST_DT,",
		"UPDUSR_NO,",
		"UPDT_DT",
		"FROM",
		"TB_AREA_CODE",
	}, " ")
	rows, err := db.Query(sqlText)
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
			&ac.UpdusrNo,
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

func SelectModelInstallInfo(db *sql.DB, callback func(*ModelInstallInfo, error) bool) error {
	sqlText := strings.Join([]string{
		"SELECT",
		"MODL_SERIAL,",
		"TRNSMIT_SERVER_NO,",
		"DATA_NO,",
		"AREA_SEQ,",
		"POST_NO,",
		"ADRES,",
		"ADRES_DETAIL,",
		"CRDNT_CODE,",
		"LA,",
		"LO,",
		"ANTCTY,",
		"BULD_NM,",
		"INSTL_FLOOR,",
		"INSTL_HO_NO,",
		"RM,",
		"USE_YN,",
		"SORT_ORDR,",
		"LAST_RECPTN_DT,",
		"REGISTER_NO,",
		"REGIST_DT,",
		"UPDUSR_NO,",
		"UPDT_DT",
		"FROM",
		"TB_MODL_INSTL_INFO",
	}, " ")
	rows, err := db.Query(sqlText)
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
		mi.ModlSerial = strings.ToUpper(mi.ModlSerial) // use upper cases
		key := fmt.Sprintf("%s_%d_%d", mi.ModlSerial, mi.TrnsmitServerNo, mi.DataNo)
		if la.Valid {
			mi.La, err = strconv.ParseFloat(strings.TrimSpace(la.String), 64)
			if err != nil {
				if !callback(&mi, fmt.Errorf("invalid latitude: %q, of modl_serial:%s", la.String, mi.ModlSerial)) {
					break
				}
				continue
			}
		} else {
			if !callback(&mi, fmt.Errorf("latitude is null, modl_serial:%s", key)) {
				break
			}
			continue
		}
		if lo.Valid {
			mi.Lo, err = strconv.ParseFloat(strings.TrimSpace(lo.String), 64)
			if err != nil {
				if !callback(&mi, fmt.Errorf("invalid longitude: %q, of modl_serial:%s", lo.String, mi.ModlSerial)) {
					break
				}
				continue
			}
		} else {
			if !callback(&mi, fmt.Errorf("longitude is null, modl_serial:%s", mi.ModlSerial)) {
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

func stringToDate(dateStr string) (time.Time, error) {
	layout := "20060102"
	return time.Parse(layout, dateStr)
}

func SelectMaxPacketSeq(db *sql.DB) (int64, error) {
	row := db.QueryRow(fmt.Sprintf(`SELECT MAX(PACKET_SEQ) FROM %s`, tableName("TB_RECPTN_PACKET_DATA")))
	if err := row.Err(); err != nil {
		return 0, err
	}
	var maxPacketSeq int64
	if err := row.Scan(&maxPacketSeq); err != nil {
		crow := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s`, tableName("TB_RECPTN_PACKET_DATA")))
		if cerr := crow.Err(); cerr != nil {
			return 0, cerr
		}
		var cnt int64
		if cerr := crow.Scan(&cnt); cerr != nil {
			return 0, cerr
		}
		if cnt == 0 {
			// no records
			return 1000, nil
		}
		return 0, err
	}
	return maxPacketSeq, nil
}

func SelectMaxPacketParsSeq(db *sql.DB) (int64, error) {
	row := db.QueryRow(fmt.Sprintf(`SELECT MAX(PACKET_PARS_SEQ) FROM %s`, tableName("TB_PACKET_PARS_DATA")))
	if err := row.Err(); err != nil {
		return 0, err
	}
	var maxPacketParsSeq int64
	if err := row.Scan(&maxPacketParsSeq); err != nil {
		crow := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s`, tableName("TB_PACKET_PARS_DATA")))
		if cerr := crow.Err(); cerr != nil {
			return 0, cerr
		}
		var cnt int64
		if cerr := crow.Scan(&cnt); cerr != nil {
			return 0, cerr
		}
		if cnt == 0 {
			// no records
			return 1000, nil
		}
		return 0, err
	}
	return maxPacketParsSeq, nil
}

type ModlOrgnPublic struct {
	CertKeySeq      int64 `sql:"certkey_seq"` // primary key
	TrnsmitServerNo int64 `sql:"trnsmit_server_no"`
	Retrive         bool  `sql:"retriv_yn"`
	Masking         bool  `sql:"maskng_yn"`
}

func SelectModelOrganizationPublic(db *sql.DB, callback func(*ModlOrgnPublic) bool) error {
	sqlText := strings.Join([]string{
		"SELECT",
		"CERTKEY_SEQ,",
		"TRNSMIT_SERVER_NO,",
		"RETRIV_YN,",
		"MASKNG_YN",
		"FROM",
		tableName("TB_MODL_ORGN_PUBLIC"),
	}, " ")
	rows, err := db.Query(sqlText)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var mo ModlOrgnPublic
		var retrive sql.NullString
		var masking sql.NullString
		err := rows.Scan(
			&mo.CertKeySeq,
			&mo.TrnsmitServerNo,
			&retrive,
			&masking)
		if err != nil {
			return err
		}
		mo.Masking = masking.Valid && masking.String == "Y"
		mo.Retrive = retrive.Valid && retrive.String == "Y"
		if !callback(&mo) {
			break
		}
	}
	return nil
}

type ModlPacketDqm struct {
	TableName       string
	TrnsmitServerNo int64
	DataNo          int
	LastArrivalTime time.Time
}

func SelectModlPacketDqm(db *sql.DB, table string, tsn int64, dataNo int) (ModlPacketDqm, error) {
	var ret = ModlPacketDqm{
		TableName:       strings.ToUpper(table),
		TrnsmitServerNo: tsn,
		DataNo:          dataNo,
		LastArrivalTime: time.Time{},
	}
	sqlText := strings.Join([]string{
		"SELECT",
		"LAST_TIME",
		"FROM",
		"TB_MODL_PACKET_DQM",
		"WHERE",
		"TRNSMIT_SERVER_NO = ?",
		"AND TABLE_NM = ?",
		"AND DATA_NO = ?",
	}, " ")
	row := db.QueryRow(sqlText, tsn, ret.TableName, dataNo)
	if err := row.Err(); err != nil {
		if err == sql.ErrNoRows || err.Error() == "sql: no rows in result set" {
			return ret, nil
		}
		return ret, err
	}
	var lastTime sql.NullString
	if err := row.Scan(&lastTime); err != nil {
		if err == sql.ErrNoRows || err.Error() == "sql: no rows in result set" {
			return ret, nil
		}
		return ret, err
	}
	if lastTime.Valid {
		tm, err := time.ParseInLocation("2006-01-02 15:04:05.000000000", lastTime.String, DefaultTZ)
		if err != nil {
			return ret, err
		}
		ret.LastArrivalTime = tm
	}
	return ret, nil
}

func UpsertModlPacketDqm(db *sql.DB, data ModlPacketDqm) error {
	sqlText := strings.Join([]string{
		"INSERT INTO TB_MODL_PACKET_DQM",
		"(TABLE_NM, TRNSMIT_SERVER_NO, DATA_NO, LAST_TIME)",
		"VALUES (?, ?, ?, ?)",
		"ON DUPLICATE KEY UPDATE",
		"LAST_TIME = ?",
	}, " ")
	lastTime := data.LastArrivalTime.In(DefaultTZ).Format("2006-01-02 15:04:05.000000000")
	_, err := db.Exec(sqlText, data.TableName, data.TrnsmitServerNo, data.DataNo, lastTime, lastTime)
	return err
}
