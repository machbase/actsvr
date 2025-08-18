package siotsvr

import (
	"context"
	"database/sql"
	"time"
)

var replicaRowsPerRun = 100

func (s *HttpServer) loopReplicaRawPacket() {
	s.replicaWg.Add(1)
	defer s.replicaWg.Done()

	rdb, err := rdbConfig.Connect()
	if err != nil {
		panic(err)
	}
	defer rdb.Close()
	lastSeq, err := SelectMaxPacketSeq(rdb)
	if err != nil {
		if err != sql.ErrNoRows {
			panic(err)
		}
		lastSeq = 0
	}

	var ctx context.Context = context.Background()
	conn, err := s.openConn(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	for s.replicaAlive {
		rows, err := conn.Query(ctx, `SELECT
			PACKET_SEQ,
			TRNSMIT_SERVER_NO,
			DATA_NO,
			PK_SEQ,
			AREA_CODE,
			MODL_SERIAL,
			DQMCRR_OP,
			PACKET,
			PACKET_STTUS_CODE,
			RECPTN_RESULT_CODE,
			RECPTN_RESULT_MSSAGE,
			PARS_SE_CODE,
			REGIST_DE,
			REGIST_TIME,
			REGIST_DT
		FROM TB_RECPTN_PACKET_DATA WHERE PACKET_SEQ > ? limit ?`, lastSeq, replicaRowsPerRun)
		if err != nil {
			panic(err)
		}
		cnt := 0
		for rows.Next() {
			cnt++
			var data ReplicaRawPacketData
			err := rows.Scan(&data.PacketSeq, &data.TrnsmitServerNo, &data.DataNo,
				&data.PkSeq, &data.AreaCode, &data.ModlSerial, &data.DqmCrrOp,
				&data.Packet, &data.PacketSttusCode,
				&data.RecptnResultCode, &data.RecptnResultMssage,
				&data.ParsSeCode, &data.RegistDe, &data.RegistTime, &data.RegistDt)
			if err != nil {
				defaultLog.Errorf("Failed to scan row: %v", err)
				continue
			}
			lastSeq = data.PacketSeq // Assuming first column is PACKET_SEQ

			_, err = rdb.ExecContext(ctx, `INSERT INTO TB_RECPTN_PACKET_DATA(
				PACKET_SE, TRNSMIT_SERVER_NO, DATA_NO,
				PK_SEQ, MODL_SERIAL, PACKET,
				PACKET_STTUS_CODE, RECPTN_RESULT_CODE, RECPTN_RESULT_MSSAGE,
				PARS_SE_CODE, PARS_DT, REGIST_DE,
				REGIST_TIME, REGIST_DT
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			`, data.PacketSeq, data.TrnsmitServerNo, data.DataNo,
				data.PkSeq, data.ModlSerial, data.Packet,
				data.PacketSttusCode, data.RecptnResultCode, data.RecptnResultMssage,
				data.ParsSeCode, nowFunc(), data.RegistDe,
				data.RegistTime, data.RegistDt)
			if err != nil {
				defaultLog.Errorf("Failed to insert row: %v", err)
				continue
			}
		}
		rows.Close()

		if cnt == 0 {
			if err := rdb.Ping(); err != nil {
				defaultLog.Errorf("Failed to ping Redis: %v", err)
			}
			time.Sleep(3 * time.Second)
		}
	}
}

func (s *HttpServer) loopReplicaParsPacket() {
	s.replicaWg.Add(1)
	defer s.replicaWg.Done()

	rdb, err := rdbConfig.Connect()
	if err != nil {
		panic(err)
	}
	defer rdb.Close()
	lastParsSeq, err := SelectMaxPacketParsSeq(rdb)
	if err != nil {
		if err != sql.ErrNoRows {
			panic(err)
		}
		lastParsSeq = 0
	}

	var ctx context.Context = context.Background()
	conn, err := s.openConn(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	for s.replicaAlive {
		rows, err := conn.Query(ctx, `SELECT
			PACKET_PARS_SEQ,
			PACKET_SEQ,
			TRNSMIT_SERVER_NO,
			DATA_NO,
			REGIST_DT,
			REGIST_DE,
			SERVICE_SEQ,
			AREA_CODE,
			MODL_SERIAL,
			DQMCRR_OP,
			COLUMN0, COLUMN1, COLUMN2, COLUMN3, COLUMN4,
			COLUMN5, COLUMN6, COLUMN7, COLUMN8, COLUMN9,
			COLUMN10, COLUMN11, COLUMN12, COLUMN13, COLUMN14,
			COLUMN15, COLUMN16, COLUMN17, COLUMN18, COLUMN19,
			COLUMN20, COLUMN21, COLUMN22, COLUMN23, COLUMN24,
			COLUMN25, COLUMN26, COLUMN27, COLUMN28, COLUMN29,
			COLUMN30, COLUMN31, COLUMN32, COLUMN33, COLUMN34,
			COLUMN35, COLUMN36, COLUMN37, COLUMN38, COLUMN39,
			COLUMN40, COLUMN41, COLUMN42, COLUMN43, COLUMN44,
			COLUMN45, COLUMN46, COLUMN47, COLUMN48, COLUMN49,
			COLUMN50, COLUMN51, COLUMN52, COLUMN53, COLUMN54,
			COLUMN55, COLUMN56, COLUMN57, COLUMN58, COLUMN59,
			COLUMN60, COLUMN61, COLUMN62, COLUMN63
		FROM TB_PACKET_PARS_DATA WHERE PACKET_PARS_SEQ > ? limit ?`, lastParsSeq, replicaRowsPerRun)
		if err != nil {
			panic(err)
		}
		snull := func(s string) any {
			if s == "" {
				return nil
			} else {
				return s
			}
		}
		cnt := 0
		for rows.Next() {
			cnt++
			var data ReplicaParsPacketData
			err := rows.Scan(&data.PacketParsSeq, &data.PacketSeq,
				&data.TrnsmitServerNo, &data.DataNo,
				&data.RegistDt, &data.RegistDe,
				// &data.ServiceSeq, &data.AreaCode, &data.ModlSerial, &data.DqmCrrOp,
				&data.Column0, &data.Column1, &data.Column2, &data.Column3, &data.Column4,
				&data.Column5, &data.Column6, &data.Column7, &data.Column8, &data.Column9,
				&data.Column10, &data.Column11, &data.Column12, &data.Column13, &data.Column14,
				&data.Column15, &data.Column16, &data.Column17, &data.Column18, &data.Column19,
				&data.Column20, &data.Column21, &data.Column22, &data.Column23, &data.Column24,
				&data.Column25, &data.Column26, &data.Column27, &data.Column28, &data.Column29,
				&data.Column30, &data.Column31, &data.Column32, &data.Column33, &data.Column34,
				&data.Column35, &data.Column36, &data.Column37, &data.Column38, &data.Column39,
				&data.Column40, &data.Column41, &data.Column42, &data.Column43, &data.Column44,
				&data.Column45, &data.Column46, &data.Column47, &data.Column48, &data.Column49,
				&data.Column50, &data.Column51, &data.Column52, &data.Column53, &data.Column54,
				&data.Column55, &data.Column56, &data.Column57, &data.Column58, &data.Column59,
				&data.Column60, &data.Column61, &data.Column62, &data.Column63,
			)
			if err != nil {
				defaultLog.Errorf("Failed to scan row: %v", err)
				continue
			}
			lastParsSeq = data.PacketParsSeq

			_, err = rdb.ExecContext(ctx, `INSERT INTO TB_PACKET_PARS_DATA (
				PACKET_PARS_SEQ, PACKET_SEQ, TRNSMIT_SERVER_NO, DATA_NO,`+
				//	SERVICE_SEQ, AREA_CODE, MODL_SERIAL, DQMCRR_OP,
				`REGIST_DT, REGIST_DE, 
				COLUMN0, COLUMN1, COLUMN2, COLUMN3, COLUMN4,
				COLUMN5, COLUMN6, COLUMN7, COLUMN8, COLUMN9, COLUMN10,
				COLUMN11, COLUMN12, COLUMN13, COLUMN14, COLUMN15, COLUMN16,
				COLUMN17, COLUMN18, COLUMN19, COLUMN20, COLUMN21, COLUMN22,
				COLUMN23, COLUMN24, COLUMN25, COLUMN26, COLUMN27, COLUMN28,
				COLUMN29, COLUMN30, COLUMN31, COLUMN32, COLUMN33, COLUMN34,
				COLUMN35, COLUMN36, COLUMN37, COLUMN38, COLUMN39, COLUMN40,
				COLUMN41, COLUMN42, COLUMN43, COLUMN44, COLUMN45, COLUMN46,
				COLUMN47, COLUMN48, COLUMN49, COLUMN50, COLUMN51, COLUMN52,
				COLUMN53, COLUMN54, COLUMN55, COLUMN56, COLUMN57, COLUMN58,
				COLUMN59, COLUMN60, COLUMN61, COLUMN62, COLUMN63
			) VALUES (
			 	?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
			 	?, ?, ?, ?, ?, ?, ?, ?,
				?, ?, ?, ?, ?, ?, ?, ?,
				?, ?, ?, ?, ?, ?, ?, ?,
				?, ?, ?, ?, ?, ?, ?, ?,
				?, ?, ?, ?, ?, ?, ?, ?,
				?, ?, ?, ?, ?, ?, ?, ?,
				?, ?, ?, ?, ?, ?, ?, ?,
				?, ?, ?, ?, ?, ?, ?, ?)
			`, data.PacketParsSeq, data.PacketSeq, data.TrnsmitServerNo, data.DataNo,
				// data.ServiceSeq, data.AreaCode, data.ModlSerial, data.DqmCrrOp,
				data.RegistDt, data.RegistDe,
				snull(data.Column0), snull(data.Column1), snull(data.Column2), snull(data.Column3), snull(data.Column4),
				snull(data.Column5), snull(data.Column6), snull(data.Column7), snull(data.Column8), snull(data.Column9),
				snull(data.Column10), snull(data.Column11), snull(data.Column12), snull(data.Column13), snull(data.Column14),
				snull(data.Column15), snull(data.Column16), snull(data.Column17), snull(data.Column18), snull(data.Column19),
				snull(data.Column20), snull(data.Column21), snull(data.Column22), snull(data.Column23), snull(data.Column24),
				snull(data.Column25), snull(data.Column26), snull(data.Column27), snull(data.Column28), snull(data.Column29),
				snull(data.Column30), snull(data.Column31), snull(data.Column32), snull(data.Column33), snull(data.Column34),
				snull(data.Column35), snull(data.Column36), snull(data.Column37), snull(data.Column38), snull(data.Column39),
				snull(data.Column40), snull(data.Column41), snull(data.Column42), snull(data.Column43), snull(data.Column44),
				snull(data.Column45), snull(data.Column46), snull(data.Column47), snull(data.Column48), snull(data.Column49),
				snull(data.Column50), snull(data.Column51), snull(data.Column52), snull(data.Column53), snull(data.Column54),
				snull(data.Column55), snull(data.Column56), snull(data.Column57), snull(data.Column58), snull(data.Column59),
				snull(data.Column60), snull(data.Column61), snull(data.Column62), snull(data.Column63),
			)
			if err != nil {
				defaultLog.Errorf("Failed to insert row: %v", err)
				continue
			}
		}
		rows.Close()

		if cnt == 0 {
			if err := rdb.Ping(); err != nil {
				defaultLog.Errorf("Failed to ping Redis: %v", err)
			}
			time.Sleep(3 * time.Second)
		}
	}
}

type ReplicaRawPacketData struct {
	PacketSeq          int64
	TrnsmitServerNo    int64
	DataNo             int
	PkSeq              int64
	AreaCode           string
	ModlSerial         string
	DqmCrrOp           string
	Packet             string
	PacketSttusCode    string
	RecptnResultCode   string
	RecptnResultMssage string
	ParsSeCode         string
	RegistDe           string
	RegistTime         string
	RegistDt           time.Time
}

type ReplicaParsPacketData struct {
	PacketParsSeq   int64
	PacketSeq       int64
	TrnsmitServerNo int64
	DataNo          int
	ServiceSeq      int64
	AreaCode        string
	ModlSerial      string
	DqmCrrOp        string
	RegistDt        time.Time
	RegistDe        string // 20060102
	Column0         string
	Column1         string
	Column2         string
	Column3         string
	Column4         string
	Column5         string
	Column6         string
	Column7         string
	Column8         string
	Column9         string
	Column10        string
	Column11        string
	Column12        string
	Column13        string
	Column14        string
	Column15        string
	Column16        string
	Column17        string
	Column18        string
	Column19        string
	Column20        string
	Column21        string
	Column22        string
	Column23        string
	Column24        string
	Column25        string
	Column26        string
	Column27        string
	Column28        string
	Column29        string
	Column30        string
	Column31        string
	Column32        string
	Column33        string
	Column34        string
	Column35        string
	Column36        string
	Column37        string
	Column38        string
	Column39        string
	Column40        string
	Column41        string
	Column42        string
	Column43        string
	Column44        string
	Column45        string
	Column46        string
	Column47        string
	Column48        string
	Column49        string
	Column50        string
	Column51        string
	Column52        string
	Column53        string
	Column54        string
	Column55        string
	Column56        string
	Column57        string
	Column58        string
	Column59        string
	Column60        string
	Column61        string
	Column62        string
	Column63        string
}
