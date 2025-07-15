package siotsvr

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func (s *HttpServer) writeRecptnPacketData(c *gin.Context) {
	var data RecptnPacketData
	if c.Request.Method == http.MethodGet {
		if err := c.ShouldBindQuery(&data); err != nil {
			s.log.Println("Error binding query parameters:", err)
			c.String(http.StatusBadRequest, "Invalid query parameters")
			return
		}
	} else {
		if err := c.ShouldBindJSON(&data); err != nil {
			s.log.Println("Error binding JSON body:", err)
			c.String(http.StatusBadRequest, "Invalid JSON body")
			return
		}
	}

	// Validate the data
	if err := data.Validate(); err != nil {
		s.log.Println("Validation error:", err)
		c.String(http.StatusBadRequest, fmt.Sprintf("Validation failed: %s", err.Error()))
		return
	}

	conn, err := s.openConn(c)
	if err != nil {
		c.String(http.StatusInternalServerError, "Database connection error")
		return
	}
	defer conn.Close()

	sqlText := `INSERT INTO TB_RECPTN_PACKET_DATA(
                    PACKET_SEQ,
                    TRNSMIT_SERVER_NO,
                    DATA_NO,
                    PK_SEQ,
                    AREA_CODE,
                    MODL_SERIAL,
                    PACKET,
                    PACKET_STTUS_CODE,
                    RECPTN_RESULT_CODE,
                    RECPTN_RESULT_MSSAGE,
                    PARS_SE_CODE,
                    REGIST_DE,
                    REGIST_TIME,
                    REGIST_DT
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	result := conn.Exec(c, sqlText, data.PacketSeq, data.TransmitServerNo, data.DataNo,
		data.PkSeq, data.AreaCode, data.ModlSerial, data.Packet,
		data.PacketSttusCode, data.RecptnResultCode, data.RecptnResultMssage,
		data.ParsSeCode, data.RegistDe, data.RegistTime, data.RegistDt)
	if result.Err() != nil {
		c.String(http.StatusInternalServerError, "Failed to write RecptnPacketData, "+result.Err().Error())
		return
	}
	c.String(http.StatusOK, "RecptnPacketData written successfully!")
}

// ----------------------------------------------------------------------------------------------------
// NAME                                                        NULL?    TYPE                LENGTH
// ----------------------------------------------------------------------------------------------------
// PACKET_SEQ                                                           long                20
// TRNSMIT_SERVER_NO                                                    integer             11
// DATA_NO                                                              integer             11
// PK_SEQ                                                               long                20
// MODL_SERIAL                                                          varchar             20
// PACKET                                                               varchar             1000
// PACKET_STTUS_CODE                                                    varchar             4
// RECPTN_RESULT_CODE                                                   varchar             10
// RECPTN_RESULT_MSSAGE                                                 varchar             200
// PARS_SE_CODE                                                         varchar             4
// PARS_DT                                                              datetime            31
// REGIST_DE                                                            varchar             8
// REGIST_TIME                                                          varchar             4
// REGIST_DT                                                            datetime            31
// AREA_CODE                                                            varchar             10

// Validation Rules Implemented:
//
// Numeric Field Validation:
// - PACKET_SEQ: Must be greater than 0 (positive integer)
// - DATA_NO: Must be non-negative (≥ 0)
// - PK_SEQ: Must be non-negative when provided
// - TRNSMIT_SERVER_NO: Must be non-negative (≥ 0)
//
// String Length Validation:
// - AREA_CODE: Maximum 10 characters
// - MODL_SERIAL: Maximum 20 characters
// - PACKET: Maximum 1000 characters
// - RECPTN_RESULT_MSSAGE: Maximum 500 characters
// - PACKET_STTUS_CODE: Maximum 4 characters
// - PARS_SE_CODE: Maximum 4 characters
// - RECPTN_RESULT_CODE: Maximum 10 characters
//
// Date/Time Format Validation:
//   - REGIST_DE: Must follow YYYYMMDD format (8 characters)
//   - REGIST_TIME: Must follow HHMM format (4 characters)
//   - REGIST_DT: Must follow YYYY-MM-DD HH:MM:SS format (19 characters)
//     If REGIST_DT is not provided, it can be empty
//     and server will set it to current time
type RecptnPacketData struct {
	PacketSeq          int64  `form:"PACKET_SEQ" json:"PACKET_SEQ"`
	TransmitServerNo   int    `form:"TRNSMIT_SERVER_NO" json:"TRNSMIT_SERVER_NO"`
	DataNo             int    `form:"DATA_NO" json:"DATA_NO"`
	PkSeq              int64  `form:"PK_SEQ" json:"PK_SEQ"`
	AreaCode           string `form:"AREA_CODE" json:"AREA_CODE"`
	ModlSerial         string `form:"MODL_SERIAL" json:"MODL_SERIAL"`
	Packet             string `form:"PACKET" json:"PACKET"`
	PacketSttusCode    string `form:"PACKET_STTUS_CODE" json:"PACKET_STTUS_CODE"`
	RecptnResultCode   string `form:"RECPTN_RESULT_CODE" json:"RECPTN_RESULT_CODE"`
	RecptnResultMssage string `form:"RECPTN_RESULT_MSSAGE" json:"RECPTN_RESULT_MSSAGE"`
	ParsSeCode         string `form:"PARS_SE_CODE" json:"PARS_SE_CODE"`
	RegistDe           string `form:"REGIST_DE" json:"REGIST_DE"`
	RegistTime         string `form:"REGIST_TIME" json:"REGIST_TIME"`
	RegistDt           string `form:"REGIST_DT" json:"REGIST_DT"`
}

func (data *RecptnPacketData) Validate() error {
	// Validate PACKET_SEQ - should be positive
	if data.PacketSeq <= 0 {
		return fmt.Errorf("invalid PACKET_SEQ: %d, must be greater than 0", data.PacketSeq)
	}

	// Validate TransmitServerNo - should not be empty
	if data.TransmitServerNo < 0 {
		return fmt.Errorf("invalid TRNSMIT_SERVER_NO: %d, must be non-negative", data.TransmitServerNo)
	}

	// Validate DataNo - should be non-negative
	if data.DataNo < 0 {
		return fmt.Errorf("invalid DATA_NO: %d, must be non-negative", data.DataNo)
	}

	// Validate PkSeq if provided - should be positive when set
	if data.PkSeq < 0 {
		return fmt.Errorf("invalid PK_SEQ: %d, must be non-negative", data.PkSeq)
	}

	// Validate AreaCode length if provided
	if data.AreaCode != "" && len(data.AreaCode) > 10 {
		return fmt.Errorf("AREA_CODE too long: %d characters, maximum 10 allowed", len(data.AreaCode))
	}

	// Validate ModlSerial length if provided
	if data.ModlSerial != "" && len(data.ModlSerial) > 20 {
		return fmt.Errorf("MODL_SERIAL too long: %d characters, maximum 20 allowed", len(data.ModlSerial))
	}

	// Validate Packet data length if provided
	if data.Packet != "" && len(data.Packet) > 1000 {
		return fmt.Errorf("PACKET too long: %d characters, maximum 1000 allowed", len(data.Packet))
	}

	// Validate PacketSttusCode - should be valid status code if provided
	if data.PacketSttusCode != "" && len(data.PacketSttusCode) > 4 {
		return fmt.Errorf("PACKET_STTUS_CODE too long: %d characters, maximum 4 allowed", len(data.PacketSttusCode))
	}

	// Validate RecptnResultCode - should be valid result code if provided
	if data.RecptnResultCode != "" && len(data.RecptnResultCode) > 10 {
		return fmt.Errorf("RECPTN_RESULT_CODE too long: %d characters, maximum 10 allowed", len(data.RecptnResultCode))
	}

	// Validate RecptnResultMssage length if provided
	if data.RecptnResultMssage != "" && len(data.RecptnResultMssage) > 500 {
		return fmt.Errorf("RECPTN_RESULT_MSSAGE too long: %d characters, maximum 500 allowed", len(data.RecptnResultMssage))
	}

	// Validate ParsSeCode - should be valid parse code if provided
	if data.ParsSeCode != "" && len(data.ParsSeCode) > 4 {
		return fmt.Errorf("PARS_SE_CODE too long: %d characters, maximum 4 allowed", len(data.ParsSeCode))
	}

	// Validate RegistDe format if provided (YYYYMMDD)
	if data.RegistDe != "" {
		if len(data.RegistDe) != 8 {
			return fmt.Errorf("invalid REGIST_DE format: %s, expected YYYYMMDD", data.RegistDe)
		}
	}

	// Validate RegistTime format if provided (HHMM)
	if data.RegistTime != "" {
		if len(data.RegistTime) != 4 {
			return fmt.Errorf("invalid REGIST_TIME format: %s, expected HH:MM:SS", data.RegistTime)
		}
	}

	// Validate RegistDt format if provided (YYYY-MM-DD HH:MM:SS)
	if data.RegistDt != "" {
		if len(data.RegistDt) != 19 {
			return fmt.Errorf("invalid REGIST_DT format: %s, expected YYYY-MM-DD HH:MM:SS", data.RegistDt)
		}
		// Basic format check
		if data.RegistDt[4] != '-' || data.RegistDt[7] != '-' || data.RegistDt[10] != ' ' ||
			data.RegistDt[13] != ':' || data.RegistDt[16] != ':' {
			return fmt.Errorf("invalid REGIST_DT format: %s, expected YYYY-MM-DD HH:MM:SS", data.RegistDt)
		}
	} else {
		data.RegistDt = time.Now().In(time.Local).Format("2006-01-02 15:04:05")
	}

	return nil
}
