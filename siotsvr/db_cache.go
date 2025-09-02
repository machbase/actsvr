package siotsvr

import (
	"fmt"
	"sync"
)

var cacheCertKeys map[string]*Certkey // key: crtfc_key
var cacheCertKeysMutex sync.RWMutex
var cachePacketDefinition map[string]*PacketDefinition // key: trnsmit_server_no
var cachePacketDefinitionMutex sync.RWMutex
var cacheModelAreaCode map[string]string // key: {{model_serial}}_{{tsn}}_{{data_no}}, value: area_code
var cacheModelAreaCodeMutex sync.RWMutex

func reloadModelAreaCode() error {
	rdb, err := rdbConfig.Connect()
	if err != nil {
		return fmt.Errorf("failed to open RDB connection: %w", err)
	}
	defer rdb.Close()
	if err := rdb.Ping(); err != nil {
		return fmt.Errorf("failed to ping RDB: %w", err)
	}
	areaCodes := map[int64]string{} // key: AREA_SEQ value: AREA_CODE
	err = SelectAreaCode(rdb, func(ac *AreaCode) bool {
		if ac.AreaCode.Valid {
			areaCodes[ac.AreaSeq] = ac.AreaCode.String
		}
		return true // Continue processing other records
	})
	if err != nil {
		return fmt.Errorf("failed to initialize ModelAreaCode: %w", err)
	}

	modelAreaCodes := map[string]string{} // key: {{model_serial}}_{{tsn}}_{{data_no}}, value: area_code
	SelectModelInstallInfo(rdb, func(info *ModelInstallInfo, merr error) bool {
		if merr != nil {
			defaultLog.Warnf("failed to load ModelInstallInfo: %v", merr)
			return true // Continue processing other records
		}
		if info.AreaSeq.Valid {
			if code, exists := areaCodes[info.AreaSeq.Int64]; exists {
				key := fmt.Sprintf("%s_%d_%d", info.ModlSerial, info.TrnsmitServerNo, info.DataNo)
				modelAreaCodes[key] = code
			}
		}
		return true // Continue processing other records
	})
	cacheModelAreaCodeMutex.Lock()
	cacheModelAreaCode = modelAreaCodes
	defaultLog.Infof("Loaded %d model area codes", len(modelAreaCodes))
	cacheModelAreaCodeMutex.Unlock()
	return nil
}

func getModelAreaCode(modlSerial string, tsn int64, dataNo int) (string, error) {
	cacheModelAreaCodeMutex.RLock()
	defer cacheModelAreaCodeMutex.RUnlock()
	if cacheModelAreaCode == nil {
		return "", fmt.Errorf("model area codes not loaded")
	}
	key := fmt.Sprintf("%s_%d_%d", modlSerial, tsn, dataNo)
	code, exists := cacheModelAreaCode[key]
	if !exists {
		return "", fmt.Errorf("unknown model serial: %q", key)
	}
	return code, nil
}

type PacketDefinition struct {
	PacketMasterSeq int64                   `json:"packetMasterSeq"`
	TrnsmitServerNo int64                   `json:"trnsmitServerNo"`
	DataNo          int64                   `json:"dataNo"`
	PacketSize      int                     `json:"packetSize"`
	HeaderSize      int                     `json:"headerSize"`
	DataSize        int                     `json:"dataSize"`
	Fields          []PacketFieldDefinition `json:"fields"`
	Public          bool                    `json:"public"`
	Masking         bool                    `json:"masking"`
}

type PacketFieldDefinition struct {
	PacketDetailSeq int64
	PacketSeCode    string
	PacketName      string
	DataCd          string
	PacketByte      int
	PacketUnit      string
	RuleType        string
	MaxValue        float64
	MinValue        float64
	ArrValue        string
	Category        string
	DC              string
	Public          bool
	Dqm             bool
}

func reloadCertkey() error {
	rdb, err := rdbConfig.Connect()
	if err != nil {
		return fmt.Errorf("failed to open RDB connection: %w", err)
	}
	defer rdb.Close()
	if err := rdb.Ping(); err != nil {
		return fmt.Errorf("failed to ping RDB: %w", err)
	}
	lst := map[string]*Certkey{}
	err = SelectCertkey(rdb, func(certkey *Certkey) bool {
		if certkey.CrtfcKey.Valid {
			lst[certkey.CrtfcKey.String] = certkey
		}
		return true // Continue processing other records
	})
	if err != nil {
		return fmt.Errorf("failed to initialize Certkey: %w", err)
	}
	cacheCertKeysMutex.Lock()
	cacheCertKeys = lst
	defaultLog.Infof("Loaded %d certificate keys", len(lst))
	cacheCertKeysMutex.Unlock()
	return nil
}

func getCertKey(certkey string) (*Certkey, error) {
	cacheCertKeysMutex.RLock()
	defer cacheCertKeysMutex.RUnlock()
	if cacheCertKeys == nil {
		return nil, fmt.Errorf("certificate keys not loaded")
	}
	key, exists := cacheCertKeys[certkey]
	if !exists {
		return nil, fmt.Errorf("unknown certificate key: %q", certkey)
	}
	return key, nil
}

func reloadPacketDefinition() error {
	rdb, err := rdbConfig.Connect()
	if err != nil {
		return fmt.Errorf("failed to open RDB connection: %w", err)
	}
	defer rdb.Close()
	if err := rdb.Ping(); err != nil {
		return fmt.Errorf("failed to ping RDB: %w", err)
	}
	lst := []*ModelPacketMaster{}
	err = SelectModelPacketMaster(rdb, func(master *ModelPacketMaster) bool {
		lst = append(lst, master)
		return true // Continue processing other records
	})
	if err != nil {
		return fmt.Errorf("failed to initialize ModelPacketMaster: %w", err)
	}
	packets := map[string]*PacketDefinition{}
	for _, master := range lst {
		dataNo := 1
		if master.DataNo.Valid {
			dataNo = int(master.DataNo.Int64)
		}
		tsn := master.TrnsmitServerNo.Int64
		fields := []PacketFieldDefinition{}
		// Load packet fields for each packet definition
		err := SelectModelPacketDetail(rdb, master.PacketMasterSeq, func(detail *ModelPacketDetail) bool {
			field := PacketFieldDefinition{
				PacketDetailSeq: detail.PacketDetailSeq,
			}
			if detail.PacketSeCode.Valid {
				field.PacketSeCode = detail.PacketSeCode.String
			}
			if detail.PacketNm.Valid {
				field.PacketName = detail.PacketNm.String
			}
			if detail.DataCd.Valid {
				field.DataCd = detail.DataCd.String
			}
			if detail.PacketByte.Valid {
				field.PacketByte = int(detail.PacketByte.Int64)
			}
			if detail.PacketUnit.Valid {
				field.PacketUnit = detail.PacketUnit.String
			}
			if detail.ValdRuleType.Valid {
				field.RuleType = detail.ValdRuleType.String
			}
			if detail.MaxValue.Valid {
				field.MaxValue = detail.MaxValue.Float64
			}
			if detail.MinValue.Valid {
				field.MinValue = detail.MinValue.Float64
			}
			if detail.ArrValue.Valid {
				field.ArrValue = detail.ArrValue.String
			}
			if detail.DC.Valid {
				field.DC = detail.DC.String
			}
			if detail.PublicYn.Valid {
				field.Public = detail.PublicYn.String == "Y"
			}
			if detail.DqmYn.Valid {
				field.Dqm = detail.DqmYn.String == "Y"
			}
			fields = append(fields, field)
			return true
		})
		if err != nil {
			return fmt.Errorf("failed to load fields for packet master %d: %w", master.PacketMasterSeq, err)
		}
		key := fmt.Sprintf("%d_%d", tsn, dataNo)
		packets[key] = &PacketDefinition{
			PacketMasterSeq: master.PacketMasterSeq,
			TrnsmitServerNo: tsn,
			DataNo:          master.DataNo.Int64,
			PacketSize:      int(master.PacketSize.Int64),
			HeaderSize:      int(master.HderSize.Int64),
			DataSize:        int(master.DataSize.Int64),
			Public:          master.Public.Valid && master.Public.String == "Y",
			Masking:         master.Masking.Valid && master.Masking.String == "Y",
			Fields:          fields,
		}
	}
	cachePacketDefinitionMutex.Lock()
	cachePacketDefinition = packets
	defaultLog.Infof("Loaded %d packet definitions", len(packets))
	cachePacketDefinitionMutex.Unlock()
	return nil
}

func getPacketDefinition(tsn int64, dataNo int) *PacketDefinition {
	cachePacketDefinitionMutex.RLock()
	defer cachePacketDefinitionMutex.RUnlock()
	if cachePacketDefinition == nil {
		return nil
	}
	key := fmt.Sprintf("%d_%d", tsn, dataNo)
	def, exists := cachePacketDefinition[key]
	if !exists {
		return nil
	}
	return def
}
