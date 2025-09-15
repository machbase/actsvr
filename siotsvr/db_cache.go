package siotsvr

import (
	"fmt"
	"strings"
	"sync"
)

const KEY_STATUS_OK = "10"

var cacheCertKeys map[string]*CertKey // key: crtfc_key
var cacheCertKeysMutex sync.RWMutex
var cacheOrgKeys map[string]*OrgKey // key: crtfc_key
var cacheOrgKeysMutex sync.RWMutex
var cachePacketDefinition map[string]*PacketDefinition // key: trnsmit_server_no
var cachePacketDefinitionMutex sync.RWMutex
var cacheModelAreaCode map[string]string // key: {{model_serial}}_{{tsn}}_{{data_no}}, value: area_code
var cacheModelAreaCodeMutex sync.RWMutex
var cacheModelDqmInfo map[int64]*ModelDqmInfo // key: trnsmit_server_no
var cacheModelDqmInfoMutex sync.RWMutex

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
				key := fmt.Sprintf("%s_%d_%d", strings.ToUpper(info.ModlSerial), info.TrnsmitServerNo, info.DataNo)
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
	// Model Serial of TB_MODL_INSTL_INFO
	// should be compared in case-insentive ways
	modlSerial = strings.ToUpper(modlSerial) // use upper cases

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

func reloadCertKey() error {
	rdb, err := rdbConfig.Connect()
	if err != nil {
		return fmt.Errorf("failed to open RDB connection: %w", err)
	}
	defer rdb.Close()
	if err := rdb.Ping(); err != nil {
		return fmt.Errorf("failed to ping RDB: %w", err)
	}
	lst := map[string]*CertKey{}
	err = SelectCertKey(rdb, func(certkey *CertKey) bool {
		if !certkey.SttusCode.Valid || certkey.SttusCode.String != KEY_STATUS_OK {
			return true
		}
		if certkey.CrtfcKey.Valid {
			lst[certkey.CrtfcKey.String] = certkey
		}
		return true // Continue processing other records
	})
	if err != nil {
		return fmt.Errorf("failed to initialize CertKey: %w", err)
	}
	cacheCertKeysMutex.Lock()
	cacheCertKeys = lst
	defaultLog.Infof("Loaded %d certificate keys", len(lst))
	cacheCertKeysMutex.Unlock()
	return nil
}

func getCertKey(certkey string) (*CertKey, error) {
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

func reloadOrgKey() error {
	rdb, err := rdbConfig.Connect()
	if err != nil {
		return fmt.Errorf("failed to open RDB connection: %w", err)
	}
	defer rdb.Close()
	if err := rdb.Ping(); err != nil {
		return fmt.Errorf("failed to ping RDB: %w", err)
	}
	lst := map[string]*OrgKey{}
	err = SelectOrgKey(rdb, func(orgkey *OrgKey) bool {
		if !orgkey.SttusCode.Valid || orgkey.SttusCode.String != KEY_STATUS_OK {
			return true
		}
		if orgkey.CrtfcKey.Valid {
			lst[orgkey.CrtfcKey.String] = orgkey
		}
		return true // Continue processing other records
	})
	if err != nil {
		return fmt.Errorf("failed to initialize OrgKey: %w", err)
	}
	cacheOrgKeysMutex.Lock()
	cacheOrgKeys = lst
	defaultLog.Infof("Loaded %d org keys", len(lst))
	cacheOrgKeysMutex.Unlock()
	return nil
}

func getOrgKey(certkey string) (*OrgKey, error) {
	cacheOrgKeysMutex.RLock()
	defer cacheOrgKeysMutex.RUnlock()
	if cacheOrgKeys == nil {
		return nil, fmt.Errorf("org keys not loaded")
	}
	key, exists := cacheOrgKeys[certkey]
	if !exists {
		return nil, fmt.Errorf("unknown org key: %q", certkey)
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
				// Table "modl_packet_detail", PUBLIC_YN: E(공개), I(비공개)
				field.Public = detail.PublicYn.String == "E"
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

func reloadModelDqmInfo() error {
	rdb, err := rdbConfig.Connect()
	if err != nil {
		return fmt.Errorf("failed to open RDB connection: %w", err)
	}
	defer rdb.Close()
	if err := rdb.Ping(); err != nil {
		return fmt.Errorf("failed to ping RDB: %w", err)
	}
	dqms := map[int64]*ModelDqmInfo{}
	err = SelectModelDqmInfo(rdb, func(dqm *ModelDqmInfo) bool {
		dqms[dqm.TrnsmitServerNo] = dqm
		return true // Continue processing other records
	})
	if err != nil {
		return fmt.Errorf("failed to initialize ModelDqmInfo: %w", err)
	}
	cacheModelDqmInfoMutex.Lock()
	cacheModelDqmInfo = dqms
	defaultLog.Infof("Loaded %d model DQM info", len(dqms))
	cacheModelDqmInfoMutex.Unlock()
	return nil
}

func getModelDqmInfo(tsn int64) *ModelDqmInfo {
	cacheModelDqmInfoMutex.RLock()
	defer cacheModelDqmInfoMutex.RUnlock()
	if cacheModelDqmInfo == nil {
		return nil
	}
	ret, ok := cacheModelDqmInfo[tsn]
	if !ok {
		return nil
	}
	return ret
}
