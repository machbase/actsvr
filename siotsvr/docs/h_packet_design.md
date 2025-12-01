# h_packet.go API Design Documentation

## 1. Overview
패킷 수신 및 파싱 API의 핵심 구현을 담당하는 핸들러로, IoT 디바이스로부터 전송되는 Raw 패킷을 수신하고 검증 및 파싱 작업을 수행합니다.

## 2. Architecture Diagram

```mermaid
graph TB
    Client[IoT Device/Client]
    
    Client -->|POST /sendPacket/:certkey/:data_no/:pk_seq/:serial_num/:packet| handleSendPacket
    Client -->|GET /serverStat/:certkey| handleServerStat
    
    subgraph "handleSendPacket Handler"
        handleSendPacket[handleSendPacket]
        handleSendPacket --> ValidateParams[Validate Parameters]
        ValidateParams --> SanitizeInput[Sanitize Input<br/>trim spaces, remove special chars]
        SanitizeInput --> VerifyCert[Verify Certkey]
        VerifyCert --> GetDefinition[Get Packet Definition]
        GetDefinition --> GetAreaCode[Get Model Area Code]
        GetAreaCode --> BuildRawData[Build RawPacketData]
        BuildRawData --> ValidateLength[Validate Packet Length]
        ValidateLength --> EnqueueRaw[Enqueue to rawPacketCh]
        ValidateLength --> CheckError{Error?}
        CheckError -->|Yes| EnqueueErr[Enqueue to errPacketCh]
        CheckError -->|No| Success
        EnqueueErr --> Success[Return Response]
    end
    
    subgraph "parseRawPacket Function"
        parseRawPacket[parseRawPacket]
        parseRawPacket --> GetPacketDef[Get Packet Definition]
        GetPacketDef --> SplitPacket[Split Packet by Field Bytes]
        SplitPacket --> ProcessFields[Process Each Field]
        ProcessFields --> TrimSpaces[Trim Spaces]
        TrimSpaces --> RemoveZeros[Remove Leading Zeros]
        RemoveZeros --> ValidateField{Validate Field}
        
        ValidateField -->|VAL_ITV| CheckInterval[Check Numeric Interval]
        ValidateField -->|VAL_ARR| CheckArray[Check Array Values]
        ValidateField -->|No Rule| SkipValidation[Skip Validation]
        
        CheckInterval --> MarkInvalid{Valid?}
        CheckArray --> MarkInvalid
        MarkInvalid -->|No| AddMarker[Add InvalidValueMarker]
        MarkInvalid -->|Yes| KeepValue[Keep Value]
        SkipValidation --> KeepValue
        
        AddMarker --> CollectErrors[Collect Validation Errors]
        KeepValue --> BuildParsed[Build ParsedPacketData]
        CollectErrors --> BuildParsed
    end
    
    subgraph "handleServerStat Handler"
        handleServerStat[handleServerStat]
        handleServerStat --> CheckCertCache[Check Certkey in Cache]
        CheckCertCache --> CheckValidity{Valid Period?}
        CheckValidity -->|Yes| ReturnReady[Return READY-000]
        CheckValidity -->|No| ReturnError[Return ERROR-200]
    end
    
    subgraph "Background Processing"
        rawPacketCh[rawPacketCh<br/>Channel]
        errPacketCh[errPacketCh<br/>Channel]
        
        EnqueueRaw --> rawPacketCh
        EnqueueErr --> errPacketCh
        
        rawPacketCh --> AsyncParser[Async Parser Worker]
        AsyncParser --> parseRawPacket
        
        errPacketCh --> ErrorHandler[Error Handler Worker]
    end
    
    subgraph "Data Storage"
        TSDB[(Time Series DB<br/>TB_RECPTN_PACKET_DATA<br/>TB_PACKET_PARS_DATA)]
        
        BuildParsed --> TSDB
        BuildRawData --> TSDB
    end
```

## 3. Sequence Diagram

```mermaid
sequenceDiagram
    participant Device as IoT Device
    participant API as handleSendPacket
    participant Cache as Cache Layer
    participant RawCh as rawPacketCh
    participant ErrCh as errPacketCh
    participant Parser as parseRawPacket
    participant TSDB as Time Series DB
    
    Device->>API: POST /sendPacket/:certkey/:data_no/:pk_seq/:serial_num/:packet
    
    Note over API: Start timer for logging
    
    API->>API: Validate required parameters
    API->>API: Sanitize inputs<br/>(trim spaces, clean pk_seq)
    
    alt Invalid parameters
        API-->>Device: 200 OK + ERROR-100
        Note over API: Log error and metrics
    end
    
    API->>API: Parse pk_seq (remove special chars)
    API->>API: Truncate pk_seq to 18 chars
    
    API->>Cache: VerifyCertkey(certkey)
    Cache-->>API: TSN (Transmit Server Number)
    
    alt Invalid certkey
        API-->>Device: 200 OK + ERROR-200
    end
    
    API->>Cache: getPacketDefinition(tsn, 1)
    Cache-->>API: Packet definition
    
    alt No definition found
        API-->>Device: 200 OK + ERROR-900
    end
    
    API->>API: Generate packetSeq
    API->>Cache: getModelAreaCode(serial, tsn, 1)
    Cache-->>API: Area code
    
    API->>API: Build RawPacketData struct
    API->>API: Check packet length vs definition
    
    alt Packet length mismatch
        API->>API: Set PacketSttusCode = 'E'
        API->>API: Set RecptnResultCode = ERROR-300
    end
    
    API->>RawCh: Enqueue RawPacketData
    
    opt Packet has error
        API->>ErrCh: Enqueue RawPacketData
    end
    
    API-->>Device: 200 OK + ResultStats
    
    Note over API: Defer: Log request info & latency
    
    Note over RawCh,Parser: Async Processing
    
    RawCh->>Parser: Consume RawPacketData
    Parser->>Cache: getPacketDefinition(tsn, 1)
    Cache-->>Parser: Field definitions
    
    loop For each field
        Parser->>Parser: Extract field by byte size
        Parser->>Parser: Trim spaces
        Parser->>Parser: Remove leading zeros
        
        alt Has validation rule
            Parser->>Parser: Validate against rule
            alt Validation failed
                Parser->>Parser: Add InvalidValueMarker
                Parser->>Parser: Collect error
            end
        end
    end
    
    Parser->>Parser: Build ParsedPacketData
    
    opt Has validation errors
        Parser->>Parser: Return ValidateError
    end
    
    Parser->>TSDB: Insert ParsedPacketData
    API->>TSDB: Insert RawPacketData
```

## 4. Data Flow Diagram

```mermaid
flowchart TB
    subgraph "Input Layer"
        PathParams[Path Parameters<br/>certkey, data_no<br/>pk_seq, serial_num<br/>packet]
        QueryParams[Query Parameters<br/>DQMCRR_OP]
    end
    
    subgraph "Input Sanitization"
        S1[Trim modelSerial spaces]
        S2[Clean pk_seq<br/>Remove: - _ : space]
        S3[Truncate pk_seq to 18 chars]
        S4[Parse integers]
    end
    
    subgraph "Validation Layer"
        V1{Required params<br/>not empty?}
        V2{data_no valid<br/>integer?}
        V3{pk_seq valid<br/>18-digit integer?}
        V4{certkey exists<br/>in cache?}
        V5{Packet definition<br/>exists?}
    end
    
    subgraph "Data Enrichment"
        E1[Generate packetSeq]
        E2[Find TSN from certkey]
        E3[Get packet definition]
        E4[Get model area code]
        E5[Set current timestamp]
    end
    
    subgraph "Packet Validation"
        PV1{Packet length ==<br/>definition.PacketSize?}
        PV2[Set PacketSttusCode = S]
        PV3[Set PacketSttusCode = E]
        PV4[Set RecptnResultCode]
    end
    
    subgraph "Raw Data Structure"
        RD[RawPacketData<br/>- PacketSeq<br/>- TrnsmitServerNo<br/>- DataNo<br/>- PkSeq<br/>- ModlSerial<br/>- Packet<br/>- PacketSttusCode<br/>- AreaCode<br/>- DqmCrrOp<br/>- RegistDt]
    end
    
    subgraph "Channel Routing"
        CH1[rawPacketCh]
        CH2[errPacketCh]
    end
    
    subgraph "Response"
        R1[API Response<br/>200 OK + ResultStats]
    end
    
    PathParams --> S1
    QueryParams --> S1
    S1 --> S2
    S2 --> S3
    S3 --> S4
    
    S4 --> V1
    V1 -->|No| R1
    V1 -->|Yes| V2
    V2 -->|No| R1
    V2 -->|Yes| V3
    V3 -->|No| R1
    V3 -->|Yes| V4
    V4 -->|No| R1
    V4 -->|Yes| V5
    V5 -->|No| R1
    V5 -->|Yes| E1
    
    E1 --> E2
    E2 --> E3
    E3 --> E4
    E4 --> E5
    
    E5 --> PV1
    PV1 -->|Yes| PV2
    PV1 -->|No| PV3
    PV2 --> PV4
    PV3 --> PV4
    
    PV4 --> RD
    
    RD --> CH1
    RD --> CH2Condition{PacketSttusCode<br/>== 'E'?}
    CH2Condition -->|Yes| CH2
    CH2Condition -->|No| Skip[Skip error queue]
    
    CH1 --> R1
    CH2 --> R1
    Skip --> R1
```

## 5. Packet Parsing Flow

```mermaid
flowchart TB
    subgraph "Parse Input"
        Input[RawPacketData]
        Input --> GetDef[Get Packet Definition<br/>by TSN, dataNo=1]
    end
    
    subgraph "Field Processing Loop"
        GetDef --> InitLoop[Initialize values array]
        InitLoop --> NextField{More fields?}
        
        NextField -->|Yes| Extract[Extract field bytes<br/>from packet string]
        Extract --> Trim[Trim spaces]
        Trim --> CheckHeader{Field type<br/>== 'H'?}
        
        CheckHeader -->|Yes| KeepOriginal[Keep original value]
        CheckHeader -->|No| RemoveZeros[removeLeadingZeros]
        
        KeepOriginal --> CheckEmpty{Value empty?}
        RemoveZeros --> CheckEmpty
        
        CheckEmpty -->|Yes| SkipValidation[Skip validation]
        CheckEmpty -->|No| CheckRule{Has validation<br/>rule?}
        
        CheckRule -->|VAL_ITV| ValidateInterval[Validate numeric interval<br/>min <= value <= max]
        CheckRule -->|VAL_ARR| ValidateArray[Validate array values<br/>value in allowed list]
        CheckRule -->|None| SkipValidation
        
        ValidateInterval --> CheckResult{Valid?}
        ValidateArray --> CheckResult
        
        CheckResult -->|No| MarkInvalid[Prepend InvalidValueMarker<br/>Add to validErr.Fields]
        CheckResult -->|Yes| StoreValue[Store value]
        SkipValidation --> StoreValue
        MarkInvalid --> StoreValue
        
        StoreValue --> NextField
    end
    
    subgraph "Output Generation"
        NextField -->|No| BuildOutput[Build ParsedPacketData]
        BuildOutput --> CheckErrors{Has validation<br/>errors?}
        
        CheckErrors -->|Yes| ReturnError[Return ParsedPacketData<br/>+ ValidateError]
        CheckErrors -->|No| ReturnSuccess[Return ParsedPacketData<br/>+ nil]
    end
    
    subgraph "Debug Logging"
        StoreValue -.->|if debug enabled| LogField[Log field details]
    end
```

## 6. removeLeadingZeros Logic

```mermaid
flowchart TB
    Start[Input: string s]
    
    Start --> CheckEmpty{s empty?}
    CheckEmpty -->|Yes| ReturnEmpty[Return empty string]
    
    CheckEmpty -->|No| CheckX{First char<br/>'x' or 'X'?}
    CheckX -->|Yes| TrimX[Trim all leading x/X]
    TrimX --> CheckAfterX{Result empty?}
    CheckAfterX -->|Yes| ReturnEmpty
    CheckAfterX -->|No| ContinueX[Continue processing]
    
    CheckX -->|No| CheckNumeric{Is numeric<br/>pattern?}
    
    CheckNumeric -->|No| TrimZerosNonNum[Trim leading '0'<br/>for non-numeric]
    TrimZerosNonNum --> Return[Return result]
    
    CheckNumeric -->|Yes| ExtractSign[Extract sign<br/>+/- prefix]
    ExtractSign --> CheckDecimal{Has decimal<br/>point?}
    
    CheckDecimal -->|Yes| SplitDecimal[Split by '.']
    SplitDecimal --> TrimIntPart[Trim leading zeros<br/>from integer part]
    TrimIntPart --> CheckZeroInt{Integer part<br/>empty?}
    CheckZeroInt -->|Yes| SetZero[Set integer part = "0"]
    CheckZeroInt -->|No| KeepInt[Keep trimmed int]
    SetZero --> CombineDecimal[Combine: sign + int + "." + decimal]
    KeepInt --> CombineDecimal
    CombineDecimal --> Return
    
    CheckDecimal -->|No| TrimInteger[Trim leading zeros]
    TrimInteger --> CheckResult{Result empty?}
    CheckResult -->|Yes| HandleZero{Has sign<br/>+/- ?}
    HandleZero -->|Yes| ReturnSignZero[Return sign + "0"]
    HandleZero -->|No| ReturnZero[Return "0"]
    CheckResult -->|No| CombineSign[Combine: sign + trimmed]
    
    ReturnSignZero --> Return
    ReturnZero --> Return
    CombineSign --> Return
    ContinueX --> CheckNumeric
```

## 7. State Diagram

```mermaid
stateDiagram-v2
    [*] --> RequestReceived
    
    RequestReceived --> Sanitizing: Extract parameters
    
    Sanitizing --> Validating: Clean input data
    
    Validating --> BuildingData: All validations passed
    Validating --> ErrorResponse: Validation failed
    
    BuildingData --> GeneratingSeq: Create RawPacketData
    
    GeneratingSeq --> EnrichingData: Generate packetSeq
    
    EnrichingData --> CheckingPacket: Add TSN, AreaCode, Timestamp
    
    CheckingPacket --> MarkingOK: Length matches
    CheckingPacket --> MarkingError: Length mismatch
    
    MarkingOK --> Enqueueing: Set status 'S'
    MarkingError --> Enqueueing: Set status 'E'
    
    Enqueueing --> RawQueue: Send to rawPacketCh
    
    RawQueue --> ErrorQueue: If PacketSttusCode == 'E'
    RawQueue --> ResponseReady: Skip error queue
    ErrorQueue --> ResponseReady
    
    ResponseReady --> SendResponse: Build ResultStats
    
    SendResponse --> LoggingMetrics: Return 200 OK
    
    LoggingMetrics --> [*]: Defer cleanup
    
    ErrorResponse --> LoggingMetrics: Return error response
    
    note right of Enqueueing
        Async processing happens
        in background workers
        consuming from channels
    end note
    
    note right of CheckingPacket
        Packet length validation:
        len(packet) vs definition.PacketSize
    end note
```

## 8. Validation Error Structure

```mermaid
classDiagram
    class ValidateError {
        +int64 TransmitServerNo
        +[]ValidateErrorField Fields
        +String() string
        +Error() string
    }
    
    class ValidateErrorField {
        +string Field
        +string RuleType
        +string Rule
        +string FailedValue
    }
    
    class RawPacketData {
        +int64 PacketSeq
        +int64 TrnsmitServerNo
        +int DataNo
        +int64 PkSeq
        +string AreaCode
        +string ModlSerial
        +string DqmCrrOp
        +string Packet
        +string PacketSttusCode
        +string RecptnResultCode
        +string RecptnResultMssage
        +string ParsSeCode
        +string RegistDe
        +string RegistTime
        +int64 RegistDt
    }
    
    class ParsedPacketData {
        +int64 PacketParsSeq
        +int64 PacketSeq
        +int64 TrnsmitServerNo
        +int DataNo
        +int64 ServiceSeq
        +string AreaCode
        +string ModlSerial
        +string DqmCrrOp
        +int64 RegistDt
        +string RegistDe
        +[]string Values
    }
    
    ValidateError "1" --> "*" ValidateErrorField
    ParsedPacketData --> ValidateError : may return
    RawPacketData ..> ParsedPacketData : parsed into
```

## 9. Component Interaction

```mermaid
graph TB
    subgraph "HTTP Handlers"
        H1[handleSendPacket]
        H2[handleServerStat]
    end
    
    subgraph "Validation Functions"
        V1[Parameter validation]
        V2[VerifyCertkey]
        V3[Packet length check]
    end
    
    subgraph "Parsing Functions"
        P1[parseRawPacket]
        P2[removeLeadingZeros]
        P3[Field validation]
    end
    
    subgraph "Cache Access"
        C1[getPacketDefinition]
        C2[getModelAreaCode]
        C3[cacheCertKeys lookup]
    end
    
    subgraph "Data Generators"
        G1[nextPacketSeq]
        G2[nextPacketParseSeq]
        G3[nowFunc]
    end
    
    subgraph "Channels"
        CH1[rawPacketCh]
        CH2[errPacketCh]
    end
    
    subgraph "Background Workers"
        W1[Raw packet writer]
        W2[Parsed packet writer]
        W3[Error handler]
    end
    
    subgraph "Storage"
        S1[(TB_RECPTN_PACKET_DATA)]
        S2[(TB_PACKET_PARS_DATA)]
    end
    
    H1 --> V1
    H1 --> V2
    H1 --> V3
    H2 --> C3
    
    H1 --> C1
    H1 --> C2
    H1 --> G1
    H1 --> G3
    H1 --> CH1
    H1 --> CH2
    
    V2 --> C3
    
    CH1 --> W1
    CH1 --> W2
    CH2 --> W3
    
    W2 --> P1
    P1 --> C1
    P1 --> P2
    P1 --> P3
    P1 --> G2
    
    W1 --> S1
    W2 --> S2
```

## 10. Error Handling Matrix

```mermaid
flowchart TD
    Start[Request Start]
    
    Start --> E1{Empty<br/>parameters?}
    E1 -->|Yes| R1[200 OK + ERROR-100<br/>empty_params]
    
    E1 -->|No| E2{Invalid<br/>data_no?}
    E2 -->|Yes| R2[200 OK + ERROR-100<br/>invalid_data_no]
    
    E2 -->|No| E3{Invalid<br/>pk_seq?}
    E3 -->|Yes| R3[200 OK + ERROR-100<br/>invalid_pk_seq]
    
    E3 -->|No| E4{Wrong<br/>certkey?}
    E4 -->|Yes| R4[200 OK + ERROR-200<br/>wrong_certkey]
    
    E4 -->|No| E5{No packet<br/>definition?}
    E5 -->|Yes| R5[200 OK + ERROR-900<br/>no_modl_detail]
    
    E5 -->|No| E6{Packet length<br/>mismatch?}
    E6 -->|Yes| R6[200 OK + ERROR-300<br/>invalid_packet]
    
    E6 -->|No| Success[200 OK + SUCC-000]
    
    R1 --> Log[Log error + metrics]
    R2 --> Log
    R3 --> Log
    R4 --> Log
    R5 --> Log
    R6 --> Log
    Success --> Log
    
    note right of R1
        All responses return
        HTTP 200 OK status.
        Error info is in
        ResultStats JSON.
    end note
```

## 11. Key Implementation Details

### 11.1 pk_seq Sanitization
```go
// Remove special characters: - _ : space
// Truncate to 18 characters max
pkSeqStr = strings.Map(func(r rune) rune {
    if r == '-' || r == '_' || r == ':' || r == ' ' {
        return -1
    }
    return r
}, pkSeqStr)
maxPkSeqLen := 18
if len(pkSeqStr) > maxPkSeqLen {
    pkSeqStr = pkSeqStr[:maxPkSeqLen]
}
```

### 11.2 Validation Rules

**VAL_ITV (Interval Validation)**
- Check if value is numeric
- Check if value is within [MinValue, MaxValue]
- Mark invalid values with `InvalidValueMarker`

**VAL_ARR (Array Validation)**
- Check if value exists in comma-separated allowed list
- Mark invalid values with `InvalidValueMarker`

### 11.3 Invalid Value Marking
```go
// Prepend marker for invalid values
val = string(InvalidValueMarker) + val

// Later processing can detect and handle marked values
if len(value) > 0 && value[0] == InvalidValueMarker {
    // Handle invalid value (e.g., masking)
}
```

### 11.4 Channel-based Async Processing
```go
// Enqueue to raw packet channel (always)
s.rawPacketCh <- &data

// Enqueue to error channel (only if error)
if data.RecptnResultCode != ApiReceiveSuccess.ResultStats.ResultCode {
    s.errPacketCh <- &data
}
```

### 11.5 Response Format
All responses return HTTP 200 OK with error information in JSON:
```json
{
  "resultStats": {
    "resultCode": "SUCC-000",
    "resultMsg": "수신 완료"
  }
}
```

### 11.6 Packet Status Codes
- `S`: 정상 (OK)
- `E`: 오류 (Error)

### 11.7 Parse Code
- `A`: 자동 (Auto)
- `M`: 수동 (Manual)

## 12. API Contract

### 12.1 Send Packet API

**Request**
```
POST /sendPacket/:certkey/:data_no/:pk_seq/:serial_num/:packet
Query params: DQMCRR_OP (optional)
```

**Response (Success)**
```json
{
  "resultStats": {
    "resultCode": "SUCC-000",
    "resultMsg": "수신 완료"
  }
}
```

**Response (Invalid Packet Length)**
```json
{
  "resultStats": {
    "resultCode": "ERROR-300",
    "resultMsg": "패킷 형식이 올바르지 않습니다 패킷정의길이:100(H:20/T:80),수신패킷길이:95"
  }
}
```

### 12.2 Server Status API

**Request**
```
GET /serverStat/:certkey
```

**Response (Ready)**
```json
{
  "resultStats": {
    "resultCode": "READY-000",
    "resultMsg": "수신 가능"
  }
}
```

**Response (Invalid Certkey)**
```json
{
  "resultStats": {
    "resultCode": "ERROR-200",
    "resultMsg": "인증키가 올바르지 않습니다."
  }
}
```

## 13. Performance Considerations

### 13.1 Async Processing
- Raw packet data immediately enqueued to channel
- Response returned without waiting for parsing
- Background workers handle parsing and storage

### 13.2 Input Sanitization
- Trim spaces from serial numbers
- Clean pk_seq format variations
- Truncate oversized inputs

### 13.3 Logging Strategy
- Defer function captures timing and status
- Debug mode provides detailed field parsing logs
- Error logging includes request details

### 13.4 Memory Management
- Stream-based packet field extraction
- No buffering of entire packet list
- Validation errors collected incrementally
