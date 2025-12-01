# h_data.go API Design Documentation

## 1. Overview
데이터 조회 API의 핵심 구현을 담당하는 핸들러로, Raw 데이터와 파싱된 데이터 조회를 지원합니다.

## 2. Architecture Diagram

```mermaid
graph TB
    Client[Client Request]
    
    Client -->|GET /data/:tsn/:data_no| handleData
    
    subgraph "handleData Handler"
        handleData[handleData]
        handleData --> ValidateParams[Parameter Validation]
        ValidateParams --> ValidateCertkey[Certkey Validation]
        ValidateCertkey --> ValidateTime[Time Range Validation]
        ValidateTime --> OpenConn[Open DB Connection]
        OpenConn --> RouteHandler{isPars?}
        
        RouteHandler -->|Yes| handleParsData
        RouteHandler -->|No| handleRawData
    end
    
    subgraph "handleParsData Flow"
        handleParsData[handleParsData]
        handleParsData --> GetPacketDef[Get Packet Definition]
        GetPacketDef --> CheckDQM[Check DQM Public Status]
        CheckDQM --> CheckOrgn[Check ORGN Retrive Status]
        CheckOrgn --> CheckMasking[Check Masking Requirements]
        CheckMasking --> BuildParsSQL[Build SQL Query]
        BuildParsSQL --> ExecuteParsQuery[Execute Query]
        ExecuteParsQuery --> StreamParsJSON[Stream JSON Response]
    end
    
    subgraph "handleRawData Flow"
        handleRawData[handleRawData]
        handleRawData --> BuildRawSQL[Build SQL Query]
        BuildRawSQL --> ExecuteRawQuery[Execute Query]
        ExecuteRawQuery --> StreamRawJSON[Stream JSON Response]
    end
    
    subgraph "Data Query Logic"
        BuildParsSQL --> DataNoCheck1{dataNo == 1?}
        DataNoCheck1 -->|Yes| TimeRangeQuery1[Time Range Query]
        DataNoCheck1 -->|No| ArrivalTimeQuery1[Arrival Time Query]
        
        BuildRawSQL --> DataNoCheck2{dataNo == 1?}
        DataNoCheck2 -->|Yes| TimeRangeQuery2[Time Range Query]
        DataNoCheck2 -->|No| ArrivalTimeQuery2[Arrival Time Query]
        
        ArrivalTimeQuery1 --> UpdateDQM1[Update Last Arrival Time]
        ArrivalTimeQuery2 --> UpdateDQM2[Update Last Arrival Time]
    end
    
    subgraph "Databases"
        TSDB[(Time Series DB<br/>TB_PACKET_PARS_DATA<br/>TB_RECPTN_PACKET_DATA)]
        RDB[(Relational DB<br/>MODL_PACKET_DQM)]
        
        ExecuteParsQuery --> TSDB
        ExecuteRawQuery --> TSDB
        ArrivalTimeQuery1 --> RDB
        ArrivalTimeQuery2 --> RDB
        UpdateDQM1 --> RDB
        UpdateDQM2 --> RDB
    end
    
    subgraph "Monitoring & Logging"
        StreamParsJSON --> Defer[Defer Stats & Logs]
        StreamRawJSON --> Defer
        Defer --> StatCh[Statistics Channel]
        Defer --> Logger[Logger]
        Defer --> Metrics[Metrics Collector]
    end
    
    StreamParsJSON --> Response[JSON Response]
    StreamRawJSON --> Response
```

## 3. Sequence Diagram

```mermaid
sequenceDiagram
    participant Client
    participant handleData
    participant Cache as Cache Layer
    participant TSDB as Time Series DB
    participant RDB as Relational DB
    participant Monitor as Monitoring
    
    Client->>handleData: GET /data/:tsn/:data_no?certKey=xxx&start=xxx&end=xxx
    
    Note over handleData: Start timer & defer stats
    
    handleData->>handleData: Validate tsn, data_no
    handleData->>Cache: getOrgKey(certkey)
    Cache-->>handleData: OrgKey info
    handleData->>handleData: Check validity period
    
    handleData->>handleData: Parse time range
    Note over handleData: Default: last 30 mins
    
    handleData->>TSDB: Open connection
    TSDB-->>handleData: Connection
    
    alt ISPARS = Y (Parsed Data)
        handleData->>handleParsData: Route to parsed handler
        
        handleParsData->>Cache: getPacketDefinition(tsn, dataNo)
        Cache-->>handleParsData: Packet definition
        
        handleParsData->>Cache: getModelDqmInfo(tsn)
        Cache-->>handleParsData: DQM info (check PUBLIC_YN)
        
        handleParsData->>Cache: getModelOrgnPublic(certKeySeq, tsn)
        Cache-->>handleParsData: ORGN public info (check RETRIVE_YN)
        
        handleParsData->>handleParsData: Determine masking flag
        
        alt dataNo == 1 (Time Range Query)
            handleParsData->>TSDB: Query by time range
        else dataNo != 1 (Arrival Time Query)
            handleParsData->>RDB: Get last arrival time
            RDB-->>handleParsData: MODL_PACKET_DQM info
            handleParsData->>TSDB: Query by arrival time
        end
        
        TSDB-->>handleParsData: Row stream
        
        loop For each row
            handleParsData->>handleParsData: Apply masking if needed
            handleParsData->>Client: Stream JSON fragment
        end
        
        opt dataNo != 1
            handleParsData->>RDB: Update last arrival time
        end
        
    else ISPARS != Y (Raw Data)
        handleData->>handleRawData: Route to raw handler
        
        alt dataNo == 1 (Time Range Query)
            handleRawData->>TSDB: Query by time range
        else dataNo != 1 (Arrival Time Query)
            handleRawData->>RDB: Get last arrival time
            RDB-->>handleRawData: MODL_PACKET_DQM info
            handleRawData->>TSDB: Query by arrival time
        end
        
        TSDB-->>handleRawData: Row stream
        
        loop For each row
            handleRawData->>Client: Stream JSON fragment
        end
        
        opt dataNo != 1
            handleRawData->>RDB: Update last arrival time
        end
    end
    
    Note over handleData: Defer execution
    handleData->>Monitor: Send statistics
    handleData->>Monitor: Log request/response
    handleData->>Monitor: Send metrics
```

## 4. Data Flow Diagram

```mermaid
flowchart LR
    subgraph "Input Parameters"
        PathParams[Path Parameters<br/>tsn, data_no]
        QueryParams[Query Parameters<br/>certKey, start, end<br/>modelSerial, areaCode<br/>ISPARS, RQSRC]
    end
    
    subgraph "Validation Layer"
        V1[Validate TSN]
        V2[Validate CertKey]
        V3[Validate Time Range]
        V4[Validate dataNo]
    end
    
    subgraph "Authorization Layer"
        A1[Check OrgKey<br/>Validity Period]
        A2[Check DQM<br/>PUBLIC_YN]
        A3[Check ORGN<br/>RETRIVE_YN]
        A4[Determine<br/>Masking Flag]
    end
    
    subgraph "Query Strategy"
        Q1{dataNo == 1?}
        Q2[Time Range Query<br/>REGIST_DT based]
        Q3[Arrival Time Query<br/>_ARRIVAL_TIME based]
        Q4[Apply LIMIT if set]
    end
    
    subgraph "Data Processing"
        P1{isPars?}
        P2[Process Parsed Data<br/>with Definition]
        P3[Process Raw Data<br/>Packet String]
        P4[Apply Masking<br/>if Required]
    end
    
    subgraph "Output Layer"
        O1[Stream JSON<br/>Response]
        O2[Update Statistics]
        O3[Update DQM<br/>Last Arrival Time]
    end
    
    PathParams --> V1
    QueryParams --> V2
    V1 --> V3
    V2 --> A1
    V3 --> V4
    V4 --> A2
    
    A1 --> A2
    A2 --> A3
    A3 --> A4
    
    A4 --> Q1
    Q1 -->|Yes| Q2
    Q1 -->|No| Q3
    Q3 --> Q4
    
    Q2 --> P1
    Q4 --> P1
    
    P1 -->|Yes| P2
    P1 -->|No| P3
    
    P2 --> P4
    P4 --> O1
    P3 --> O1
    
    O1 --> O2
    O1 --> O3
```

## 5. Component Interaction

```mermaid
graph TB
    subgraph "HTTP Layer"
        GinContext[Gin Context]
    end
    
    subgraph "Handler Functions"
        handleData[handleData]
        handleParsData[handleParsData]
        handleRawData[handleRawData]
    end
    
    subgraph "Cache Functions"
        getOrgKey[getOrgKey]
        getPacketDefinition[getPacketDefinition]
        getModelDqmInfo[getModelDqmInfo]
        getModelOrgnPublic[getModelOrgnPublic]
    end
    
    subgraph "Database Functions"
        SelectModlPacketDqm[SelectModlPacketDqm]
        UpsertModlPacketDqm[UpsertModlPacketDqm]
    end
    
    subgraph "Database Connections"
        TSDBConn[TSDB Connection<br/>api.Conn]
        RDBConn[RDB Connection<br/>sql.DB]
    end
    
    subgraph "External Systems"
        StatChannel[Statistics Channel]
        MetricsCollector[Metrics Collector]
        Logger[Logger]
    end
    
    GinContext --> handleData
    handleData --> getOrgKey
    handleData --> handleParsData
    handleData --> handleRawData
    
    handleParsData --> getPacketDefinition
    handleParsData --> getModelDqmInfo
    handleParsData --> getModelOrgnPublic
    handleParsData --> SelectModlPacketDqm
    handleParsData --> UpsertModlPacketDqm
    handleParsData --> TSDBConn
    
    handleRawData --> SelectModlPacketDqm
    handleRawData --> UpsertModlPacketDqm
    handleRawData --> TSDBConn
    
    SelectModlPacketDqm --> RDBConn
    UpsertModlPacketDqm --> RDBConn
    
    handleData --> StatChannel
    handleData --> MetricsCollector
    handleData --> Logger
```

## 6. Error Handling Flow

```mermaid
flowchart TD
    Start[Request Start]
    
    Start --> CheckTSN{Valid TSN?}
    CheckTSN -->|No| E1[400 BadRequest<br/>invalid_tsn]
    
    CheckTSN -->|Yes| CheckCertKey{Valid CertKey?}
    CheckCertKey -->|No| E2[403 Forbidden<br/>wrong_certkey]
    
    CheckCertKey -->|Yes| CheckExpiry{Within<br/>Valid Period?}
    CheckExpiry -->|No| E3[403 Forbidden<br/>certkey_expired]
    
    CheckExpiry -->|Yes| CheckDataNo{Valid<br/>dataNo?}
    CheckDataNo -->|No| E4[400 BadRequest<br/>invalid_data_no]
    
    CheckDataNo -->|Yes| CheckTime{Valid Time<br/>Format?}
    CheckTime -->|No| E5[400 BadRequest<br/>invalid_start/end_time]
    
    CheckTime -->|Yes| OpenDB{DB Connection<br/>Success?}
    OpenDB -->|No| E6[500 Server Error<br/>connection_failed]
    
    OpenDB -->|Yes| CheckPublic{DQM Public?}
    CheckPublic -->|No| E7[403 Forbidden<br/>ERROR-650]
    
    CheckPublic -->|Yes| CheckRetrive{ORGN Retrive?}
    CheckRetrive -->|No| E8[403 Forbidden<br/>ERROR-660]
    
    CheckRetrive -->|Yes| ExecuteQuery[Execute Query]
    ExecuteQuery --> CheckQueryError{Query<br/>Success?}
    CheckQueryError -->|No| E9[500 Server Error<br/>query_failed]
    
    CheckQueryError -->|Yes| StreamData[Stream Response]
    StreamData --> CheckCancel{Request<br/>Cancelled?}
    CheckCancel -->|Yes| LogCancel[Log Cancellation]
    CheckCancel -->|No| Success[200 OK]
    
    E1 --> LogError[Log Error]
    E2 --> LogError
    E3 --> LogError
    E4 --> LogError
    E5 --> LogError
    E6 --> LogError
    E7 --> LogError
    E8 --> LogError
    E9 --> LogError
    
    LogError --> SendMetrics[Send Error Metrics]
    LogCancel --> SendMetrics
    Success --> SendMetrics
```

## 7. State Diagram

```mermaid
stateDiagram-v2
    [*] --> RequestReceived
    
    RequestReceived --> Validating: Parse parameters
    
    Validating --> Authorized: Validation passed
    Validating --> ErrorResponse: Validation failed
    
    Authorized --> CheckingPermissions: Check certkey validity
    
    CheckingPermissions --> QueryPreparation: Permissions OK
    CheckingPermissions --> ErrorResponse: Permission denied
    
    QueryPreparation --> DetermineQueryType: Build SQL
    
    DetermineQueryType --> TimeRangeQuery: dataNo == 1
    DetermineQueryType --> ArrivalTimeQuery: dataNo != 1
    
    TimeRangeQuery --> Executing: Query ready
    ArrivalTimeQuery --> LoadLastArrival: Load DQM info
    
    LoadLastArrival --> Executing: Query ready
    LoadLastArrival --> ErrorResponse: RDB error
    
    Executing --> Streaming: Query successful
    Executing --> ErrorResponse: Query failed
    
    Streaming --> ProcessingRow: Has next row
    ProcessingRow --> ApplyMasking: isPars && masking enabled
    ProcessingRow --> WriteJSON: No masking
    
    ApplyMasking --> WriteJSON: Masking applied
    
    WriteJSON --> CheckMore: Row written
    
    CheckMore --> ProcessingRow: More rows
    CheckMore --> UpdateDQM: No more rows (dataNo != 1)
    CheckMore --> FinalizeResponse: No more rows (dataNo == 1)
    
    UpdateDQM --> FinalizeResponse: DQM updated
    
    FinalizeResponse --> SendStats: Response complete
    
    SendStats --> [*]: Done
    
    ErrorResponse --> SendStats: Log error
    
    note right of Streaming
        Request cancellation
        can occur at any time
    end note
```

## 8. Key Implementation Details

### 8.1 Time Range Logic
```
IF start == "" && end == "":
    endTime = now
    startTime = now - 30 minutes
ELSE IF start == "" && end != "":
    IF modelSerial == "":
        startTime = endTime - 60 minutes
    ELSE:
        startTime = endTime - 30 minutes
ELSE IF start != "" && end == "":
    endTime = startTime + 60 minutes
```

### 8.2 Data Query Strategy
- **dataNo = 1**: Time range query using `REGIST_DT`
- **dataNo = 2 or 3**: Arrival time query using `_ARRIVAL_TIME` with DQM tracking

### 8.3 Masking Logic
```
activateMasking = dqmInfo.Masking OR orgnPublic.Masking

IF value[0] == InvalidValueMarker:
    IF activateMasking:
        value = repeat(MaskingStrValue, width)
    ELSE:
        value = value[1:]  // Remove marker
```

### 8.4 Response Streaming
- Uses chunked JSON streaming for memory efficiency
- Supports request cancellation via context
- No buffering of entire result set

### 8.5 Statistics & Monitoring
- Request/response logging with latency
- Query statistics per organization
- Metrics collection (count, latency, errors)
- SQL query logging in debug mode

## 9. Database Schema References

### 9.1 Time Series Tables
- `TB_PACKET_PARS_DATA`: Parsed packet data
- `TB_RECPTN_PACKET_DATA`: Raw packet data

### 9.2 Relational Tables
- `MODL_DQM_INFO`: Data quality metadata
- `MODL_ORGN_PUBLIC`: Organization access control
- `MODL_PACKET_DQM`: Last arrival time tracking

## 10. API Contract

### Request
```
GET /data/:tsn/:data_no?certKey=xxx&start=yyyymmddHHMMSS&end=yyyymmddHHMMSS&modelSerial=xxx&areaCode=xxx&ISPARS=Y
```

### Response (Success)
```json
{
  "dataNo": "1",
  "datasetNo": "123",
  "resultCode": "SUCC-000",
  "resultMsg": "조회 완료",
  "startDateTime": "20231201120000",
  "endDateTime": "20231201130000",
  "resultdata": [...]
}
```

### Error Codes
- `400 BadRequest`: Invalid parameters
- `403 Forbidden`: Authentication/authorization failed
- `404 NotFound`: Resource not found
- `500 InternalServerError`: Server error
