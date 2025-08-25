
---------------------------------------
-- TB_RECPTN_PACKET_DATA
---------------------------------------

CREATE TABLE IF NOT EXISTS TB_RECPTN_PACKET_DATA (
    PACKET_SEQ             long,
    TRNSMIT_SERVER_NO      int,
    DATA_NO                int,
    PK_SEQ                 long,
    AREA_CODE              varchar(10),
    MODL_SERIAL            varchar(20),
    DQMCRR_OP              int,
    PACKET                 varchar(1000),
    PACKET_STTUS_CODE      varchar(4),
    RECPTN_RESULT_CODE     varchar(10),
    RECPTN_RESULT_MSSAGE   varchar(200),
    PARS_SE_CODE           varchar(4),
    PARS_DT                datetime,
    REGIST_DE              varchar(8),
    REGIST_TIME            varchar(4),
    REGIST_DT              datetime
);

create index IDX_TB_RECPTN_PACKET_DATA_01 
on TB_RECPTN_PACKET_DATA(TRNSMIT_SERVER_NO);

create index IDX_TB_RECPTN_PACKET_DATA_02 
on TB_RECPTN_PACKET_DATA(PACKET_SEQ);

create index IDX_TB_RECPTN_PACKET_DATA_03
on TB_RECPTN_PACKET_DATA(REGIST_DT);

-- create index TB_RECPTN_PACKET_DATA_MODEL_SERIAL
-- on TB_RECPTN_PACKET_DATA(MODL_SERIAL);

-- create index TB_RECPTN_PACKET_DATA_AREA_CODE
-- on TB_RECPTN_PACKET_DATA(AREA_CODE);


---------------------------------------
-- TB_PACKET_PARS_DATA
---------------------------------------

CREATE TABLE IF NOT EXISTS TB_PACKET_PARS_DATA (
    PACKET_PARS_SEQ   long,
    PACKET_SEQ        long,
    TRNSMIT_SERVER_NO int,
    DATA_NO           int,
    REGIST_DT         datetime,
    REGIST_DE         varchar(8),
    SERVICE_SEQ       int,
    AREA_CODE         varchar(10),
    MODL_SERIAL       varchar(20),
    DQMCRR_OP         varchar(10),
    COLUMN0           varchar(50),
    COLUMN1           varchar(50),
    COLUMN2           varchar(50),
    COLUMN3           varchar(50),
    COLUMN4           varchar(50),
    COLUMN5           varchar(50),
    COLUMN6           varchar(50),
    COLUMN7           varchar(50),
    COLUMN8           varchar(50),
    COLUMN9           varchar(50),
    COLUMN10          varchar(50),
    COLUMN11          varchar(50),
    COLUMN12          varchar(50),
    COLUMN13          varchar(50),
    COLUMN14          varchar(50),
    COLUMN15          varchar(50),
    COLUMN16          varchar(50),
    COLUMN17          varchar(50),
    COLUMN18          varchar(50),
    COLUMN19          varchar(50),
    COLUMN20          varchar(50),
    COLUMN21          varchar(50),
    COLUMN22          varchar(50),
    COLUMN23          varchar(50),
    COLUMN24          varchar(50),
    COLUMN25          varchar(50),
    COLUMN26          varchar(50),
    COLUMN27          varchar(50),
    COLUMN28          varchar(50),
    COLUMN29          varchar(50),
    COLUMN30          varchar(50),
    COLUMN31          varchar(50),
    COLUMN32          varchar(50),
    COLUMN33          varchar(50),
    COLUMN34          varchar(50),
    COLUMN35          varchar(50),
    COLUMN36          varchar(50),
    COLUMN37          varchar(50),
    COLUMN38          varchar(50),
    COLUMN39          varchar(50),
    COLUMN40          varchar(50),
    COLUMN41          varchar(50),
    COLUMN42          varchar(50),
    COLUMN43          varchar(50),
    COLUMN44          varchar(50),
    COLUMN45          varchar(50),
    COLUMN46          varchar(50),
    COLUMN47          varchar(50),
    COLUMN48          varchar(50),
    COLUMN49          varchar(50),
    COLUMN50          varchar(50),
    COLUMN51          varchar(50),
    COLUMN52          varchar(50),
    COLUMN53          varchar(50),
    COLUMN54          varchar(50),
    COLUMN55          varchar(50),
    COLUMN56          varchar(50),
    COLUMN57          varchar(50),
    COLUMN58          varchar(50),
    COLUMN59          varchar(50),
    COLUMN60          varchar(50),
    COLUMN61          varchar(10),
    COLUMN62          varchar(10),
    COLUMN63          varchar(10)
);

create index IDX_PACKET_PARS_DATA_01 
on TB_PACKET_PARS_DATA(TRNSMIT_SERVER_NO);

create index IDX_PACKET_PARS_DATA_02
on TB_PACKET_PARS_DATA(PACKET_PARS_SEQ);

create index IDX_PACKET_PARS_DATA_03
on TB_PACKET_PARS_DATA(REGIST_DT);

create index IDX_PACKET_PARS_DATA_04
on TB_PACKET_PARS_DATA(PACKET_SEQ);


-- create index TB_PACKET_PARS_DATA_MODL_SERIAL
-- on TB_PACKET_PARS_DATA(MODL_SERIAL);

-- create index IDX_TB_PACKET_PARS_DATA
-- on TB_PACKET_PARS_DATA(SERVICE_SEQ);

-- create index TB_PACKET_PARS_DATA_AREA_CODE
-- on TB_PACKET_PARS_DATA(AREA_CODE);


---------------------------------------
-- NTB_ERR_LOG
---------------------------------------

CREATE TABLE IF NOT EXISTS NTB_ERR_LOG (
    PACKET_SEQ             long,
    TRNSMIT_SERVER_NO      int,
    DATA_NO                int,
    PK_SEQ                 long,
    MODL_SERIAL            varchar(20),
    PACKET                 varchar(1000),
    RECPTN_RESULT_CODE     varchar(10),
    RECPTN_RESULT_MSSAGE   varchar(200),
    REGIST_DE              varchar(8),
    REGIST_DT              datetime
);

create index IDX_NTB_ERR_LOG_01 
on NTB_ERR_LOG(TRNSMIT_SERVER_NO);

create index IDX_NTB_ERR_LOG_02 
on NTB_ERR_LOG(PACKET_SEQ);

create index IDX_NTB_ERR_LOG_03
on NTB_ERR_LOG(REGIST_DT);

---------------------------------------
-- TAG TAble
---------------------------------------

CREATE TAG TABLE IF NOT EXISTS TAG (
    NAME    varchar(100) primary key,
    TIME    datetime basetime,
    VALUE   double
);
