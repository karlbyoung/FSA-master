CREATE TABLE IF NOT EXISTS DEV.${vj_fsa_schema}.FSA_HISTORICAL (
    PO_ID NUMBER(23,5),
    FSA_LOAD_STATUS VARCHAR(16777216),
    ID NUMBER(18,0),
    /* 20230804 - KBY, RSF23-1861 - Convert ID's from FLOAT to NUMBER */
    ITEM_ID NUMBER,
    ORIGINAL_DDA DATE,
    SEQUENCING_DDA DATE,
    ORDER_NUMBER VARCHAR(360),
    TOTAL_AVAIL_QTY NUMBER,
    QUANTITY NUMBER,
    REMAINING_AVAIL_QTY NUMBER,
    SUM_ROLLUP NUMBER,
    PRELIM_EDD DATE,
    ITEM VARCHAR(4400),
    LOCATION VARCHAR(480),
    NS_LINE_NUMBER VARCHAR(16777216),
    ROW_NO NUMBER(38,0),
    PRIORITY NUMBER(38,0),
    SEQ NUMBER(38,0),
    TRANSACTION_TYPE VARCHAR(16777216),
    TYPE_NAME VARCHAR(480),
    TRANSACTION_ID VARCHAR(16777216),
    LINE_ID VARCHAR(16777216),
    /* 20230804 - KBY, RSF23-1861 - Convert ID's from FLOAT to NUMBER */
    UNIQUE_KEY NUMBER,
    PO_SLIPPAGE BOOLEAN,
    ITEM_ROW_NO NUMBER(18,0),
    SOURCE_TYPE VARCHAR(8),
    COMPONENT_ITEM VARCHAR(4400),
    COMPONENT_ITEM_ID NUMBER(38,0),
    /* 20230804 - KBY, RSF23-1861 - Convert ID's from FLOAT to NUMBER */
    ITEM_ID_BY_TRANSACTION_TYPE NUMBER,
    SOURCE_LOAD_DATE TIMESTAMP_LTZ(9),
    PK_ID VARCHAR(16777216),
    IS_ASSEMBLY_COMPONENT BOOLEAN,
    CREATE_DATE TIMESTAMP_TZ(9),
    /* 20230804 - KBY, RSF23-1861 - Convert ID's from FLOAT to NUMBER */
    PO_INDICATOR NUMBER,
    PO_UPDATE_DATETIME VARCHAR(16777216),
    PO_ORDER_NUMBER VARCHAR(16777216),
    PO_QUANTITY_REMAINING NUMBER,
    PO_RECEIVE_BY_DATE DATE,
    PO_INDICATOR_ASSIGN NUMBER(1,0),
    PO_TOTAL_QUANTITY_TO_BE_RECEIVED NUMBER,
    SHARED_ORDER_NUMBER VARCHAR(16777216),
    FR_PREV_DAYS NUMBER,
    BOB_ORDER_NUMBER VARCHAR(360),
    AVAIL_DATE DATE,
    BOB_ITEM VARCHAR(4400),
    FREDD DATE,
    BUCKET_ON_AVAIL_DATE VARCHAR(16777216),
    BUCKET_DATE_ON_AVAIL_DATE DATE,
    IS_GT_15_BIZDAYS BOOLEAN,
    IF_BUCKET1 DATE,
    IF_BUCKET2 DATE,
    IF_BUCKET3 DATE,
    IF_BUCKET4 DATE,
    IF_BUCKET5 DATE,
    IF_BUCKET6 DATE,
    IF_BUCKET7 DATE,
    IF_BUCKET8 DATE,
    IF_BUCKET9 DATE,
    IF_BUCKET10 DATE,
    IF_BUCKET11 DATE,
    IF_BUCKET12 DATE,
    IF_BUCKET13 DATE,
    IF_BUCKET14 DATE,
    FSA_UPDATED_ORIGINAL_DDA DATE,
    ITEM_AVAIL_DATE DATE,
    FSA_OUTPUT_STATUS VARCHAR(16777216),
    CAPPING_DDA DATE,
	  /* 20231109 - KBY, RFS23-3534 - Include FR Release Date */
    FR_RELEASE_DATE DATE,
    ORIG_CAP_DDA DATE,
    NEW_AVAIL_DATE DATE,
    PREV_AVAIL_DATE DATE,
    PREV_CAPPING_DDA DATE,
    PREV_PO_INDICATOR NUMBER(38,0),
    PREV_PO_INDICATOR_ASSIGN NUMBER(38,0),
    PREV_PO_ORDER_NUMBER VARCHAR(16777216),
    PREV_PO_RECEIVE_BY_DATE DATE,
    /* 20230920 - KBY, RFS23-2696 Include FSA_COMPLETE */
    FSA_COMPLETE VARCHAR(16777216),
    /* 20231128 - KBY, RSF23-3656 - Include NO_PO_DATE */ 
    NO_PO_DATE TIMESTAMP_LTZ,
      FSA_INSERT_DATE TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
      IS_VALID BOOLEAN DEFAULT TRUE
);

/*
--    20231128 - KBY, RSF23-3656 - Include NO_PO_DATE
--    Expand FSA_HISTORICAL to include NO_PO_DATE
CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}.FSA_HISTORICAL_NEW AS
  SELECT 
    PO_ID, 
    FSA_LOAD_STATUS, 
    ID, 
    ITEM_ID, 
    ORIGINAL_DDA, 
    SEQUENCING_DDA, 
    ORDER_NUMBER, 
    TOTAL_AVAIL_QTY, 
    QUANTITY, 
    REMAINING_AVAIL_QTY, 
    SUM_ROLLUP, 
    PRELIM_EDD, 
    ITEM, 
    LOCATION, 
    NS_LINE_NUMBER, 
    ROW_NO, 
    PRIORITY, 
    SEQ, 
    TRANSACTION_TYPE, 
    TYPE_NAME, 
    TRANSACTION_ID, 
    LINE_ID, 
    UNIQUE_KEY, 
    PO_SLIPPAGE, 
    ITEM_ROW_NO, 
    SOURCE_TYPE, 
    COMPONENT_ITEM, 
    COMPONENT_ITEM_ID, 
    ITEM_ID_BY_TRANSACTION_TYPE, 
    SOURCE_LOAD_DATE, 
    PK_ID, 
    IS_ASSEMBLY_COMPONENT, 
    CREATE_DATE, 
    PO_INDICATOR, 
    PO_UPDATE_DATETIME, 
    PO_ORDER_NUMBER, 
    PO_QUANTITY_REMAINING, 
    PO_RECEIVE_BY_DATE, 
    PO_INDICATOR_ASSIGN, 
    PO_TOTAL_QUANTITY_TO_BE_RECEIVED, 
    SHARED_ORDER_NUMBER,
    FR_PREV_DAYS, 
    BOB_ORDER_NUMBER, 
    AVAIL_DATE, 
    BOB_ITEM, 
    FREDD, 
    BUCKET_ON_AVAIL_DATE, 
    BUCKET_DATE_ON_AVAIL_DATE, 
    IS_GT_15_BIZDAYS, 
    IF_BUCKET1, 
    IF_BUCKET2, 
    IF_BUCKET3, 
    IF_BUCKET4, 
    IF_BUCKET5, 
    IF_BUCKET6, 
    IF_BUCKET7, 
    IF_BUCKET8, 
    IF_BUCKET9, 
    IF_BUCKET10, 
    IF_BUCKET11, 
    IF_BUCKET12, 
    IF_BUCKET13, 
    IF_BUCKET14, 
    FSA_UPDATED_ORIGINAL_DDA, 
    ITEM_AVAIL_DATE, 
    FSA_OUTPUT_STATUS, 
    CAPPING_DDA, 
    FR_RELEASE_DATE, 
    ORIG_CAP_DDA, 
    NEW_AVAIL_DATE, 
    PREV_AVAIL_DATE, 
    PREV_CAPPING_DDA, 
    PREV_PO_INDICATOR, 
    PREV_PO_INDICATOR_ASSIGN, 
    PREV_PO_ORDER_NUMBER, 
    PREV_PO_RECEIVE_BY_DATE, 
    FSA_COMPLETE,
    NULL::TIMESTAMP_LTZ NO_PO_DATE,
    FSA_INSERT_DATE,
    IS_VALID
FROM DEV.NETSUITE2_FSA.FSA_HISTORICAL;
*/