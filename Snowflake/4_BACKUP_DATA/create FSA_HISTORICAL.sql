CREATE TABLE IF NOT EXISTS DEV.${FSA_PROD_SCHEMA}.FSA_HISTORICAL (
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
      ORIG_CAP_DDA DATE,
    NEW_AVAIL_DATE DATE,
    PREV_AVAIL_DATE DATE,
    PREV_CAPPING_DDA DATE,
    PREV_PO_INDICATOR NUMBER(38,0),
    PREV_PO_INDICATOR_ASSIGN NUMBER(38,0),
    PREV_PO_ORDER_NUMBER VARCHAR(16777216),
    PREV_PO_RECEIVE_BY_DATE DATE,
    /* 20230920 - KBY, RFS23-2696 Include FSA_COMPLETE */
    FSA_COMPLETE,
      FSA_INSERT_DATE TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
      IS_VALID BOOLEAN DEFAULT TRUE
);
