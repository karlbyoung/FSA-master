CREATE TABLE IF NOT EXISTS DEV.${FSA_PROD_SCHEMA}.DEMAND_PO_ALL_HISTORICAL (
	FK_ID NUMBER(38,0),
	ORDER_NUMBER VARCHAR(360),
	UNIQUE_KEY FLOAT,
	DDA DATE,
	DDA_MODIFIED DATE,
	ORIGINAL_DDA DATE,
	SEQUENCING_DDA DATE,
	TRANSACTION_ID VARCHAR(16777216),
	LINE_ID VARCHAR(16777216),
	TRANSACTION_TYPE VARCHAR(16777216),
	TYPE_NAME VARCHAR(480),
	TOTAL_AMT FLOAT,
	PRIORITY_LEVEL FLOAT,
	NS_LINE_NUMBER VARCHAR(16777216),
	ITEM VARCHAR(4400),
	ITEM_ID FLOAT,
	COMPONENT_ITEM_ID NUMBER(38,0),
	COMPONENT_ITEM VARCHAR(4400),
	QUANTITY FLOAT,
	/* 2023-07-25, KBY, expand Historical to cover new columns */
	TOTAL_AVAIL_QTY FLOAT,
	TOTAL_AVAIL_QTY_FWD FLOAT,
	TOTAL_AVAIL_QTY_NONFWD FLOAT,
	QTY_ORDERED FLOAT,
	COMPONENT_QTY_ORDERED FLOAT,
	LOCATION VARCHAR(480),
	SOURCE_TYPE VARCHAR(8),
	IS_ASSEMBLY_COMPONENT BOOLEAN,
	CREATE_DATE TIMESTAMP_TZ(9),
	PO_SLIPPAGE BOOLEAN,
	HASH_VALUE VARCHAR(16777216),
	FSA_LOAD_STATUS VARCHAR(16777216),
	INSERT_DATE TIMESTAMP_LTZ(9),
	ID NUMBER(18,0),
	PK_ID VARCHAR(16777216),
    FSA_INSERT_DATE TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  	IS_VALID BOOLEAN DEFAULT TRUE
);