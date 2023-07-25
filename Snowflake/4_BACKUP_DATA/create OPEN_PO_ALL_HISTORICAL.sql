CREATE TABLE IF NOT EXISTS DEV.${FSA_PROD_SCHEMA}.OPEN_PO_ALL_HISTORICAL (
	PO_ROW_NO NUMBER(18,0),
	PO_ITEM_ID FLOAT,
	PO_ITEM_TYPE VARCHAR(11),
	ITEM_ID FLOAT,
	ITEM VARCHAR(4400),
	ITEM_ID_C NUMBER(38,0),
	ITEM_C VARCHAR(4400),
	ITEM_DISPLAY_NAME VARCHAR(2000),
	ASSEMBLY_ITEM_ID FLOAT,
	ASSEMBLY_ITEM VARCHAR(4400),
	ASSEMBLY_ITEM_DISPLAY_NAME VARCHAR(2000),
	ORDER_NUMBER VARCHAR(360),
	PURCHASE_ORDER_TRANSACTION_ID FLOAT,
	STATUS VARCHAR(32000),
	LOCATION VARCHAR(480),
	RECEIVE_BY_DATE DATE,
	NS_RECEIVE_BY_DATE DATE,
	UNIQUE_KEY FLOAT,
	QUANTITY_TO_BE_RECEIVED FLOAT,
	HASH_VALUE VARCHAR(16777216),
	FSA_LOAD_STATUS VARCHAR(16777216),
	INSERT_DATE TIMESTAMP_LTZ(9),
	PK_ID VARCHAR(16777216),
  	FSA_INSERT_DATE TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  	IS_VALID BOOLEAN DEFAULT TRUE
);
