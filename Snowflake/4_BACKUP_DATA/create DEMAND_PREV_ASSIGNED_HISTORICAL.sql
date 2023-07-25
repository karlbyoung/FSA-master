create TABLE IF NOT EXISTS DEV.${FSA_PROD_SCHEMA}.DEMAND_PREV_ASSIGNED_HISTORICAL (
	PK_ID VARCHAR(16777216),
	UNIQUE_KEY FLOAT,
	COMPONENT_ITEM_ID FLOAT,
	ORDER_NUMBER VARCHAR(16777216),
	NS_LINE_NUMBER VARCHAR(16777216),
	HASH_VALUE NUMBER(38,0),
	HASH_FSA_OUTPUT NUMBER(38,0),
	INSERT_DATE TIMESTAMP_LTZ(9),
	LAST_MODIFIED TIMESTAMP_LTZ(9),
	ROW_NO NUMBER(38,0),
	PREV_AVAIL_DATE DATE,
	PREV_CAPPING_DDA DATE,
	ORIG_CAP_DDA DATE,
	PO_INDICATOR NUMBER(38,0),
	PO_INDICATOR_ASSIGN NUMBER(38,0),
	PO_ORDER_NUMBER VARCHAR(16777216),
	PO_RECEIVE_BY_DATE DATE,
    IS_VALID BOOLEAN DEFAULT TRUE,
  	FSA_INSERT_DATE TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);