create TABLE IF NOT EXISTS DEV.${FSA_PROD_SCHEMA}.DEMAND_PREV_ASSIGNED_HISTORICAL (
	PK_ID VARCHAR(16777216),
    /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment, but not in HASH_VALUE */
	UNIQUE_KEY NUMBER,
	COMPONENT_ITEM_ID NUMBER,
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
    /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment */
    FR_PREV_DAYS NUMBER,
    IS_VALID BOOLEAN DEFAULT TRUE,
  	FSA_INSERT_DATE TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);