create TABLE IF NOT EXISTS DEV.${vj_fsa_schema}.OPEN_PO_PREV_ASSIGNED_HISTORICAL (
	PK_ID VARCHAR(16777216),
	HASH_VALUE NUMBER(38,0),
	INSERT_DATE TIMESTAMP_LTZ(9),
	LAST_MODIFIED TIMESTAMP_LTZ(9),
    IS_VALID BOOLEAN DEFAULT TRUE,
  	FSA_INSERT_DATE TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);