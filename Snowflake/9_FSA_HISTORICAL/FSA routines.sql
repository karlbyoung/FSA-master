-- functions and procedures to support historical retrieval of FSA file
--
--  Function LATEST_FSA()
--		returns most recent version of FSA
--		ex: SELECT * FROM TABLE(LATEST_FSA())
--
--  Function LATEST_FSA(MAX_DATE)
--		returns the version of FSA table AT or BEFORE the given MAX_DATE
--		ex: SELECT * FROM TABLE(LATEST_FSA('2023-04-25 23:59'::timestamp_ltz))
--
--  Procedure MAKE_FSA()
--		Creates FSA table from the most recent version, named FSA
--		ex: CALL MAKE_FSA()
--
--  Procedure MAKE_FSA(MAX_DATE)
--		Creates FSA table from the version AT or BEFORE the given MAX_DATE, named FSA
--		ex: CALL MAKE_FSA('2023-04-25 23:59')
--		ex: CALL MAKE_FSA('2023-04-26') 		-- same as previous (within a minute anyway)
--
--  Procedure MAKE_FSA(MAX_DATE,TARGET_TABLE)
--		Creates FSA table from the version AT or BEFORE the given MAX_DATE, named TARGET_TABLE
--		ex: CALL MAKE_FSA('2023-04-25 23:59','TEST_FSA')
--


CREATE OR REPLACE FUNCTION DEV.${FSA_CURRENT_SCHEMA}.LATEST_FSA (MAX_DATE timestamp_ltz)
  RETURNS TABLE (
	PO_ID NUMBER(23,5),
	FSA_LOAD_STATUS VARCHAR(16777216),
	ID NUMBER(18,0),
	ITEM_ID FLOAT,
	ORIGINAL_DDA DATE,
	SEQUENCING_DDA DATE,
	ORDER_NUMBER VARCHAR(360),
	TOTAL_AVAIL_QTY FLOAT,
	QUANTITY FLOAT,
	REMAINING_AVAIL_QTY FLOAT,
	SUM_ROLLUP FLOAT,
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
	UNIQUE_KEY FLOAT,
	PO_SLIPPAGE BOOLEAN,
	ITEM_ROW_NO NUMBER(18,0),
	SOURCE_TYPE VARCHAR(8),
	COMPONENT_ITEM VARCHAR(4400),
	COMPONENT_ITEM_ID NUMBER(38,0),
	ITEM_ID_BY_TRANSACTION_TYPE FLOAT,
	SOURCE_LOAD_DATE TIMESTAMP_LTZ(9),
	PK_ID VARCHAR(16777216),
	IS_ASSEMBLY_COMPONENT BOOLEAN,
	CREATE_DATE TIMESTAMP_TZ(9),
	PO_INDICATOR FLOAT,
	PO_UPDATE_DATETIME VARCHAR(16777216),
	PO_ORDER_NUMBER VARCHAR(16777216),
	PO_QUANTITY_REMAINING FLOAT,
	PO_RECEIVE_BY_DATE DATE,
	PO_INDICATOR_ASSIGN NUMBER(1,0),
	PO_TOTAL_QUANTITY_TO_BE_RECEIVED FLOAT,
	SHARED_ORDER_NUMBER VARCHAR(16777216),
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
	PREV_PO_RECEIVE_BY_DATE DATE) AS
$$
    select t1.* exclude fsa_insert_date 
      from dev.${FSA_CURRENT_SCHEMA}.fsa_historical t1
      join (
        select distinct source_load_date,max(fsa_insert_date) over () fsa_insert_date
        from dev.${FSA_CURRENT_SCHEMA}.fsa_historical
        where source_load_date = 
          (select max(source_load_date) source_load_date
            from dev.${FSA_CURRENT_SCHEMA}.fsa_historical
            where source_load_date <= MAX_DATE)) t2
      on t1.source_load_date = t2.source_load_date
        and t1.fsa_insert_date = t2.fsa_insert_date
$$;

CREATE OR REPLACE FUNCTION DEV.${FSA_CURRENT_SCHEMA}.LATEST_FSA ()
  RETURNS TABLE (
	PO_ID NUMBER(23,5),
	FSA_LOAD_STATUS VARCHAR(16777216),
	ID NUMBER(18,0),
	ITEM_ID FLOAT,
	ORIGINAL_DDA DATE,
	SEQUENCING_DDA DATE,
	ORDER_NUMBER VARCHAR(360),
	TOTAL_AVAIL_QTY FLOAT,
	QUANTITY FLOAT,
	REMAINING_AVAIL_QTY FLOAT,
	SUM_ROLLUP FLOAT,
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
	UNIQUE_KEY FLOAT,
	PO_SLIPPAGE BOOLEAN,
	ITEM_ROW_NO NUMBER(18,0),
	SOURCE_TYPE VARCHAR(8),
	COMPONENT_ITEM VARCHAR(4400),
	COMPONENT_ITEM_ID NUMBER(38,0),
	ITEM_ID_BY_TRANSACTION_TYPE FLOAT,
	SOURCE_LOAD_DATE TIMESTAMP_LTZ(9),
	PK_ID VARCHAR(16777216),
	IS_ASSEMBLY_COMPONENT BOOLEAN,
	CREATE_DATE TIMESTAMP_TZ(9),
	PO_INDICATOR FLOAT,
	PO_UPDATE_DATETIME VARCHAR(16777216),
	PO_ORDER_NUMBER VARCHAR(16777216),
	PO_QUANTITY_REMAINING FLOAT,
	PO_RECEIVE_BY_DATE DATE,
	PO_INDICATOR_ASSIGN NUMBER(1,0),
	PO_TOTAL_QUANTITY_TO_BE_RECEIVED FLOAT,
	SHARED_ORDER_NUMBER VARCHAR(16777216),
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
	PREV_PO_RECEIVE_BY_DATE DATE) AS
$$
	select * from TABLE(DEV.${FSA_CURRENT_SCHEMA}.LATEST_FSA(current_timestamp()))
$$;

CREATE OR REPLACE PROCEDURE DEV.${FSA_CURRENT_SCHEMA}.MAKE_FSA(MAX_DATE timestamp_ltz,TARGET_TABLE text)
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  BEGIN
  LET rs RESULTSET := (CREATE OR REPLACE TABLE IDENTIFIER(:TARGET_TABLE) AS SELECT * FROM TABLE(DEV.${FSA_CURRENT_SCHEMA}.LATEST_FSA(:MAX_DATE)));
  return TABLE(rs);
  END;
$$;

CREATE OR REPLACE PROCEDURE DEV.${FSA_CURRENT_SCHEMA}.MAKE_FSA(MAX_DATE timestamp_ltz)
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  BEGIN
  LET rs RESULTSET := (CALL DEV.${FSA_CURRENT_SCHEMA}.MAKE_FSA(:MAX_DATE,'DEV.${FSA_CURRENT_SCHEMA}.FSA'));
  return TABLE(rs);
  END;
$$;

CREATE OR REPLACE PROCEDURE DEV.${FSA_CURRENT_SCHEMA}.MAKE_FSA()
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  BEGIN
  LET rs RESULTSET := (CALL DEV.${FSA_CURRENT_SCHEMA}.MAKE_FSA(:MAX_DATE));
  return TABLE(rs);
  END;
$$;
