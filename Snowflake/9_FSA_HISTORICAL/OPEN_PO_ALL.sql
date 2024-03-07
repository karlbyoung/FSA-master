-- functions and procedures to support historical retrieval of OPEN_PO_ALL file
--
--  Function LATEST_OPEN_PO_ALL()
--		returns most recent version of OPEN_PO_ALL
--		ex: SELECT * FROM TABLE(LATEST_OPEN_PO_ALL())
--
--  Function LATEST_OPEN_PO_ALL(MAX_DATE)
--		returns the version of OPEN_PO_ALL table AT or BEFORE the given MAX_DATE
--		ex: SELECT * FROM TABLE(LATEST_OPEN_PO_ALL('2023-04-25 23:59'::timestamp_ltz))
--
--  Procedure MAKE_OPEN_PO_ALL()
--		Creates OPEN_PO_ALL table from the most recent version, named OPEN_PO_ALL
--		ex: CALL MAKE_OPEN_PO_ALL()
--
--  Procedure MAKE_OPEN_PO_ALL(MAX_DATE)
--		Creates OPEN_PO_ALL table from the version AT or BEFORE the given MAX_DATE, named OPEN_PO_ALL
--		ex: CALL MAKE_OPEN_PO_ALL('2023-04-25 23:59')
--		ex: CALL MAKE_OPEN_PO_ALL('2023-04-26') 		-- same as previous (within a minute anyway)
--
--  Procedure MAKE_OPEN_PO_ALL(MAX_DATE,TARGET_TABLE)
--		Creates FOPEN_PO_ALLSA table from the version AT or BEFORE the given MAX_DATE, named TARGET_TABLE
--		ex: CALL MAKE_OPEN_PO_ALL('2023-04-25 23:59','TEST_OPEN_PO_ALL')
--

CREATE OR REPLACE FUNCTION DEV.${vj_fsa_schema}.LATEST_OPEN_PO_ALL (MAX_DATE timestamp_ltz)
  RETURNS TABLE (
	PO_ROW_NO NUMBER(18,0),
	PO_ITEM_ID NUMBER,
	PO_ITEM_TYPE VARCHAR,
	ITEM_ID NUMBER,
	ITEM VARCHAR(4400),
	ITEM_ID_C NUMBER(38,0),
	ITEM_C VARCHAR(4400),
	ITEM_DISPLAY_NAME VARCHAR(2000),
	ASSEMBLY_ITEM_ID NUMBER,
	ASSEMBLY_ITEM VARCHAR(4400),
	ASSEMBLY_ITEM_DISPLAY_NAME VARCHAR(2000),
	ORDER_NUMBER VARCHAR(360),
	PURCHASE_ORDER_TRANSACTION_ID NUMBER,
	STATUS VARCHAR(32000),
	LOCATION VARCHAR(480),
	RECEIVE_BY_DATE DATE,
	NS_RECEIVE_BY_DATE DATE,
	UNIQUE_KEY NUMBER,
	QUANTITY_TO_BE_RECEIVED NUMBER,
	HASH_VALUE VARCHAR(16777216),
	FSA_LOAD_STATUS VARCHAR(16777216),
	INSERT_DATE TIMESTAMP_LTZ(9),
	PK_ID VARCHAR(16777216)
)
AS $$
    select t1.* exclude (fsa_insert_date,is_valid)
      from DEV.${vj_fsa_schema}.open_po_all_historical t1
      join (
        select distinct insert_date,max(fsa_insert_date) over () fsa_insert_date
        from DEV.${vj_fsa_schema}.open_po_all_historical
        where insert_date = 
          (select max(insert_date) insert_date
            from DEV.${vj_fsa_schema}.open_po_all_historical
            where insert_date <= MAX_DATE)) t2
      on t1.insert_date = t2.insert_date
        and t1.fsa_insert_date = t2.fsa_insert_date
$$;

CREATE OR REPLACE FUNCTION DEV.${vj_fsa_schema}.LATEST_OPEN_PO_ALL ()
  RETURNS TABLE (
	PO_ROW_NO NUMBER(18,0),
	PO_ITEM_ID NUMBER,
	PO_ITEM_TYPE VARCHAR,
	ITEM_ID NUMBER,
	ITEM VARCHAR(4400),
	ITEM_ID_C NUMBER(38,0),
	ITEM_C VARCHAR(4400),
	ITEM_DISPLAY_NAME VARCHAR(2000),
	ASSEMBLY_ITEM_ID NUMBER,
	ASSEMBLY_ITEM VARCHAR(4400),
	ASSEMBLY_ITEM_DISPLAY_NAME VARCHAR(2000),
	ORDER_NUMBER VARCHAR(360),
	PURCHASE_ORDER_TRANSACTION_ID NUMBER,
	STATUS VARCHAR(32000),
	LOCATION VARCHAR(480),
	RECEIVE_BY_DATE DATE,
	NS_RECEIVE_BY_DATE DATE,
	UNIQUE_KEY NUMBER,
	QUANTITY_TO_BE_RECEIVED NUMBER,
	HASH_VALUE VARCHAR(16777216),
	FSA_LOAD_STATUS VARCHAR(16777216),
	INSERT_DATE TIMESTAMP_LTZ(9),
	PK_ID VARCHAR(16777216)
)
AS $$
	select * from TABLE(DEV.${vj_fsa_schema}.LATEST_OPEN_PO_ALL(current_timestamp()))
$$;

CREATE OR REPLACE FUNCTION DEV.${vj_fsa_schema}.LATEST_OPEN_PO_ALL (time_as_text text)
  RETURNS TABLE (
	PO_ROW_NO NUMBER(18,0),
	PO_ITEM_ID NUMBER,
	PO_ITEM_TYPE VARCHAR,
	ITEM_ID NUMBER,
	ITEM VARCHAR(4400),
	ITEM_ID_C NUMBER(38,0),
	ITEM_C VARCHAR(4400),
	ITEM_DISPLAY_NAME VARCHAR(2000),
	ASSEMBLY_ITEM_ID NUMBER,
	ASSEMBLY_ITEM VARCHAR(4400),
	ASSEMBLY_ITEM_DISPLAY_NAME VARCHAR(2000),
	ORDER_NUMBER VARCHAR(360),
	PURCHASE_ORDER_TRANSACTION_ID NUMBER,
	STATUS VARCHAR(32000),
	LOCATION VARCHAR(480),
	RECEIVE_BY_DATE DATE,
	NS_RECEIVE_BY_DATE DATE,
	UNIQUE_KEY NUMBER,
	QUANTITY_TO_BE_RECEIVED NUMBER,
	HASH_VALUE VARCHAR(16777216),
	FSA_LOAD_STATUS VARCHAR(16777216),
	INSERT_DATE TIMESTAMP_LTZ(9),
	PK_ID VARCHAR(16777216)
)
AS $$
	select * from TABLE(DEV.${vj_fsa_schema}.LATEST_OPEN_PO_ALL(try_to_timestamp_ltz(time_as_text)))
$$;


CREATE OR REPLACE PROCEDURE DEV.${vj_fsa_schema}.MAKE_OPEN_PO_ALL(MAX_DATE timestamp_ltz,TARGET_TABLE text)
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  BEGIN
  LET rs RESULTSET := (CREATE OR REPLACE TABLE IDENTIFIER(:TARGET_TABLE) AS SELECT * FROM TABLE(DEV.${vj_fsa_schema}.LATEST_OPEN_PO_ALL(:MAX_DATE)));
  return TABLE(rs);
  END;
$$;

CREATE OR REPLACE PROCEDURE DEV.${vj_fsa_schema}.MAKE_OPEN_PO_ALL(MAX_DATE timestamp_ltz)
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  BEGIN
  LET rs RESULTSET := (CALL DEV.${vj_fsa_schema}.MAKE_OPEN_PO_ALL(:MAX_DATE,'DEV.${vj_fsa_schema}.OPEN_PO_ALL'));
  return TABLE(rs);
  END;
$$;

CREATE OR REPLACE PROCEDURE DEV.${vj_fsa_schema}.MAKE_OPEN_PO_ALL()
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  BEGIN
  LET rs RESULTSET := (CALL DEV.${vj_fsa_schema}.MAKE_OPEN_PO_ALL(current_timestamp()));
  return TABLE(rs);
  END;
$$;

CREATE OR REPLACE FUNCTION DEV.${vj_fsa_schema}.DATES_OPEN_PO_ALL()
  RETURNS TABLE(
    RECENT_OR_INORDER NUMBER,
    INSERT_DATE TIMESTAMP_LTZ,
    FSA_INSERT_DATE TIMESTAMP_LTZ
  	) AS 
$$
  select * from
  (
    select row_number() over (order by insert_date,fsa_insert_date)-1 rep_order,
            insert_date,
            max(fsa_insert_date) over (partition by insert_date) fsa_insert_date
        from DEV.${vj_fsa_schema}.open_po_all_historical 
        where is_valid 
        group by insert_date,fsa_insert_date
    union
    select row_number() over (order by insert_date desc,fsa_insert_date)*-1 rep_order,
            insert_date,
            max(fsa_insert_date) over (partition by insert_date) fsa_insert_date
        from DEV.${vj_fsa_schema}.open_po_all_historical 
        where is_valid 
        group by insert_date,fsa_insert_date
  )
  order by rep_order
$$;

CREATE OR REPLACE FUNCTION DEV.${vj_fsa_schema}.LATEST_OPEN_PO_ALL (recent_or_inorder number)
  RETURNS TABLE (
	PO_ROW_NO NUMBER(18,0),
	PO_ITEM_ID NUMBER,
	PO_ITEM_TYPE VARCHAR,
	ITEM_ID NUMBER,
	ITEM VARCHAR(4400),
	ITEM_ID_C NUMBER(38,0),
	ITEM_C VARCHAR(4400),
	ITEM_DISPLAY_NAME VARCHAR(2000),
	ASSEMBLY_ITEM_ID NUMBER,
	ASSEMBLY_ITEM VARCHAR(4400),
	ASSEMBLY_ITEM_DISPLAY_NAME VARCHAR(2000),
	ORDER_NUMBER VARCHAR(360),
	PURCHASE_ORDER_TRANSACTION_ID NUMBER,
	STATUS VARCHAR(32000),
	LOCATION VARCHAR(480),
	RECEIVE_BY_DATE DATE,
	NS_RECEIVE_BY_DATE DATE,
	UNIQUE_KEY NUMBER,
	QUANTITY_TO_BE_RECEIVED NUMBER,
	HASH_VALUE VARCHAR(16777216),
	FSA_LOAD_STATUS VARCHAR(16777216),
	INSERT_DATE TIMESTAMP_LTZ(9),
	PK_ID VARCHAR(16777216)
)
AS $$
	select p.* exclude (fsa_insert_date,is_valid)
    from DEV.${vj_fsa_schema}.OPEN_PO_ALL_HISTORICAL p
    join table(DEV.${vj_fsa_schema}.DATES_OPEN_PO_ALL()) d
    	on p.insert_date = d.insert_date
           and p.fsa_insert_date = d.fsa_insert_date
    where p.is_valid
		and d.RECENT_OR_INORDER = recent_or_inorder
$$;
