-- functions and procedures to support historical retrieval of DEMAND_PO_ALL file
--
--  Function LATEST_DEMAND_PO_ALL()
--		returns most recent version of DEMAND_PO_ALL
--		ex: SELECT * FROM TABLE(LATEST_DEMAND_PO_ALL())
--
--  Function LATEST_DEMAND_PO_ALL(MAX_DATE)
--		returns the version of DEMAND_PO_ALL table AT OR BEFORE the given MAX_DATE
--		ex: SELECT * FROM TABLE(LATEST_DEMAND_PO_ALL('2023-04-25 23:59'::timestamp_ltz))
--
--  Procedure MAKE_DEMAND_PO_ALL()
--		Creates DEMAND_PO_ALL table from the most recent version, named DEMAND_PO_ALL
--		ex: CALL MAKE_DEMAND_PO_ALL()
--
--  Procedure MAKE_DEMAND_PO_ALL(MAX_DATE)
--		Creates DEMAND_PO_ALL table from the version AT OR BEFORE the given MAX_DATE, named DEMAND_PO_ALL
--		ex: CALL MAKE_DEMAND_PO_ALL('2023-04-25 23:59')
--		ex: CALL MAKE_DEMAND_PO_ALL('2023-04-26') 		-- same as previous (within a minute anyway)
--
--  Procedure MAKE_DEMAND_PO_ALL(MAX_DATE,TARGET_TABLE)
--		Creates FDEMAND_PO_ALLSA table from the version AT OR BEFORE the given MAX_DATE, named TARGET_TABLE
--		ex: CALL MAKE_DEMAND_PO_ALL('2023-04-25 23:59','TEST_DEMAND_PO_ALL')
--

CREATE OR REPLACE FUNCTION DEV.${FSA_CURRENT_SCHEMA}.LATEST_DEMAND_PO_ALL (MAX_DATE timestamp_ltz)
  RETURNS TABLE (
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
	TOTAL_AVAIL_QTY FLOAT,
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
	PK_ID VARCHAR(16777216))
AS $$
    select t1.* exclude fsa_insert_date 
      from DEV.${FSA_CURRENT_SCHEMA}.demand_po_all_historical t1
      join (
        select distinct insert_date,max(fsa_insert_date) over () fsa_insert_date
        from DEV.${FSA_CURRENT_SCHEMA}.demand_po_all_historical
        where insert_date = 
          (select max(insert_date) insert_date
            from DEV.${FSA_CURRENT_SCHEMA}.demand_po_all_historical
            where insert_date <= MAX_DATE)) t2
      on t1.insert_date = t2.insert_date
        and t1.fsa_insert_date = t2.fsa_insert_date
$$;

CREATE OR REPLACE FUNCTION DEV.${FSA_CURRENT_SCHEMA}.LATEST_DEMAND_PO_ALL ()
  RETURNS TABLE (
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
	TOTAL_AVAIL_QTY FLOAT,
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
	PK_ID VARCHAR(16777216))
AS $$
	select * from TABLE(DEV.${FSA_CURRENT_SCHEMA}.LATEST_DEMAND_PO_ALL(current_timestamp()))
$$;

CREATE OR REPLACE PROCEDURE DEV.${FSA_CURRENT_SCHEMA}.MAKE_DEMAND_PO_ALL(MAX_DATE timestamp_ltz,TARGET_TABLE text)
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  BEGIN
  LET rs RESULTSET := (CREATE OR REPLACE TABLE IDENTIFIER(:TARGET_TABLE) AS SELECT * FROM TABLE(DEV.${FSA_CURRENT_SCHEMA}.LATEST_DEMAND_PO_ALL(:MAX_DATE)));
  return TABLE(rs);
  END;
$$;

CREATE OR REPLACE PROCEDURE DEV.${FSA_CURRENT_SCHEMA}.MAKE_DEMAND_PO_ALL(MAX_DATE timestamp_ltz)
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  BEGIN
  LET rs RESULTSET := (CALL DEV.${FSA_CURRENT_SCHEMA}.MAKE_DEMAND_PO_ALL(:MAX_DATE,'DEV.${FSA_CURRENT_SCHEMA}.DEMAND_PO_ALL'));
  return TABLE(rs);
  END;
$$;

CREATE OR REPLACE PROCEDURE DEV.${FSA_CURRENT_SCHEMA}.MAKE_DEMAND_PO_ALL()
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  BEGIN
  LET rs RESULTSET := (CALL DEV.${FSA_CURRENT_SCHEMA}.MAKE_DEMAND_PO_ALL(current_timestamp()));
  return TABLE(rs);
  END;
$$;