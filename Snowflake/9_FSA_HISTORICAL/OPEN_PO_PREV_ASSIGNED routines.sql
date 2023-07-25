-- functions and procedures to support historical retrieval of OPEN_PO_PREV_ASSIGNED file
--
--  Function LATEST_OPEN_PO_PREV_ASSIGNED()
--		returns most recent version of OPEN_PO_PREV_ASSIGNED
--		ex: SELECT * FROM TABLE(LATEST_OPEN_PO_PREV_ASSIGNED())
--
--  Function LATEST_OPEN_PO_PREV_ASSIGNED(MAX_DATE)
--		returns the version of OPEN_PO_PREV_ASSIGNED table AT or BEFORE the given MAX_DATE
--		ex: SELECT * FROM TABLE(LATEST_OPEN_PO_PREV_ASSIGNED('2023-04-25 23:59'::timestamp_ltz))
--
--  Procedure MAKE_OPEN_PO_PREV_ASSIGNED()
--		Creates OPEN_PO_PREV_ASSIGNED table from the most recent version, named OPEN_PO_PREV_ASSIGNED
--		ex: CALL MAKE_OPEN_PO_PREV_ASSIGNED()
--
--  Procedure MAKE_OPEN_PO_PREV_ASSIGNED(MAX_DATE)
--		Creates OPEN_PO_PREV_ASSIGNED table from the version AT or BEFORE the given MAX_DATE, named OPEN_PO_PREV_ASSIGNED
--		ex: CALL MAKE_OPEN_PO_PREV_ASSIGNED('2023-04-25 23:59')
--		ex: CALL MAKE_OPEN_PO_PREV_ASSIGNED('2023-04-26') 		-- same as previous (within a minute anyway)
--
--  Procedure MAKE_OPEN_PO_PREV_ASSIGNED(MAX_DATE,TARGET_TABLE)
--		Creates FOPEN_PO_PREV_ASSIGNEDSA table from the version AT or BEFORE the given MAX_DATE, named TARGET_TABLE
--		ex: CALL MAKE_OPEN_PO_PREV_ASSIGNED('2023-04-25 23:59','TEST_OPEN_PO_PREV_ASSIGNED')
--

CREATE OR REPLACE FUNCTION DEV.${FSA_CURRENT_SCHEMA}.LATEST_OPEN_PO_PREV_ASSIGNED (MAX_DATE timestamp_ltz)
  RETURNS TABLE (
	PK_ID VARCHAR(16777216),
	HASH_VALUE NUMBER(38,0),
	INSERT_DATE TIMESTAMP_LTZ(9),
	LAST_MODIFIED TIMESTAMP_LTZ(9)
)
AS $$
    select t1.* exclude (used_in_process,fsa_insert_date)
    from DEV.${FSA_CURRENT_SCHEMA}.open_po_prev_assigned_historical t1
    join (
      select distinct
        pk_id,
        max(last_modified) over (partition by pk_id) max_mod_date,
        max(fsa_insert_date) over (partition by pk_id) max_fsa_date
          from DEV.${FSA_CURRENT_SCHEMA}.open_po_prev_assigned_historical
          where last_modified <= MAX_DATE
            and used_in_process) t2
    on t1.pk_id = t2.pk_id
      and t1.last_modified = t2.max_mod_date
      and t1.fsa_insert_date = t2.max_fsa_date
$$;

CREATE OR REPLACE FUNCTION DEV.${FSA_CURRENT_SCHEMA}.LATEST_OPEN_PO_PREV_ASSIGNED ()
  RETURNS TABLE (
	PK_ID VARCHAR(16777216),
	HASH_VALUE NUMBER(38,0),
	INSERT_DATE TIMESTAMP_LTZ(9),
	LAST_MODIFIED TIMESTAMP_LTZ(9)
)
AS $$
	select * from TABLE(DEV.${FSA_CURRENT_SCHEMA}.LATEST_OPEN_PO_PREV_ASSIGNED(current_timestamp()))
$$;

CREATE OR REPLACE PROCEDURE DEV.${FSA_CURRENT_SCHEMA}.MAKE_OPEN_PO_PREV_ASSIGNED(MAX_DATE timestamp_ltz,TARGET_TABLE text)
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  BEGIN
  LET rs RESULTSET := (CREATE OR REPLACE TABLE IDENTIFIER(:TARGET_TABLE) AS SELECT * FROM TABLE(DEV.${FSA_CURRENT_SCHEMA}.LATEST_OPEN_PO_PREV_ASSIGNED(:MAX_DATE)));
  return TABLE(rs);
  END;
$$;

CREATE OR REPLACE PROCEDURE DEV.${FSA_CURRENT_SCHEMA}.MAKE_OPEN_PO_PREV_ASSIGNED(MAX_DATE timestamp_ltz)
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  BEGIN
  LET rs RESULTSET := (CALL DEV.${FSA_CURRENT_SCHEMA}.MAKE_OPEN_PO_PREV_ASSIGNED(:MAX_DATE,'DEV.${FSA_CURRENT_SCHEMA}.OPEN_PO_PREV_ASSIGNED'));
  return TABLE(rs);
  END;
$$;

CREATE OR REPLACE PROCEDURE DEV.${FSA_CURRENT_SCHEMA}.MAKE_OPEN_PO_PREV_ASSIGNED()
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  BEGIN
  LET rs RESULTSET := (CALL DEV.${FSA_CURRENT_SCHEMA}.MAKE_OPEN_PO_PREV_ASSIGNED(current_timestamp()));
  return TABLE(rs);
  END;
$$;
