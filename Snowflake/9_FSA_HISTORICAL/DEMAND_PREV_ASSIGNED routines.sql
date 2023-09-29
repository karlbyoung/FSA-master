-- functions and procedures to support historical retrieval of DEMAND_PREV_ASSIGNED file
--
--  Function LATEST_DEMAND_PREV_ASSIGNED()
--		returns most recent version of DEMAND_PREV_ASSIGNED
--		ex: SELECT * FROM TABLE(LATEST_DEMAND_PREV_ASSIGNED())
--
--  Function LATEST_DEMAND_PREV_ASSIGNED(MAX_DATE)
--		returns the version of DEMAND_PREV_ASSIGNED table AT or BEFORE the given MAX_DATE
--		ex: SELECT * FROM TABLE(LATEST_DEMAND_PREV_ASSIGNED('2023-04-25 23:59'::timestamp_ltz))
--
--  Procedure MAKE_DEMAND_PREV_ASSIGNED()
--		Creates DEMAND_PREV_ASSIGNED table from the most recent version, named DEMAND_PREV_ASSIGNED
--		ex: CALL MAKE_DEMAND_PREV_ASSIGNED()
--
--  Procedure MAKE_DEMAND_PREV_ASSIGNED(MAX_DATE)
--		Creates DEMAND_PREV_ASSIGNED table from the version AT or BEFORE the given MAX_DATE, named DEMAND_PREV_ASSIGNED
--		ex: CALL MAKE_DEMAND_PREV_ASSIGNED('2023-04-25 23:59')
--		ex: CALL MAKE_DEMAND_PREV_ASSIGNED('2023-04-26') 		-- same as previous (within a minute anyway)
--
--  Procedure MAKE_DEMAND_PREV_ASSIGNED(MAX_DATE,TARGET_TABLE)
--		Creates FDEMAND_PREV_ASSIGNEDSA table from the version AT or BEFORE the given MAX_DATE, named TARGET_TABLE
--		ex: CALL MAKE_DEMAND_PREV_ASSIGNED('2023-04-25 23:59','TEST_DEMAND_PREV_ASSIGNED')
--

CREATE OR REPLACE FUNCTION DEV.${vj_fsa_schema}.LATEST_DEMAND_PREV_ASSIGNED (MAX_DATE timestamp_ltz)
  RETURNS TABLE (
  PK_ID VARCHAR(16777216),
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
  FR_PREV_DAYS NUMBER
)
AS $$
    select t1.* exclude (is_valid,fsa_insert_date)
    from DEV.${vj_fsa_schema}.demand_prev_assigned_historical t1
    join (
      select distinct
        pk_id,
        max(last_modified) over (partition by pk_id) max_mod_date,
        max(fsa_insert_date) over (partition by pk_id) max_fsa_date
          from DEV.${vj_fsa_schema}.demand_prev_assigned_historical
          where last_modified <= MAX_DATE
            and is_valid) t2
    on t1.pk_id = t2.pk_id
      and t1.last_modified = t2.max_mod_date
      and t1.fsa_insert_date = t2.max_fsa_date
$$;

CREATE OR REPLACE FUNCTION DEV.${vj_fsa_schema}.LATEST_DEMAND_PREV_ASSIGNED ()
  RETURNS TABLE (
  PK_ID VARCHAR(16777216),
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
  FR_PREV_DAYS NUMBER
)
AS $$
  select * from TABLE(DEV.${vj_fsa_schema}.LATEST_DEMAND_PREV_ASSIGNED(current_timestamp()))
$$;

CREATE OR REPLACE FUNCTION DEV.${vj_fsa_schema}.LATEST_DEMAND_PREV_ASSIGNED (time_as_text text)
  RETURNS TABLE (
  PK_ID VARCHAR(16777216),
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
  FR_PREV_DAYS NUMBER
)
AS $$
  select * from TABLE(DEV.${vj_fsa_schema}.LATEST_DEMAND_PREV_ASSIGNED(try_to_timestamp_ltz(time_as_text)))
$$;

CREATE OR REPLACE PROCEDURE DEV.${vj_fsa_schema}.MAKE_DEMAND_PREV_ASSIGNED(MAX_DATE timestamp_ltz,TARGET_TABLE text)
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  BEGIN
  LET rs RESULTSET := (CREATE OR REPLACE TABLE IDENTIFIER(:TARGET_TABLE) AS SELECT * FROM TABLE(DEV.${vj_fsa_schema}.LATEST_DEMAND_PREV_ASSIGNED(:MAX_DATE)));
  return TABLE(rs);
  END;
$$;

CREATE OR REPLACE PROCEDURE DEV.${vj_fsa_schema}.MAKE_DEMAND_PREV_ASSIGNED(MAX_DATE timestamp_ltz)
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  BEGIN
  LET rs RESULTSET := (CALL DEV.${vj_fsa_schema}.MAKE_DEMAND_PREV_ASSIGNED(:MAX_DATE,'DEV.${vj_fsa_schema}.DEMAND_PREV_ASSIGNED'));
  return TABLE(rs);
  END;
$$;

CREATE OR REPLACE PROCEDURE DEV.${vj_fsa_schema}.MAKE_DEMAND_PREV_ASSIGNED()
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  BEGIN
  LET rs RESULTSET := (CALL DEV.${vj_fsa_schema}.MAKE_DEMAND_PREV_ASSIGNED(current_timestamp()));
  return TABLE(rs);
  END;
$$;

CREATE OR REPLACE FUNCTION DEV.${vj_fsa_schema}.DATES_DEMAND_PREV_ASSIGNED()
  RETURNS TABLE(
    RECENT_OR_INORDER NUMBER,
    SOURCE_DATE TIMESTAMP_LTZ,
    FSA_INSERT_DATE TIMESTAMP_LTZ
    ) AS 
$$
  select * from
  (
    select row_number() over (order by source_date,fsa_insert_date)-1 rep_order,
          source_date,
          max(fsa_insert_date) over (partition by source_date) fsa_insert_date
        from (
          select max(last_modified) over (partition by fsa_insert_date) source_date,
            fsa_insert_date
          from DEV.${vj_fsa_schema}.demand_prev_assigned_historical 
          where is_valid
        ) 
        group by source_date,fsa_insert_date
    union
      select row_number() over (order by source_date desc,fsa_insert_date)*-1 rep_order,
            source_date,
            max(fsa_insert_date) over (partition by source_date) fsa_insert_date
          from (
            select max(last_modified) over (partition by fsa_insert_date) source_date,
              fsa_insert_date
            from DEV.${vj_fsa_schema}.demand_prev_assigned_historical 
            where is_valid
          ) 
    group by source_date,fsa_insert_date
  )
  order by rep_order
$$;

CREATE OR REPLACE FUNCTION DEV.${vj_fsa_schema}.LATEST_DEMAND_PREV_ASSIGNED (recent_or_inorder NUMBER)
  RETURNS TABLE (
  PK_ID VARCHAR(16777216),
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
  FR_PREV_DAYS NUMBER
)
AS $$
  select p.* exclude (fsa_insert_date,is_valid)
    from DEV.${vj_fsa_schema}.DEMAND_PREV_ASSIGNED_HISTORICAL p
    join table(DEV.${vj_fsa_schema}.DATES_DEMAND_PREV_ASSIGNED()) d
      on p.fsa_insert_date = d.fsa_insert_date
    where p.is_valid
    and d.RECENT_OR_INORDER = recent_or_inorder
$$;
