EXECUTE IMMEDIATE
$$
  BEGIN
  LET cur_run_date DATE := CURRENT_DATE();
  SELECT MAX(INSERT_DATE) INTO :cur_run_date FROM DEV.${vj_fsa_schema}.DEMAND_PO;

/*
	AVAIL_DATE_1
*/

CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}.SEQUENCING_PO_ASSIGN_TMP1 AS
  SELECT  a.*
  		EXCLUDE (PO_QUANTITY_TO_BE_RECEIVED)
        , CASE 
       	  WHEN PO_INDICATOR = -1
          	THEN IFF(PO_RECEIVE_BY_DATE < :cur_run_date, today."MIN_ADD_5", PO_RECEIVE_BY_DATE)
          ELSE :cur_run_date  
        END AS "AVAIL_DATE"
  FROM (
      (SELECT * EXCLUDE OG_QUANTITY_TO_BE_RECEIVED
            /* 20230718 - KBY, RFS23-2047 - PO_INDICATOR_ASSIGN should be 1 if OpenSO is already assigned a location */
            , IFF((IS_ALREADY_ASSIGNED OR REMAINING_QTY_ON_HAND >= 0 OR PO_ORDER_NUMBER IS NOT NULL), 1, 0) AS "PO_INDICATOR_ASSIGN"
            /*  20230609 - KBY, HyperCare 113 - change partition from parent (ITEM_ID) to individual (ITEM_ID_BY_TRANSACTION_TYPE) within the order */
            , MAX(PO_QUANTITY_TO_BE_RECEIVED) OVER (PARTITION BY ITEM_ID_BY_TRANSACTION_TYPE, PO_ORDER_NUMBER) AS "PO_TOTAL_QUANTITY_TO_BE_RECEIVED"
       FROM DEV.${vj_fsa_schema}.ASSIGNED_DEMAND)

      UNION

      (SELECT *
            , NULL AS "PO_ORDER_NUMBER"
            , NULL AS "PO_QUANTITY_TO_BE_RECEIVED"
            , NULL AS "PO_QUANTITY_REMAINING"
            , NULL AS "PO_RECEIVE_BY_DATE"
            /* 20230718 - KBY, RFS23-2047 - PO_INDICATOR_ASSIGN should be 1 if OpenSO is already assigned a location */
            , IFF((IS_ALREADY_ASSIGNED OR REMAINING_QTY_ON_HAND >= 0 OR PO_ORDER_NUMBER IS NOT NULL), 1, 0) AS "PO_INDICATOR_ASSIGN"
            , NULL AS "PO_TOTAL_QUANTITY_TO_BE_RECEIVED"
      FROM DEV.${vj_fsa_schema}.UNASSIGNED_DEMAND
      WHERE (ORDER_NUMBER, NS_LINE_NUMBER, IFNULL(COMPONENT_ITEM_ID, 0)) NOT IN (
        					SELECT ORDER_NUMBER, NS_LINE_NUMBER, IFNULL(COMPONENT_ITEM_ID, 0) 
                            FROM DEV.${vj_fsa_schema}.ASSIGNED_DEMAND))
  ) a

  -- DDA    
  LEFT JOIN "DEV"."BUSINESS_OPERATIONS"."DIM_FULFILLMENT_CALENDAR" dda ON dda."RAW_DATE" = DDA::DATE
  -- DDA + 60 DAYS
  LEFT JOIN "DEV"."BUSINESS_OPERATIONS"."DIM_FULFILLMENT_CALENDAR" dda60 ON dda60."RAW_DATE" = DATEADD('day', 60, DDA::DATE)
  -- TODAY
  LEFT JOIN "DEV"."BUSINESS_OPERATIONS"."DIM_FULFILLMENT_CALENDAR" today ON today."RAW_DATE" = :cur_run_date
  -- TODAY + 60 DAYS
  LEFT JOIN "DEV"."BUSINESS_OPERATIONS"."DIM_FULFILLMENT_CALENDAR" today60 ON today60."RAW_DATE" = DATEADD('day', 60, :cur_run_date)
  ORDER BY ITEM_ID, ROW_NO, IFNULL(PO_ID, 0);

/*
	AVAIL_DATE_2
*/

CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}.SEQUENCING_PO_ASSIGN_TMP2 AS
  SELECT a.* 
         EXCLUDE AVAIL_DATE
       , CASE 
        	  /* AC 6/7 */
       		  WHEN PO_INDICATOR = '-1' AND PO_INDICATOR_ASSIGN = '1' AND SOURCE_TYPE IN ('OpenSO', 'XFER') 
       		   THEN IFF(PO_RECEIVE_BY_DATE < :cur_run_date, today."MIN_ADD_5", day_after_po.LAND_DATE)
       		  WHEN PO_INDICATOR_ASSIGN = '1'
               THEN IFF(a.AVAIL_DATE < :cur_run_date, :cur_run_date, AVAIL_DATE)
              WHEN PO_INDICATOR_ASSIGN = '0'
               THEN IFF(AVAIL_DATE < :cur_run_date OR a.AVAIL_DATE IS NULL, before_today60.LAND_DATE, AVAIL_DATE)
              ELSE NULL
              END AS "AVAIL_DATE"
       ,IFF(a.IS_ASSEMBLY_COMPONENT, a.ORDER_NUMBER, a.PO_ORDER_NUMBER) AS "SHARED_ORDER_NUMBER"
  FROM DEV.${vj_fsa_schema}.SEQUENCING_PO_ASSIGN_TMP1 a
  -- TODAY + 60 DAYS
  /* 202307126 - KBY, RFS23-2033 - Adjust FREDD calculation to account for shorter turnaround at 3PLs,  use global parameter FR_PREV_DAYS to back off business days */
  LEFT JOIN "DEV"."BUSINESS_OPERATIONS"."DIM_CALENDAR_BUSINESS_DAYS_SPAN" before_today60 
    ON before_today60.RAW_DATE = DATEADD('day', 60, :cur_run_date)
      AND before_today60.BIZDAYS = -a.FR_PREV_DAYS
  /* 20240314 - KBY, RFS23-4898 -Update AVAIL_DATE for any PO Supply or TO supply to one calendar day after po receive-by date */
  LEFT JOIN DEV.BUSINESS_OPERATIONS.DIM_CALENDAR_BUSINESS_DAYS_SPAN day_after_po 
    ON day_after_po.RAW_DATE = PO_RECEIVE_BY_DATE
      AND day_after_po.BIZDAYS = 1
/* AC 6/7 */
  -- TODAY
  LEFT JOIN "DEV"."BUSINESS_OPERATIONS"."DIM_FULFILLMENT_CALENDAR" today ON today."RAW_DATE" = :cur_run_date;
  
/*
	AVAIL_DATE_3
*/

CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}.SEQUENCING_PO_ASSIGN AS
  SELECT  a.PO_ID
         ,a.FSA_LOAD_STATUS
         ,a.ID
         ,a.ITEM_ID
         ,a.ORIGINAL_DDA
         ,a.SEQUENCING_DDA
         ,a.ORDER_NUMBER
         ,a.QTY_ON_HAND
         ,FLOOR(a.QTY_ORDERED,0) AS QTY_ORDERED
         ,a.REMAINING_QTY_ON_HAND
         ,a.TOTAL_QTY_SOLD
         ,a.DDA
         ,a.ITEM
         ,a.LOCATION
         ,a.NS_LINE_NUMBER
         ,a.ROW_NO
         ,a.PRIORITY
         ,a.SEQ
         ,a.TRANSACTION_TYPE
         ,a.TYPE_NAME
         ,a.TRANSACTION_ID
         ,a.LINE_ID
         ,a.UNIQUE_KEY
         ,a.PO_SLIPPAGE
         ,a.ITEM_ROW_NO
         ,a.SOURCE_TYPE
         ,a.COMPONENT_ITEM
         ,a.COMPONENT_ITEM_ID
         ,a.ITEM_ID_BY_TRANSACTION_TYPE
         ,a.SOURCE_LOAD_DATE
         ,a.PK_ID
         ,a.IS_ASSEMBLY_COMPONENT
         ,a.CREATE_DATE
         ,a.PO_INDICATOR
         ,a.PO_UPDATE_DATETIME
         ,a.PO_ORDER_NUMBER
         ,FLOOR(a.PO_QUANTITY_REMAINING,0) AS PO_QUANTITY_REMAINING
         ,a.PO_RECEIVE_BY_DATE
         ,a.PO_INDICATOR_ASSIGN
         ,FLOOR(a.PO_TOTAL_QUANTITY_TO_BE_RECEIVED,0) AS PO_TOTAL_QUANTITY_TO_BE_RECEIVED
         ,a.SHARED_ORDER_NUMBER
         /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment */
         ,a.FR_PREV_DAYS
         /* 20230920 - KBY, RFS23-2696 Include FSA_COMPLETE */
         ,FSA_COMPLETE
      	 ,CASE 
         
           /* 20230607 - AC - REVIEW AND DELETE ON 6/08 */
           /* 20230608 - KBY - Removed, to avoid AVAIL_DATE constantly incrementing day by day (after initial setting to "today")
          --WHEN a.PO_INDICATOR = '-1' AND a.PO_INDICATOR_ASSIGN = '1' AND a.SOURCE_TYPE IN ('OpenSO', 'XFER') AND a.TYPE_NAME = 'Assembly'
       		--THEN a.AVAIL_DATE
           /* ---- */ 
           
          /* 20230825 - KBY, RFS23-2625 - Compare potentially NULL values with IFNULL, not NULLIF */
           WHEN IFNULL(a.PO_ORDER_NUMBER,'0') != IFNULL(prev.PO_ORDER_NUMBER,'0') 
            THEN a.AVAIL_DATE
           WHEN IFNULL(a.PO_RECEIVE_BY_DATE,'2000-01-01'::DATE) != IFNULL(prev.PO_RECEIVE_BY_DATE,'2000-01-01'::DATE) 
            THEN a.AVAIL_DATE
           WHEN a.PO_INDICATOR != prev.PO_INDICATOR 
            THEN a.AVAIL_DATE
           WHEN a.PO_INDICATOR_ASSIGN != prev.PO_INDICATOR_ASSIGN 
            THEN a.AVAIL_DATE
           WHEN prev.PREV_AVAIL_DATE IS NULL 
            THEN a.AVAIL_DATE
           -- added 2023-05-30, KBY
           --   if PO is assigned and has quantity, and previous AVAIL date is in the future, then set AVAIL date to today (i.e. what is in a.AVAIL_DATE)
           WHEN a.PO_INDICATOR IN (0,1) AND a.PO_INDICATOR_ASSIGN = 1 AND prev.PREV_AVAIL_DATE > :cur_run_date
            THEN a.AVAIL_DATE
           /* 20230728 - KBY, RSF23-2033 - Check global parameter FR_PREV_DAYS to see if it has changed
              -- except in the case where the OpenSO has a valid location assigned */
           WHEN a.FR_PREV_DAYS != prev.FR_PREV_DAYS and not IS_ALREADY_ASSIGNED
            THEN a.AVAIL_DATE
           ELSE prev.PREV_AVAIL_DATE
          END AS "AVAIL_DATE"
  FROM DEV.${vj_fsa_schema}.SEQUENCING_PO_ASSIGN_TMP2 a
  LEFT JOIN DEV.${vj_fsa_schema}.DEMAND_PREV_ASSIGNED prev
      ON a.PK_ID = prev.PK_ID;
 END;
$$;