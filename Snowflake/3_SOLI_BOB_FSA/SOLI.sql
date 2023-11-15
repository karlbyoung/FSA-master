CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}."SOLI" AS

    WITH "CTE_MAX_AVAIL" AS (
        SELECT "ITEM_ID"
              ,"SHARED_ORDER_NUMBER"
              ,MAX(AVAIL_DATE) AS "MAX_AVAIL_DATE"
        FROM DEV.${vj_fsa_schema}."SEQUENCING_PO_ASSIGN" 
        GROUP BY 1,2   
    )
    
    ,"SOLI_CTE" AS (
        SELECT seq.* RENAME "AVAIL_DATE" AS "ITEM_AVAIL_DATE"
              /* 20230607 - AC - Hypercare Ref #122 - Temp removal until data/logic resolved */
            --,IFF(seq."SHARED_ORDER_NUMBER" IS NULL, "AVAIL_DATE", mad."MAX_AVAIL_DATE") AS "AVAIL_DATE"
            ,"AVAIL_DATE"
        FROM DEV.${vj_fsa_schema}."SEQUENCING_PO_ASSIGN" seq
        LEFT JOIN "CTE_MAX_AVAIL" mad
          ON seq."ITEM_ID"             = mad."ITEM_ID"
         AND seq."SHARED_ORDER_NUMBER" = mad."SHARED_ORDER_NUMBER" 
        ORDER BY "ORDER_NUMBER", "ITEM", "UNIQUE_KEY"
    )
    
    SELECT soli.ORDER_NUMBER 		AS "ORDER_NUMBER",
           soli.UNIQUE_KEY 			AS "UNIQUE_KEY", 
           soli.LINE_ID				AS "LINE_ID",
           soli.NS_LINE_NUMBER 		AS "NS_LINE_NUMBER", 
           soli.ITEM 				AS "ITEM",
           soli.ITEM_ID				AS "ITEM_ID",
           soli.ITEM_AVAIL_DATE     AS "ITEM_AVAIL_DATE",   -- line avail_date
           soli.AVAIL_DATE 		    AS "AVAIL_DATE", 		-- agg avail_date
          /* 202307126 - KBY, RFS23-2033 - Adjust FREDD calculation to account for shorter turnaround at 3PLs */
           cal.LAND_DATE		AS "FREDD", 
           soli.ID 					AS "FK_SPA_ID", 
           soli.SOURCE_LOAD_DATE 	AS "SOURCE_LOAD_DATE",
          /* 20230605 - KBY, Hypercare Ref #117 - include SOURCE_TYPE */
           soli.SOURCE_TYPE,
          /* 20231109 - KBY, RFS23-3534 - include FR_PREV_DAYS for calculating FR_RELEASE_DATE */
           soli.FR_PREV_DAYS  
    FROM "SOLI_CTE" soli
    /* 202307126 - KBY, RFS23-2033 - Adjust FREDD calculation to account for shorter turnaround at 3PLs,  use variable span over business days */
    LEFT JOIN "DEV"."BUSINESS_OPERATIONS"."DIM_CALENDAR_BUSINESS_DAYS_SPAN" cal
      ON cal."RAW_DATE" = soli."AVAIL_DATE"
        AND cal."BIZDAYS" = soli."FR_PREV_DAYS"

    ORDER BY "ORDER_NUMBER", "ITEM", "UNIQUE_KEY";