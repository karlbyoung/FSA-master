CREATE OR REPLACE TABLE DEV.${FSA_PROD_SCHEMA}."SOLI" AS

    WITH "CTE_MAX_AVAIL" AS (
        SELECT "ITEM_ID"
              ,"SHARED_ORDER_NUMBER"
              ,MAX(AVAIL_DATE) AS "MAX_AVAIL_DATE"
        FROM DEV.${FSA_PROD_SCHEMA}."SEQUENCING_PO_ASSIGN" 
        GROUP BY 1,2   
    )
    
    ,"SOLI_CTE" AS (
        SELECT seq.* RENAME "AVAIL_DATE" AS "ITEM_AVAIL_DATE"
              ,IFF(seq."SHARED_ORDER_NUMBER" IS NULL, "AVAIL_DATE", mad."MAX_AVAIL_DATE") AS "AVAIL_DATE"
        FROM DEV.${FSA_PROD_SCHEMA}."SEQUENCING_PO_ASSIGN" seq
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
           cal.MIN_ADD_15::DATE		AS "FREDD", 
           soli.ID 					AS "FK_SPA_ID", 
           soli.SOURCE_LOAD_DATE 	AS "SOURCE_LOAD_DATE"
           
         
           
    FROM "SOLI_CTE" soli
    LEFT JOIN "DEV"."BUSINESS_OPERATIONS"."DIM_FULFILLMENT_CALENDAR" cal
      ON cal."RAW_DATE" = soli."AVAIL_DATE"

    ORDER BY "ORDER_NUMBER", "ITEM", "UNIQUE_KEY";