CREATE OR REPLACE TABLE DEV.${FSA_CURRENT_SCHEMA}."OPEN_PO_ALL" AS (
  WITH OPEN_PO_NO_HASH AS (
    SELECT DENSE_RANK() OVER (PARTITION BY "ITEM_ID_C" 
                              ORDER BY "ITEM_ID_C", "RECEIVE_BY_DATE"::DATE ASC, "ORDER_NUMBER" ASC) AS "PO_ROW_NO"
    	 , IFF("ASSEMBLY_ITEM_ID" IS NOT NULL, "ASSEMBLY_ITEM_ID", "ITEM_ID")  AS "PO_ITEM_ID"
         , IFF("ASSEMBLY_ITEM_ID" IS NOT NULL, 'ASSEMBLY', 'No Assembly')      AS "PO_ITEM_TYPE"
    	 , "ITEM_ID"
         , "ITEM"   
    	 , "ITEM_ID_C"
    	 , "ITEM_C"
         , "ITEM_DISPLAY_NAME"  
         , "ASSEMBLY_ITEM_ID"
         , "ASSEMBLY_ITEM"   
         , "ASSEMBLY_ITEM_DISPLAY_NAME"       
         , "ORDER_NUMBER"       
         , "PURCHASE_ORDER_TRANSACTION_ID"     
         , "STATUS"   				  AS "STATUS"
         , "LOCATION" 				  AS "LOCATION"
         , IFF("RECEIVE_BY_DATE"::DATE < CURRENT_DATE()
             , CAL."MIN_ADD_5"
             , "RECEIVE_BY_DATE")     AS "RECEIVE_BY_DATE"
         , "RECEIVE_BY_DATE"          AS "NS_RECEIVE_BY_DATE"
         , "UNIQUE_KEY" 			  AS "UNIQUE_KEY"
         , "QUANITITY_TO_BE_RECEIVED" AS "QUANTITY_TO_BE_RECEIVED"
    FROM "DEV".${FSA_CURRENT_SCHEMA}."V_OPENPO" 
    LEFT JOIN "DEV"."BUSINESS_OPERATIONS"."DIM_FULFILLMENT_CALENDAR" CAL
           ON CAL.RAW_DATE = CURRENT_DATE()
    WHERE YEAR(RECEIVE_BY_DATE) >= 2022
    AND ORDER_NUMBER NOT LIKE ('Planning%')
    AND (LOCATION IS NULL OR LOCATION IN ('Barrett Distribution'
                                         ,'BR Printers'
                                         ,'BR Printers CN'
                                         ,'BR Printers KY'
                                         ,'BR Printers SJ'
                                         ,'hand2mind'
                                         ,'JPS Graphics'
                                         ,'LSC Airwest'
                                         ,'LSC Linn'
                                         ,'LSC Owensville'
                                         ,'Not Yet Assigned'
                                         ,'Wards VWR'))
  ) 
  SELECT DISTINCT *
       , HASH(*)::TEXT         AS "HASH_VALUE"
  	   , NULL 				   AS "FSA_LOAD_STATUS"
       , CURRENT_TIMESTAMP()   AS "INSERT_DATE"
  	   , "ORDER_NUMBER"||'^'||"UNIQUE_KEY"::TEXT||'^'||ZEROIFNULL("ITEM_ID_C")::TEXT PK_ID
  FROM "OPEN_PO_NO_HASH"
);