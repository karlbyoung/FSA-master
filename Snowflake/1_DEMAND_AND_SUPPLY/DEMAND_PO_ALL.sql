CREATE OR REPLACE TABLE DEV.${FSA_PROD_SCHEMA}.DEMAND_PO_ALL AS 
(
  WITH DEMAND_PO_LOAD AS (
      SELECT *
    		 EXCLUDE TRANSACTION_TYPE,
             IFF(TRANSACTION_TYPE = 'Sale' AND SALES_ORDER_TYPE = 104,'Reserve Order', TRANSACTION_TYPE) AS TRANSACTION_TYPE
      FROM DEV.${FSA_PROD_SCHEMA}."V_DEMAND_PO"
      WHERE "ORDER_NUMBER" NOT IN ('Planning%')
      AND IFNULL("SO_MATERIAL_SUPPORT_STATUS", '') NOT IN (
           'M/Y: Complete - (23-24)'
          ,'M/Y: Confirmed No Material - (23-24)'
          ,'M/Y: MMAF Not Received - (23-24)'
          ,'M/Y: Hold - (23-24)'
          ,'M/Y: Processed in SF - (23-24)'
          ,'M/Y: Implementation Complete' 
   	  )
      AND IFF(SOURCETYPE = 'OpenSO', SALES_ORDER_TYPE IN ('2','104'), TRUE)
      AND ORDER_NUMBER NOT IN ('26817','25981','26501') -- PROD ONLY
      AND NOT (SOURCETYPE = 'Assembly' AND COMPONENT_ITEM IS NULL)
      AND TYPE_NAME <> 'Kit/Package'
      ORDER BY TRANSACTION_TYPE, DDA, UNIQUE_KEY
  )

  ,"CTE_DEMAND_PO" AS (
    SELECT  PRI.ID                   AS "FK_ID"
           ,DPO.ORDER_NUMBER         AS "ORDER_NUMBER" 
           ,UNIQUE_KEY               AS "UNIQUE_KEY"
       	   ,IFF(SOURCETYPE = 'OpenSO', IFF(ESTIMATED_DELIVERY_DATE IS NULL, CAL.MIN_ADD_15, ESTIMATED_DELIVERY_DATE::DATE), DDA) AS "DDA"
           -- ,DDA                      AS "DDA"
           ,IFF(UPPER("TRANSACTION_TYPE") = 'ASSEMBLY',
                IFNULL(CAL."MIN_ADD_10", CAL."NEXT_BUSINESS_DAY"),
                DDA)                 AS "DDA_MODIFIED"
           ,ORG_DDA                  AS "ORG_DDA"           
           ,TRANSACTION_TYPE         AS "TRANSACTION_TYPE"   
    	   ,TYPE_NAME                AS "TYPE_NAME"       
           ,TOTAL_AMT                AS "TOTAL_AMT"       
           ,IFF(PRI.ID IS NULL, 4, PRIORITY_LEVEL) AS "PRIORITY_LEVEL"
           ,NS_LINE_NUMBER           AS "NS_LINE_NUMBER"                              
           ,ITEM                     AS "ITEM"                                          
           ,ITEM_ID                  AS "ITEM_ID"                                  
           ,COMPONENT_ITEM_ID        AS "COMPONENT_ITEM_ID"             
           ,COMPONENT_ITEM           AS "COMPONENT_ITEM"          
           ,QTY_ORDERED              AS "QTY_ORDERED"       
           ,TOTAL_AVAIL_QTY          AS "TOTAL_AVAIL_QTY"           
          /* 20230711 - KBY, RFS23-1850 - Include inventories for FSA forward-facing-only locations also */
           ,TOTAL_AVAIL_QTY_FWD      AS "TOTAL_AVAIL_QTY_FWD"
           ,TOTAL_AVAIL_QTY_NONFWD   AS "TOTAL_AVAIL_QTY_NONFWD"
           ,COMPONENT_QTY_ORDERED    AS "COMPONENT_QTY_ORDERED"                 
           ,LOCATION                 AS "LOCATION"    
           ,SOURCETYPE               AS "SOURCETYPE"      
    	   ,TRANSACTION_ID           AS "TRANSACTION_ID"          
    	   ,LINE_ID                  AS "LINE_ID"
    	   ,TRANSACTION_CREATE_DATE  AS "CREATE_DATE"
           -- ,CAL.*
    FROM DEMAND_PO_LOAD DPO
    INNER JOIN DEV.${FSA_PROD_SCHEMA}."DEMAND_PRIORITY" PRI
            ON DPO.TRANSACTION_TYPE = PRI.SEQ_DESC
           AND DPO.PRIORITY_LEVEL   = PRI.PRIORITY
    LEFT JOIN "DEV"."BUSINESS_OPERATIONS"."DIM_FULFILLMENT_CALENDAR" CAL
           ON CAL.RAW_DATE = CURRENT_DATE()
  )

  ,"CTE_SEQ_DDA" AS (
    SELECT ORDER_NUMBER      AS "ORDER_NUMBER"
          ,MIN(DDA_MODIFIED) AS "MIN_DDA"
    FROM CTE_DEMAND_PO
    GROUP BY ORDER_NUMBER
  )

  ,"DEMAND_NO_HASH" AS (
      SELECT d.FK_ID                        AS "FK_ID"
            ,d.ORDER_NUMBER                 AS "ORDER_NUMBER"
            ,d.UNIQUE_KEY                   AS "UNIQUE_KEY"
            ,d.DDA                          AS "DDA"
            ,d.DDA_MODIFIED                 AS "DDA_MODIFIED"
            ,d.ORG_DDA             			AS "ORIGINAL_DDA"
            ,s.MIN_DDA             			AS "SEQUENCING_DDA"
    		,d.TRANSACTION_ID 		        AS "TRANSACTION_ID" -- Internal ID
    		,d.LINE_ID                      AS "LINE_ID"
    		,d.TRANSACTION_TYPE             AS "TRANSACTION_TYPE"
    		,d.TYPE_NAME                    AS "TYPE_NAME"
            ,d.TOTAL_AMT                    AS "TOTAL_AMT"
            ,d.PRIORITY_LEVEL               AS "PRIORITY_LEVEL"
            ,d.NS_LINE_NUMBER               AS "NS_LINE_NUMBER"
            ,d.ITEM                         AS "ITEM"
            ,d.ITEM_ID                      AS "ITEM_ID"
            ,d.COMPONENT_ITEM_ID            AS "COMPONENT_ITEM_ID"
    		,NULLIF(d.COMPONENT_ITEM, '') 	AS "COMPONENT_ITEM" --,IFF(d.COMPONENT_ITEM = '', NULL, d.COMPONENT_ITEM) AS "COMPONENT_ITEM"
            ,d.QTY_ORDERED            		AS "QUANTITY"
            ,d.TOTAL_AVAIL_QTY              AS "TOTAL_AVAIL_QTY"
          /* 20230711 - KBY, RFS23-1850 - Include inventories for FSA forward-facing-only locations also */
            ,d.TOTAL_AVAIL_QTY_FWD          AS "TOTAL_AVAIL_QTY_FWD"
            ,d.TOTAL_AVAIL_QTY_NONFWD       AS "TOTAL_AVAIL_QTY_NONFWD"
            ,d.COMPONENT_QTY_ORDERED        AS "COMPONENT_QTY_ORDERED"
            ,d.LOCATION                     AS "LOCATION"
            ,d.SOURCETYPE             		AS "SOURCE_TYPE"
            ,(UPPER(d."TRANSACTION_TYPE") = 'ASSEMBLY' AND UPPER(d."TYPE_NAME") = 'ASSEMBLY' AND UPPER("SOURCE_TYPE") = 'ASSEMBLY') AS "IS_ASSEMBLY_COMPONENT"
    		,d.CREATE_DATE
      FROM "CTE_DEMAND_PO" d
      LEFT JOIN "CTE_SEQ_DDA" s
             ON s.ORDER_NUMBER = d.ORDER_NUMBER
  )

  SELECT "FK_ID"                                                  AS "FK_ID"
       , "ORDER_NUMBER"                                           AS "ORDER_NUMBER"
       , "UNIQUE_KEY"                                             AS "UNIQUE_KEY"
       , "DDA"                                                    AS "DDA"
       , "DDA_MODIFIED"                                           AS "DDA_MODIFIED"
       , "ORIGINAL_DDA"                                           AS "ORIGINAL_DDA"
       , "SEQUENCING_DDA"                                         AS "SEQUENCING_DDA"
       , "TRANSACTION_ID"                                         AS "TRANSACTION_ID"
       , "LINE_ID"                                                AS "LINE_ID"
       , "TRANSACTION_TYPE"                                       AS "TRANSACTION_TYPE"
       , "TYPE_NAME"                                              AS "TYPE_NAME"
       , "TOTAL_AMT"                                              AS "TOTAL_AMT"
       , "PRIORITY_LEVEL"                                         AS "PRIORITY_LEVEL"
       , "NS_LINE_NUMBER"                                         AS "NS_LINE_NUMBER"
       , "ITEM"                                                   AS "ITEM"
       , "ITEM_ID"                                                AS "ITEM_ID"
       , "COMPONENT_ITEM_ID"                                      AS "COMPONENT_ITEM_ID"
       , "COMPONENT_ITEM"                                         AS "COMPONENT_ITEM"
       , IFF("IS_ASSEMBLY_COMPONENT","COMPONENT_QTY_ORDERED","QUANTITY") AS "QUANTITY"
       , "TOTAL_AVAIL_QTY"                                        AS "TOTAL_AVAIL_QTY"
          /* 20230711 - KBY, RFS23-1850 - Include inventories for FSA forward-facing-only locations also */
       , "TOTAL_AVAIL_QTY_FWD"                                    AS "TOTAL_AVAIL_QTY_FWD"
       , "TOTAL_AVAIL_QTY_NONFWD"                                 AS "TOTAL_AVAIL_QTY_NONFWD"
       , "QUANTITY"                                               AS "QTY_ORDERED"
       , "COMPONENT_QTY_ORDERED"                                  AS "COMPONENT_QTY_ORDERED"
       , "LOCATION"                                               AS "LOCATION"
       , "SOURCE_TYPE"                                            AS "SOURCE_TYPE"
       , "IS_ASSEMBLY_COMPONENT"                                  AS "IS_ASSEMBLY_COMPONENT"
       , "CREATE_DATE"                                            AS "CREATE_DATE"
       , FALSE                                                    AS "PO_SLIPPAGE"
       , HASH(*)::TEXT                                            AS "HASH_VALUE"
  	   , NULL 				                                      AS "FSA_LOAD_STATUS"
       , CURRENT_TIMESTAMP()                                      AS "INSERT_DATE"
  	   , ROW_NUMBER() OVER (ORDER BY TRANSACTION_TYPE, DDA)       AS "ID"
  	   , "UNIQUE_KEY"||'^'||ZEROIFNULL("COMPONENT_ITEM_ID")::TEXT AS "PK_ID"
  FROM "DEMAND_NO_HASH"
);