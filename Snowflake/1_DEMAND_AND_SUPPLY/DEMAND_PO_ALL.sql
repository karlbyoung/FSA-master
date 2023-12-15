CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}.DEMAND_PO_ALL AS 
(
  WITH DEMAND_PO_LOAD AS (
      SELECT *
        EXCLUDE TRANSACTION_TYPE,
         /* 20230912 - KBY, RFS23-2652 - updatedate TRANSACTION_TYPE for Sample orders */
        CASE
          WHEN TRANSACTION_TYPE = 'Sale' AND SALES_ORDER_TYPE = 104 THEN 'Reserve Order'
          WHEN TRANSACTION_TYPE = 'Sample (aka Internal fulfillment)' THEN 'Sample'
          ELSE TRANSACTION_TYPE
        END AS TRANSACTION_TYPE
      FROM DEV.${vj_fsa_schema}."V_DEMAND_PO"
      WHERE "ORDER_NUMBER" NOT IN ('Planning%')
      AND IFNULL("SO_MATERIAL_SUPPORT_STATUS", '') NOT IN (
           'M/Y: Complete - (23-24)'
          ,'M/Y: Confirmed No Material - (23-24)'
          ,'M/Y: MMAF Not Received - (23-24)'
          ,'M/Y: Hold - (23-24)'
          ,'M/Y: Processed in SF - (23-24)'
          ,'M/Y: Implementation Complete' 
          /* 20231004 - KBY, RSF23-2900 - Exclude "Hold Review OE" from DEMAND_PO */
          ,'Hold Review OE'
          /* 20231208 - KBY, RFS23-3779 - Exclude "OE Info Collection" from DEMAND_PO */
          ,'OE Info Collection'
       )
      AND IFF(SOURCETYPE = 'OpenSO', SALES_ORDER_TYPE IN ('2','104'), TRUE)
      AND ORDER_NUMBER NOT IN ('26817','25981','26501') -- PROD ONLY
      AND NOT (SOURCETYPE = 'Assembly' AND COMPONENT_ITEM IS NULL)
      AND TYPE_NAME <> 'Kit/Package'
      /* 20230804 - KBY, RSF23-1861 - Convert ID's from FLOAT to NUMBER */
      ORDER BY TRANSACTION_TYPE, DDA, UNIQUE_KEY::NUMBER
  )

  ,"CTE_DEMAND_PO" AS (
    SELECT  PRI.ID                   AS "FK_ID"
           ,DPO.ORDER_NUMBER         AS "ORDER_NUMBER" 
          /* 20230804 - KBY, RSF23-1861 - Convert ID's from FLOAT to NUMBER */
           ,UNIQUE_KEY::NUMBER       AS "UNIQUE_KEY"
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
          /* 20230804 - KBY, RSF23-1861 - Convert ID's from FLOAT to NUMBER */
           ,ITEM_ID::NUMBER          AS "ITEM_ID"                                  
           ,COMPONENT_ITEM_ID        AS "COMPONENT_ITEM_ID"             
           ,COMPONENT_ITEM           AS "COMPONENT_ITEM"          
           ,FLOOR(QTY_ORDERED)::NUMBER             AS "QTY_ORDERED"       
           ,FLOOR(TOTAL_AVAIL_QTY)::NUMBER         AS "TOTAL_AVAIL_QTY"           
          /* 20230711 - KBY, RFS23-1850 - Include inventories for FSA forward-facing-only locations also */
           ,FLOOR(TOTAL_AVAIL_QTY_FWD)::NUMBER     AS "TOTAL_AVAIL_QTY_FWD"
           ,FLOOR(TOTAL_AVAIL_QTY_NONFWD)::NUMBER  AS "TOTAL_AVAIL_QTY_NONFWD"
           ,FLOOR(COMPONENT_QTY_ORDERED)::NUMBER   AS "COMPONENT_QTY_ORDERED"                 
           ,LOCATION                 AS "LOCATION"    
           ,SOURCETYPE               AS "SOURCETYPE"      
         ,TRANSACTION_ID           AS "TRANSACTION_ID"          
         ,LINE_ID                  AS "LINE_ID"
         ,TRANSACTION_CREATE_DATE  AS "CREATE_DATE"
        /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment */
         ,FR_PREV_DAYS               AS "FR_PREV_DAYS"
        /* 20230920 - KBY, RFS23-2696 Include FSA_COMPLETE */
         ,FSA_COMPLETE               AS "FSA_COMPLETE"
    FROM DEMAND_PO_LOAD DPO
    INNER JOIN DEV.${vj_fsa_schema}."DEMAND_PRIORITY" PRI
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
            ,d.ORG_DDA                      AS "ORIGINAL_DDA"
            ,s.MIN_DDA             	        AS "SEQUENCING_DDA"
            ,d.TRANSACTION_ID               AS "TRANSACTION_ID" -- Internal ID
            ,d.LINE_ID                      AS "LINE_ID"
            ,d.TRANSACTION_TYPE             AS "TRANSACTION_TYPE"
            ,d.TYPE_NAME                    AS "TYPE_NAME"
            ,d.TOTAL_AMT                    AS "TOTAL_AMT"
            ,d.PRIORITY_LEVEL               AS "PRIORITY_LEVEL"
            ,d.NS_LINE_NUMBER               AS "NS_LINE_NUMBER"
            ,d.ITEM                         AS "ITEM"
            ,d.ITEM_ID                      AS "ITEM_ID"
            ,d.COMPONENT_ITEM_ID            AS "COMPONENT_ITEM_ID"
            ,NULLIF(d.COMPONENT_ITEM, '')   AS "COMPONENT_ITEM" --,IFF(d.COMPONENT_ITEM = '', NULL, d.COMPONENT_ITEM) AS "COMPONENT_ITEM"
            ,FLOOR(d.QTY_ORDERED)::NUMBER   AS "QUANTITY"
            ,d.TOTAL_AVAIL_QTY              AS "TOTAL_AVAIL_QTY"
          /* 20230711 - KBY, RFS23-1850 - Include inventories for FSA forward-facing-only locations also */
            ,d.TOTAL_AVAIL_QTY_FWD          AS "TOTAL_AVAIL_QTY_FWD"
            ,d.TOTAL_AVAIL_QTY_NONFWD       AS "TOTAL_AVAIL_QTY_NONFWD"
            ,d.COMPONENT_QTY_ORDERED        AS "COMPONENT_QTY_ORDERED"
            ,d.LOCATION                     AS "LOCATION"
            ,d.SOURCETYPE                   AS "SOURCE_TYPE"
            ,(UPPER(d."TRANSACTION_TYPE") = 'ASSEMBLY' AND UPPER(d."TYPE_NAME") = 'ASSEMBLY' AND UPPER("SOURCE_TYPE") = 'ASSEMBLY') AS "IS_ASSEMBLY_COMPONENT"
            ,d.CREATE_DATE
      FROM "CTE_DEMAND_PO" d
      LEFT JOIN "CTE_SEQ_DDA" s
             ON s.ORDER_NUMBER = d.ORDER_NUMBER
  )
, DEMAND_WITH_HASH AS
(
  SELECT *
      ,HASH(*)::TEXT            AS HASH_VALUE
  FROM DEMAND_NO_HASH
)
  SELECT h."FK_ID"                                                  AS "FK_ID"
      , h."ORDER_NUMBER"                                            AS "ORDER_NUMBER"
      , h."UNIQUE_KEY"                                              AS "UNIQUE_KEY"
      , h."DDA"                                                     AS "DDA"
      , h."DDA_MODIFIED"                                            AS "DDA_MODIFIED"
      , h."ORIGINAL_DDA"                                            AS "ORIGINAL_DDA"
      , h."SEQUENCING_DDA"                                          AS "SEQUENCING_DDA"
      , h."TRANSACTION_ID"                                          AS "TRANSACTION_ID"
      , h."LINE_ID"                                                 AS "LINE_ID"
      , h."TRANSACTION_TYPE"                                        AS "TRANSACTION_TYPE"
      , h."TYPE_NAME"                                               AS "TYPE_NAME"
      , h."TOTAL_AMT"                                               AS "TOTAL_AMT"
      , h."PRIORITY_LEVEL"                                          AS "PRIORITY_LEVEL"
      , h."NS_LINE_NUMBER"                                          AS "NS_LINE_NUMBER"
      , h."ITEM"                                                    AS "ITEM"
      , h."ITEM_ID"                                                 AS "ITEM_ID"
      , h."COMPONENT_ITEM_ID"                                       AS "COMPONENT_ITEM_ID"
      , h."COMPONENT_ITEM"                                          AS "COMPONENT_ITEM"
      , IFF(h."IS_ASSEMBLY_COMPONENT",
              h."COMPONENT_QTY_ORDERED",
              h."QUANTITY")                                         AS "QUANTITY"
      , h."TOTAL_AVAIL_QTY"                                         AS "TOTAL_AVAIL_QTY"
        /* 20230711 - KBY, RFS23-1850 - Include inventories for FSA forward-facing-only locations also */
      , h."TOTAL_AVAIL_QTY_FWD"                                     AS "TOTAL_AVAIL_QTY_FWD"
      , h."TOTAL_AVAIL_QTY_NONFWD"                                  AS "TOTAL_AVAIL_QTY_NONFWD"
      , h."QUANTITY"                                                AS "QTY_ORDERED"
      , h."COMPONENT_QTY_ORDERED"                                   AS "COMPONENT_QTY_ORDERED"
      , h."LOCATION"                                                AS "LOCATION"
      , h."SOURCE_TYPE"                                             AS "SOURCE_TYPE"
      , h."IS_ASSEMBLY_COMPONENT"                                   AS "IS_ASSEMBLY_COMPONENT"
      , h."CREATE_DATE"                                             AS "CREATE_DATE"
      , FALSE                                                       AS "PO_SLIPPAGE"
      , h."HASH_VALUE"                                              AS "HASH_VALUE"
    /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment, but not in HASH_VALUE */
      , d.FR_PREV_DAYS                                              AS "FR_PREV_DAYS"
      , NULL::TEXT                                                  AS "FSA_LOAD_STATUS"
      , CURRENT_TIMESTAMP()                                         AS "INSERT_DATE"
      , ROW_NUMBER() OVER (ORDER BY h.TRANSACTION_TYPE, h.DDA)      AS "ID"
      , h.UNIQUE_KEY||'^'||ZEROIFNULL(h.COMPONENT_ITEM_ID)::TEXT    AS "PK_ID"
        /* 20230920 - KBY, RFS23-2696 Include FSA_COMPLETE */
      , d.FSA_COMPLETE                                              AS "FSA_COMPLETE"
  FROM DEMAND_WITH_HASH h
    JOIN CTE_DEMAND_PO d
      ON h.UNIQUE_KEY = d.UNIQUE_KEY
        and ZEROIFNULL(h.COMPONENT_ITEM_ID) = ZEROIFNULL(d.COMPONENT_ITEM_ID)
);