CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}.OPEN_PO_ALL AS (
  WITH OPEN_PO AS (
    SELECT
      /* 20230804 - KBY, RSF23-1861 - Convert ID's from FLOAT to NUMBER */
        IFF(ASSEMBLY_ITEM_ID IS NOT NULL, ASSEMBLY_ITEM_ID, ITEM_ID)::NUMBER      AS PO_ITEM_ID
        , IFF(ASSEMBLY_ITEM_ID IS NOT NULL, 'ASSEMBLY', 'No Assembly')            AS PO_ITEM_TYPE
      /* 20230804 - KBY, RSF23-1861 - Convert ID's from FLOAT to NUMBER */
        , ITEM_ID::NUMBER                                                         AS ITEM_ID
        , ITEM   
        , ITEM_ID_C
        , ITEM_C
        , ITEM_DISPLAY_NAME  
        /* 20230804 - KBY, RSF23-1861 - Convert ID's from FLOAT to NUMBER */
        , ASSEMBLY_ITEM_ID::NUMBER                                                AS ASSEMBLY_ITEM_ID
        , ASSEMBLY_ITEM   
        , ASSEMBLY_ITEM_DISPLAY_NAME       
        , ORDER_NUMBER       
        /* 20230804 - KBY, RSF23-1861 - Convert ID's from FLOAT to NUMBER */
        , PURCHASE_ORDER_TRANSACTION_ID::NUMBER                                   AS PURCHASE_ORDER_TRANSACTION_ID
        , STATUS                                                                  AS STATUS
        , LOCATION                                                                AS LOCATION
        , IFF(RECEIVE_BY_DATE::DATE < CURRENT_DATE()
             , CAL.MIN_ADD_5
             , RECEIVE_BY_DATE)                                                   AS RECEIVE_BY_DATE
         , RECEIVE_BY_DATE                                                        AS NS_RECEIVE_BY_DATE
        /* 20230804 - KBY, RSF23-1861 - Convert ID's from FLOAT to NUMBER */
        , UNIQUE_KEY::NUMBER                                                      AS UNIQUE_KEY
        , FLOOR(QUANITITY_TO_BE_RECEIVED)::NUMBER                                 AS QUANTITY_TO_BE_RECEIVED
    FROM DEV.${vj_fsa_schema}.V_OPENPO 
    LEFT JOIN DEV.BUSINESS_OPERATIONS.DIM_FULFILLMENT_CALENDAR CAL
           ON CAL.RAW_DATE = CURRENT_DATE()
    WHERE YEAR(RECEIVE_BY_DATE) >= 2022
    AND ORDER_NUMBER NOT LIKE ('Planning%')
    AND (LOCATION IS NULL OR LOCATION IN 
          ('Barrett Distribution'
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
          ,'Wards VWR'
          ,'Booksource', 'Continuum' -- 2023.05.18 Alex: FSA
          )
        )
  )
  , XFER_SUPPLY AS (
    SELECT
          IFF(ASSEMBLY_ITEM_ID IS NOT NULL, ASSEMBLY_ITEM_ID, ITEM_ID)::NUMBER          AS PO_ITEM_ID
        , CASE TRANSFER_ORDER_TYPE
            WHEN 'Fulfillment' THEN 'Fulfillment Transfer Order'
            WHEN 'Assembly' THEN 'Assembly Transfer Order'
          END                                                                           AS PO_ITEM_TYPE
      , ITEM_ID::NUMBER                                                                 AS ITEM_ID
        , ITEM   
        , ITEM_ID_C
        , ITEM_C
        , ITEM_DISPLAY_NAME  
        , ASSEMBLY_ITEM_ID::NUMBER                                                      AS ASSEMBLY_ITEM_ID
        , ASSEMBLY_ITEM   
        , ASSEMBLY_ITEM_DISPLAY_NAME       
        , ORDER_NUMBER
        , TRANSACTION_ID::NUMBER                                                        AS PURCHASE_ORDER_TRANSACTION_ID
        , STATUS                                                                        AS STATUS
        , LOCATION_TO                                                                   AS LOCATION
        , IFF(REQUESTED_DDA::DATE < CURRENT_DATE()
             , CAL.MIN_ADD_5
             , REQUESTED_DDA)                                                           AS RECEIVE_BY_DATE
         , REQUESTED_DDA                                                                AS NS_RECEIVE_BY_DATE
        , UNIQUE_KEY::NUMBER                                                            AS UNIQUE_KEY
        , FLOOR(QUANTITY_COMMITTED-QUANTITY_FULFILLED)::NUMBER                        AS QUANTITY_TO_BE_RECEIVED
    FROM DEV.${vj_fsa_schema}.V_XFER_SUPPLY 
    LEFT JOIN DEV.BUSINESS_OPERATIONS.DIM_FULFILLMENT_CALENDAR CAL
           ON CAL.RAW_DATE = CURRENT_DATE()
    WHERE YEAR(RECEIVE_BY_DATE) >= 2022
    AND ORDER_NUMBER NOT LIKE ('Planning%')
    AND (LOCATION IS NULL OR LOCATION IN 
          ('Barrett Distribution'
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
          ,'Wards VWR'
          ,'Booksource', 'Continuum' -- 2023.05.18 Alex: FSA
          )
        )
  )
  , SUPPLY_UNION AS (
    SELECT * FROM OPEN_PO 
    UNION
    SELECT * FROM XFER_SUPPLY
  )
  , OPEN_PO_NO_HASH AS (
    SELECT 
      DENSE_RANK() OVER 
          (PARTITION BY ITEM_ID_C 
            ORDER BY ITEM_ID_C, RECEIVE_BY_DATE::DATE ASC, ORDER_NUMBER ASC
            )       AS PO_ROW_NO
      , supply.*
    FROM SUPPLY_UNION as supply
  ) 
  SELECT DISTINCT *
       , HASH(*)::TEXT         AS HASH_VALUE
       , NULL 				   AS FSA_LOAD_STATUS
       , CURRENT_TIMESTAMP()   AS INSERT_DATE
       , ORDER_NUMBER||'^'||UNIQUE_KEY::TEXT||'^'||ZEROIFNULL(ITEM_ID_C)::TEXT PK_ID
  FROM OPEN_PO_NO_HASH
);