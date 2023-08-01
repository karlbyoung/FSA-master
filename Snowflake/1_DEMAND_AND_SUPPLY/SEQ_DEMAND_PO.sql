CREATE OR REPLACE TABLE DEV.${FSA_PROD_SCHEMA}.SEQ_DEMAND_PO AS
    WITH CTE_MAX AS (
      SELECT ZEROIFNULL(MAX(ROW_NO)) PREV_MAX FROM DEV.${FSA_PROD_SCHEMA}."DEMAND_PREV_ASSIGNED"
    )
    
    ,CTE_NEW AS (
      SELECT DPO.FSA_LOAD_STATUS
            ,DPO.COMPONENT_ITEM
            ,DPO.COMPONENT_ITEM_ID
            ,DPO.COMPONENT_QTY_ORDERED
            ,DPO.DDA
            ,DPO.ID
            ,DPO.ITEM
            ,DPO.ITEM_ID
            ,DPO.LOCATION
            ,DPO.NS_LINE_NUMBER
            ,DPO.ORDER_NUMBER
            ,DPO.ORIGINAL_DDA
            ,DPO.PO_SLIPPAGE
            ,DPO.QUANTITY 	                                                                                        AS "QTY_ORDERED"
            /* 20230608 - KBY, HyperCare 123 - Set IS_ALREADY_ASSIGNED when: 1) source type is OpenSO, 2) Location is valid (not blank and not 'Not Yet Assigned') */
            ,(DPO.SOURCE_TYPE = 'OpenSO' AND (DPO.LOCATION IS NOT NULL AND DPO.LOCATION != 'Not Yet Assigned'))     AS "IS_ALREADY_ASSIGNED"
            /* 20230712 - KBY, RFS23-1850 - Include quantities ordered for FSA forward-facing-only locations also */
            ,IFF(NOT DPO.IS_ASSEMBLY_COMPONENT AND NOT IS_ALREADY_ASSIGNED, DPO.QUANTITY, 0)                        AS "QTY_ORDERED_ACCOUNTED_FWD"
            ,IFF(DPO.IS_ASSEMBLY_COMPONENT, DPO.QUANTITY, 0)                                                        AS "QTY_ORDERED_ACCOUNTED_NONFWD"
            ,IFF(IS_ALREADY_ASSIGNED, 0, DPO.QUANTITY)                                                              AS "QTY_ORDERED_ACCOUNTED"
            ,DPO.INSERT_DATE 	                                                                                      AS "SOURCE_LOAD_DATE"
            ,DPO.SOURCE_TYPE
            ,DPO.TOTAL_AMT
            /* 20230712 - KBY, RFS23-1850 - Include inventories for FSA forward-facing-only locations also */
            ,ZEROIFNULL(DPO.TOTAL_AVAIL_QTY_FWD)                                            AS "QTY_ON_HAND_FWD"
            ,ZEROIFNULL(DPO.TOTAL_AVAIL_QTY_NONFWD)                                         AS "QTY_ON_HAND_NONFWD"
            ,ZEROIFNULL(DPO.TOTAL_AVAIL_QTY)                                                AS "QTY_ON_HAND_ALL"
            ,DPO.TRANSACTION_TYPE 	                                                        AS "TRANSACTION_TYPE"
            ,DPO.TYPE_NAME 			                                                            AS "TYPE_NAME"
            ,DPO.TRANSACTION_ID 	                                                          AS "TRANSACTION_ID"
            ,DPO.LINE_ID
            ,DPO.UNIQUE_KEY
            ,PRI.PRIORITY
            ,PRI.SEQ
            ,DPO.SEQUENCING_DDA
            ,IFF(DPO."IS_ASSEMBLY_COMPONENT", COMPONENT_ITEM_ID, ITEM_ID)                   AS "ITEM_ID_BY_TRANSACTION_TYPE"
            ,IFF(DPO."IS_ASSEMBLY_COMPONENT", COMPONENT_ITEM, ITEM)                         AS "ITEM_NAME_BY_TRANSACTION_TYPE"
            ,DENSE_RANK() OVER (ORDER BY PRI.PRIORITY,
                                        DPO.SEQUENCING_DDA,
                                        PRI.SEQ,
                                        DPO.ORDER_NUMBER,
                                        DPO.TOTAL_AMT::NUMBER DESC) + CTE_MAX.PREV_MAX      AS "ROW_NO"
            ,DPO.IS_ASSEMBLY_COMPONENT
            ,DPO.PK_ID
            ,DPO.CREATE_DATE
            ,DENSE_RANK() OVER (ORDER BY ITEM_ID_BY_TRANSACTION_TYPE)                       AS "ITEM_ROW_NO"
            /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment */
            ,DPO.FR_PREV_DAYS
      FROM DEV.${FSA_PROD_SCHEMA}."DEMAND_PO" DPO
      LEFT OUTER JOIN DEV.${FSA_PROD_SCHEMA}."DEMAND_PRIORITY" PRI
      ON DPO.FK_ID = PRI.ID
      JOIN CTE_MAX
      ON DPO.FK_ID = PRI.ID
      WHERE DPO.ROW_NO IS NULL
    )
    , CTE_PREV AS (
      SELECT DPO.FSA_LOAD_STATUS
            ,DPO.COMPONENT_ITEM
            ,DPO.COMPONENT_ITEM_ID
            ,DPO.COMPONENT_QTY_ORDERED
            ,DPO.DDA
            ,DPO.ID
            ,DPO.ITEM
            ,DPO.ITEM_ID
            ,DPO.LOCATION
            ,DPO.NS_LINE_NUMBER
            ,DPO.ORDER_NUMBER
            ,DPO.ORIGINAL_DDA
            ,DPO.PO_SLIPPAGE
            ,DPO.QUANTITY 	                                                                                        AS "QTY_ORDERED"
            /* 20230608 - KBY, HyperCare 123 - Set IS_ALREADY_ASSIGNED when: 1) source type is OpenSO, 2) Location is valid (not blank and not 'Not Yet Assigned') */
            ,(DPO.SOURCE_TYPE = 'OpenSO' AND (DPO.LOCATION IS NOT NULL AND DPO.LOCATION != 'Not Yet Assigned'))     AS "IS_ALREADY_ASSIGNED"
            /* 20230712 - KBY, RFS23-1850 - Include quantities ordered for FSA forward-facing-only locations also */
            ,IFF(NOT DPO.IS_ASSEMBLY_COMPONENT AND NOT IS_ALREADY_ASSIGNED, DPO.QUANTITY, 0)                        AS "QTY_ORDERED_ACCOUNTED_FWD"
            ,IFF(DPO.IS_ASSEMBLY_COMPONENT, DPO.QUANTITY, 0)                                                        AS "QTY_ORDERED_ACCOUNTED_NONFWD"
            ,IFF(IS_ALREADY_ASSIGNED, 0, DPO.QUANTITY)                                                              AS "QTY_ORDERED_ACCOUNTED"
            ,DPO.INSERT_DATE 	    AS "SOURCE_LOAD_DATE"
            ,DPO.SOURCE_TYPE
            ,DPO.TOTAL_AMT
            /* 20230712 - KBY, RFS23-1850 - Include inventories for FSA forward-facing-only locations also */
            ,ZEROIFNULL(DPO.TOTAL_AVAIL_QTY_FWD)                                            AS "QTY_ON_HAND_FWD"
            ,ZEROIFNULL(DPO.TOTAL_AVAIL_QTY_NONFWD)                                         AS "QTY_ON_HAND_NONFWD"
            ,ZEROIFNULL(DPO.TOTAL_AVAIL_QTY)                                                AS "QTY_ON_HAND_ALL"
            ,DPO.TRANSACTION_TYPE 	                                                        AS "TRANSACTION_TYPE"
            ,DPO.TYPE_NAME 			                                                            AS "TYPE_NAME"
            ,DPO.TRANSACTION_ID 	                                                          AS "TRANSACTION_ID"
            ,DPO.LINE_ID
            ,DPO.UNIQUE_KEY
            ,PRI.PRIORITY
            ,PRI.SEQ
            ,DPO.SEQUENCING_DDA
            ,IFF(DPO."IS_ASSEMBLY_COMPONENT", COMPONENT_ITEM_ID, ITEM_ID)                   AS "ITEM_ID_BY_TRANSACTION_TYPE"
            ,IFF(DPO."IS_ASSEMBLY_COMPONENT", COMPONENT_ITEM, ITEM)                         AS "ITEM_NAME_BY_TRANSACTION_TYPE"
            ,DPO.ROW_NO
            ,DPO.IS_ASSEMBLY_COMPONENT
            ,DPO.PK_ID
            ,DPO.CREATE_DATE
            ,DENSE_RANK() OVER (ORDER BY ITEM_ID_BY_TRANSACTION_TYPE)                     AS "ITEM_ROW_NO"
            /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment */
            ,DPO.FR_PREV_DAYS
      FROM DEV.${FSA_PROD_SCHEMA}."DEMAND_PO" DPO
      LEFT OUTER JOIN DEV.${FSA_PROD_SCHEMA}."DEMAND_PRIORITY" PRI
      ON DPO.FK_ID = PRI.ID
      WHERE DPO.ROW_NO IS NOT NULL 		-- FSA_LOAD_STATUS != 'NEW'
    )
    , CTE_ALL AS
    (
      SELECT * FROM CTE_PREV
      UNION ALL
      SELECT * FROM CTE_NEW
    )
    , CTE_FINAL AS
    (
      SELECT 
        FSA_LOAD_STATUS
        , ID
        , ITEM_ID
        , ORIGINAL_DDA
        , SEQUENCING_DDA
        , ORDER_NUMBER
        /* 20230712 - KBY, RFS23-1850 - Include inventories for FSA forward-facing-only locations also */
        , IFF(IS_ASSEMBLY_COMPONENT,QTY_ON_HAND_ALL,QTY_ON_HAND_FWD)            AS "QTY_ON_HAND" -- will become TOTAL_AVAIL_QTY
        , QTY_ON_HAND_FWD
        , QTY_ON_HAND_NONFWD
        , QTY_ORDERED
        /* 20230712 - KBY, RFS23-1850 - Calculate Remaining quantities depending on forward-facing and non-forward-facing, and type of order */
        , QTY_ON_HAND_ALL - (SUM(QTY_ORDERED_ACCOUNTED) OVER (
                    PARTITION BY ITEM_ID_BY_TRANSACTION_TYPE 
                    ORDER BY "ROW_NO", "NS_LINE_NUMBER"::NUMBER, "ID"
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))          AS REMAINING_QTY
        , QTY_ON_HAND_FWD - (SUM(QTY_ORDERED_ACCOUNTED_FWD) OVER (
                    PARTITION BY ITEM_ID_BY_TRANSACTION_TYPE 
                    ORDER BY "ROW_NO", "NS_LINE_NUMBER"::NUMBER, "ID"
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))          AS REMAINING_QTY_FWD
        , QTY_ON_HAND_NONFWD - (SUM(QTY_ORDERED_ACCOUNTED_NONFWD) OVER (
                    PARTITION BY ITEM_ID_BY_TRANSACTION_TYPE 
                    ORDER BY "ROW_NO", "NS_LINE_NUMBER"::NUMBER, "ID"
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))          AS REMAINING_QTY_NONFWD
        , CASE 
            WHEN (IS_ASSEMBLY_COMPONENT)
              THEN IFF(REMAINING_QTY_NONFWD >= 0 AND REMAINING_QTY_FWD < 0,
                        REMAINING_QTY_NONFWD,
                        REMAINING_QTY)
            ELSE IFF(REMAINING_QTY_FWD < REMAINING_QTY,
                        REMAINING_QTY_FWD,
                        REMAINING_QTY)
          END                                                                   AS "REMAINING_QTY_ON_HAND" -- will become REMAINING_TOTAL_QTY
        /* 20230608 - KBY, HyperCare 123 - Use the same SUM as for REMAINING_QTY_ON_HAND to calculate SUM_ROLLUP */
        , SUM(QTY_ORDERED_ACCOUNTED) OVER (
            PARTITION BY ITEM_ID_BY_TRANSACTION_TYPE /*, "ITEM_ROW_NO" */ -- 20230711 - KBY - Bug fix, remove ITEM_ROW_NO
            ORDER BY "ROW_NO", "NS_LINE_NUMBER"::NUMBER, "ID"       
            ROWS BETWEEN UNBOUNDED PRECEDING 
                AND CURRENT ROW)                                                AS "TOTAL_QTY_SOLD" -- will become SUM_ROLLUP
        /* 20230608 - KBY, HyperCare 123 - Carry IS_ALREADY_ASSIGNED through to next table process */
        , IS_ALREADY_ASSIGNED
        , DDA
        , ITEM
        , LOCATION
        , NS_LINE_NUMBER
        , ROW_NO
        , PRIORITY
        , SEQ
        , TRANSACTION_TYPE
        , TYPE_NAME
        , TRANSACTION_ID
        , LINE_ID
        , UNIQUE_KEY
        , PO_SLIPPAGE
        , ITEM_ROW_NO
        , SOURCE_TYPE
        , COMPONENT_ITEM
        , COMPONENT_ITEM_ID
        , ITEM_ID_BY_TRANSACTION_TYPE
        , SOURCE_LOAD_DATE
        , PK_ID
        , IS_ASSEMBLY_COMPONENT
        , CREATE_DATE
        /* 20230609 - KBY, HyperCare 113 - allow for using remaining quantity to partially source the sales order */
        , CASE 
            WHEN (REMAINING_QTY_ON_HAND + QTY_ORDERED_ACCOUNTED) <= 0 THEN 0  -- nothing left available
            ELSE CASE 
                WHEN REMAINING_QTY_ON_HAND >= 0                               -- if there's enough available
                  THEN QTY_ORDERED_ACCOUNTED                                  --   use all that was asked for
                  ELSE (REMAINING_QTY_ON_HAND + QTY_ORDERED_ACCOUNTED)        --   otherwise use up just what's available
                END
            END AS AVAIL_QTY_USED
        , (AVAIL_QTY_USED > 0 AND QTY_ORDERED_ACCOUNTED != AVAIL_QTY_USED) IS_PARTIAL_QTY -- flag to indicate we only partly could fill SO from what was available
        /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment */
        ,FR_PREV_DAYS
        FROM CTE_ALL
    )
    SELECT
      * EXCLUDE (QTY_ON_HAND_FWD,QTY_ON_HAND_NONFWD,REMAINING_QTY,REMAINING_QTY_FWD,REMAINING_QTY_NONFWD)
    FROM CTE_FINAL
    ORDER BY ITEM_ID_BY_TRANSACTION_TYPE, ITEM_ID, ROW_NO, ID;