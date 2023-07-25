CREATE OR REPLACE TABLE DEV.BUSINESS_OPERATIONS.FSA AS
    WITH "CTE_MAX_AVAIL" AS (
        SELECT ORDER_NUMBER,
               ITEM_ID,
               MAX(AVAILABILITY_DATE) AS "MAX_AVAIL_DATE"
        FROM DEV.BUSINESS_OPERATIONS.SEQUENCING_PO_ASSIGN
        GROUP BY 1,2
    )
    
    , "CTE_FSA" AS (
      SELECT SPA.*
             RENAME (
               QTY_ON_HAND             AS "TOTAL_AVAIL_QTY",
               REMAINING_QTY_ON_HAND   AS "REMAINING_AVAIL_QTY",
               TOTAL_QTY_SOLD          AS "SUM_ROLLUP",
               QTY_ORDERED             AS "QUANTITY",
               AVAILABILITY_DATE	     AS "AVAIL_DATE"
             )
            ,bob.ORDER_NUMBER AS BOB_ORDER_NUMBER
            ,bob.ITEM_NO      AS BOB_ITEM
            ,bob.FREDD
            ,bob.BUCKET_ON_RECEIPT AS BUCKET_ON_RECEIVE_BY_DATE
            ,bob.BUCKET_DATE_ON_RECEIPT AS BUCKET_DATE_ON_RECEIVE_BY_DATE
            ,bob.CAPPING_DDA
            ,bob.IS_GT_15_BIZDAYS
            ,bob.IF_BUCKET1
            ,bob.IF_BUCKET2
            ,bob.IF_BUCKET3
            ,bob.IF_BUCKET4
            ,bob.IF_BUCKET5
            ,bob.IF_BUCKET6
            ,bob.IF_BUCKET7
            ,bob.IF_BUCKET8
            ,bob.IF_BUCKET9
            ,bob.IF_BUCKET10
            ,bob.IF_BUCKET11
            ,bob.IF_BUCKET12
            ,bob.IF_BUCKET13
            ,bob.IF_BUCKET14
            ,IFF(ORIGINAL_DDA IS NULL, DDA, ORIGINAL_DDA) AS FSA_UPDATED_ORIGINAL_DDA
      FROM DEV.BUSINESS_OPERATIONS.SEQUENCING_PO_ASSIGN SPA
      INNER JOIN DEV.BUSINESS_OPERATIONS.BOB bob
      ON SPA.ID = bob.FK_SPA_ID
      ORDER BY ITEM_ID, IFNULL(PO_ID, 0)
    )
    
    SELECT fsa.*
           EXCLUDE PO_RECEIVE_BY_DATE
           RENAME "AVAIL_DATE" AS "ITEM_AVAIL_DATE"
          ,IFNULL(mad."MAX_AVAIL_DATE", fsa."AVAIL_DATE") AS "AVAIL_DATE"
          ,IFF("TRANSACTION_TYPE" = 'Assembly', mad."MAX_AVAIL_DATE", fsa."PO_RECEIVE_BY_DATE") AS "PO_RECEIVE_BY_DATE"
    FROM CTE_FSA fsa
    LEFT JOIN CTE_MAX_AVAIL mad
    ON fsa.ORDER_NUMBER = mad.ORDER_NUMBER
    AND fsa.ITEM_ID = mad.ITEM_ID
;