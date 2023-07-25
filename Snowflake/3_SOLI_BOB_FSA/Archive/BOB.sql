CREATE OR REPLACE TABLE DEV."BUSINESS_OPERATIONS"."BOB" AS
    WITH CTE_SOLI AS (
      SELECT soli.*
            ,MAX_RECEIVEDATE > c.MIN_ADD_15 AS IS_GT_15_BIZDAYS
            ,c.MIN_ADD_7   ,c.MIN_ADD_14  ,c.MIN_ADD_15
            ,c.MIN_ADD_21  ,c.MIN_ADD_28  ,c.MIN_ADD_30
            ,c.MIN_ADD_35  ,c.MIN_ADD_42  ,c.MIN_ADD_45
            ,c.MIN_ADD_49  ,c.MIN_ADD_56  ,c.MIN_ADD_60
            ,c.MIN_ADD_63  ,c.MIN_ADD_70  ,c.MIN_ADD_75
            ,c.MIN_ADD_77  ,c.MIN_ADD_84  ,c.MIN_ADD_90
            ,c.MIN_ADD_91  ,c.MIN_ADD_105 ,c.MIN_ADD_120
            ,c.MIN_ADD_135 ,c.MIN_ADD_150 ,c.MIN_ADD_165
            ,c.MIN_ADD_180 ,c.MIN_ADD_195
      FROM (SELECT ORDER_NUMBER
                   ,MIN(RECEIPT_DATE)::DATE AS MIN_RECEIVEDATE
                   ,MAX(RECEIPT_DATE)::DATE AS MAX_RECEIVEDATE
                   ,DATEDIFF('day', MIN(RECEIPT_DATE)::DATE, MAX(RECEIPT_DATE)::DATE) AS DAY_RANGE_BY_ORDER_NO
            FROM DEV.BUSINESS_OPERATIONS.SOLI soli
            GROUP BY ORDER_NUMBER) soli
      LEFT JOIN DEV.BUSINESS_OPERATIONS.DIM_FULFILLMENT_CALENDAR AS c
             ON soli.MIN_RECEIVEDATE = c.RAW_DATE
    )
    
    ,CTE_SOLI_BIZDAY AS (
        SELECT soli.ORDER_NUMBER, COUNT(c2.RAW_DATE) AS BIZDAY_RANGE_BY_ORDER_NO
        FROM CTE_SOLI soli
        LEFT JOIN DEV.BUSINESS_OPERATIONS.DIM_FULFILLMENT_CALENDAR c2
               ON c2.RAW_DATE BETWEEN soli.MIN_RECEIVEDATE AND soli.MAX_RECEIVEDATE
        WHERE c2.IS_BUSINESS_DAY
        GROUP BY soli.ORDER_NUMBER
    )
    
    ,CTE_SETIFDATE AS (
        SELECT soli.ORDER_NUMBER
               ,soli.MIN_RECEIVEDATE
               ,soli.MAX_RECEIVEDATE
               ,soli.DAY_RANGE_BY_ORDER_NO
               ,biz.BIZDAY_RANGE_BY_ORDER_NO
               ,soli.IS_GT_15_BIZDAYS
               ,soli.MIN_RECEIVEDATE AS IF_BUCKET1
               ,CASE WHEN DAY_RANGE_BY_ORDER_NO > 14
                     THEN MIN_ADD_15
                     ELSE MIN_ADD_7
                     END AS IF_BUCKET2
               ,CASE WHEN DAY_RANGE_BY_ORDER_NO > 14
                     THEN MIN_ADD_30
                     ELSE MIN_ADD_14
                     END AS IF_BUCKET3
               ,CASE WHEN DAY_RANGE_BY_ORDER_NO > 14
                     THEN MIN_ADD_45
                     ELSE MIN_ADD_21
                     END AS IF_BUCKET4
               ,CASE WHEN DAY_RANGE_BY_ORDER_NO > 14
                     THEN MIN_ADD_60
                     ELSE MIN_ADD_28
                     END AS IF_BUCKET5
               ,CASE WHEN DAY_RANGE_BY_ORDER_NO > 14
                     THEN MIN_ADD_75
                     ELSE MIN_ADD_35
                     END AS IF_BUCKET6
               ,CASE WHEN DAY_RANGE_BY_ORDER_NO > 14
                     THEN MIN_ADD_90
                     ELSE MIN_ADD_42
                     END AS IF_BUCKET7
               ,CASE WHEN DAY_RANGE_BY_ORDER_NO > 14
                     THEN MIN_ADD_105
                     ELSE MIN_ADD_49
                     END AS IF_BUCKET8
               ,CASE WHEN DAY_RANGE_BY_ORDER_NO > 14
                     THEN MIN_ADD_120
                     ELSE MIN_ADD_56
                     END AS IF_BUCKET9
               ,CASE WHEN DAY_RANGE_BY_ORDER_NO > 14
                     THEN MIN_ADD_135
                     ELSE MIN_ADD_63
                     END AS IF_BUCKET10
               ,CASE WHEN DAY_RANGE_BY_ORDER_NO > 14
                     THEN MIN_ADD_150
                     ELSE MIN_ADD_70
                     END AS IF_BUCKET11
               ,CASE WHEN DAY_RANGE_BY_ORDER_NO > 14
                     THEN MIN_ADD_165
                     ELSE MIN_ADD_77
                     END AS IF_BUCKET12
               ,CASE WHEN DAY_RANGE_BY_ORDER_NO > 14
                     THEN MIN_ADD_180
                     ELSE MIN_ADD_84
                     END AS IF_BUCKET13
               ,CASE WHEN DAY_RANGE_BY_ORDER_NO > 14
                     THEN MIN_ADD_195
                     ELSE MIN_ADD_91
                     END AS IF_BUCKET14
      
        FROM CTE_SOLI soli
        LEFT JOIN CTE_SOLI_BIZDAY biz
        ON soli.ORDER_NUMBER = biz.ORDER_NUMBER
    )
    
    ,CTE_SETBUCKETS AS (
        SELECT   soli.ORDER_NUMBER
                ,soli.UNIQUE_KEY
                ,soli.NS_LINE_NUMBER
                ,soli.ITEM_NO
                ,soli.RECEIPT_DATE
                ,soli.FREDD
                ,soli.FK_SPA_ID
                ,soli.SOURCE_LOAD_DATE
                ,c.MIN_ADD_12 AS RECEIPT_DATE_ADD_12_BIZ
                ,CASE WHEN soli.FREDD BETWEEN IF_BUCKET1  AND IF_BUCKET2  THEN 'BUCKET1'
                      WHEN soli.FREDD BETWEEN IF_BUCKET2  AND IF_BUCKET3  THEN 'BUCKET2'
                      WHEN soli.FREDD BETWEEN IF_BUCKET3  AND IF_BUCKET4  THEN 'BUCKET3'
                      WHEN soli.FREDD BETWEEN IF_BUCKET4  AND IF_BUCKET5  THEN 'BUCKET4'
                      WHEN soli.FREDD BETWEEN IF_BUCKET5  AND IF_BUCKET6  THEN 'BUCKET5'
                      WHEN soli.FREDD BETWEEN IF_BUCKET6  AND IF_BUCKET7  THEN 'BUCKET6'
                      WHEN soli.FREDD BETWEEN IF_BUCKET7  AND IF_BUCKET8  THEN 'BUCKET7'
                      WHEN soli.FREDD BETWEEN IF_BUCKET8  AND IF_BUCKET9  THEN 'BUCKET8'
                      WHEN soli.FREDD BETWEEN IF_BUCKET9  AND IF_BUCKET10 THEN 'BUCKET9'
                      WHEN soli.FREDD BETWEEN IF_BUCKET10 AND IF_BUCKET11 THEN 'BUCKET10'
                      WHEN soli.FREDD BETWEEN IF_BUCKET11 AND IF_BUCKET12 THEN 'BUCKET11'
                      WHEN soli.FREDD BETWEEN IF_BUCKET12 AND IF_BUCKET13 THEN 'BUCKET12'
                      WHEN soli.FREDD BETWEEN IF_BUCKET13 AND IF_BUCKET14 THEN 'BUCKET13'
                      END AS BUCKET_ON_FREDD
                ,CASE WHEN soli.FREDD BETWEEN IF_BUCKET1  AND IF_BUCKET2  THEN IF_BUCKET1
                      WHEN soli.FREDD BETWEEN IF_BUCKET2  AND IF_BUCKET3  THEN IF_BUCKET2
                      WHEN soli.FREDD BETWEEN IF_BUCKET3  AND IF_BUCKET4  THEN IF_BUCKET3
                      WHEN soli.FREDD BETWEEN IF_BUCKET4  AND IF_BUCKET5  THEN IF_BUCKET4
                      WHEN soli.FREDD BETWEEN IF_BUCKET5  AND IF_BUCKET6  THEN IF_BUCKET5
                      WHEN soli.FREDD BETWEEN IF_BUCKET6  AND IF_BUCKET7  THEN IF_BUCKET6
                      WHEN soli.FREDD BETWEEN IF_BUCKET7  AND IF_BUCKET8  THEN IF_BUCKET7
                      WHEN soli.FREDD BETWEEN IF_BUCKET8  AND IF_BUCKET9  THEN IF_BUCKET8
                      WHEN soli.FREDD BETWEEN IF_BUCKET9  AND IF_BUCKET10 THEN IF_BUCKET9
                      WHEN soli.FREDD BETWEEN IF_BUCKET10 AND IF_BUCKET11 THEN IF_BUCKET10
                      WHEN soli.FREDD BETWEEN IF_BUCKET11 AND IF_BUCKET12 THEN IF_BUCKET11
                      WHEN soli.FREDD BETWEEN IF_BUCKET12 AND IF_BUCKET13 THEN IF_BUCKET12
                      WHEN soli.FREDD BETWEEN IF_BUCKET13 AND IF_BUCKET14 THEN IF_BUCKET13
                      END AS BUCKET_DATE
                ,CASE WHEN soli.RECEIPT_DATE <=      IF_BUCKET1 THEN 'BUCKET1'
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET1  AND IF_BUCKET2  THEN 'BUCKET2'
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET2  AND IF_BUCKET3  THEN 'BUCKET3'
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET3  AND IF_BUCKET4  THEN 'BUCKET4'
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET4  AND IF_BUCKET5  THEN 'BUCKET5'
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET5  AND IF_BUCKET6  THEN 'BUCKET6'
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET6  AND IF_BUCKET7  THEN 'BUCKET7'
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET7  AND IF_BUCKET8  THEN 'BUCKET8'
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET8  AND IF_BUCKET9  THEN 'BUCKET9'
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET9  AND IF_BUCKET10 THEN 'BUCKET10'
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET10 AND IF_BUCKET11 THEN 'BUCKET11'
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET11 AND IF_BUCKET12 THEN 'BUCKET12'
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET12 AND IF_BUCKET13 THEN 'BUCKET13'
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET13 AND IF_BUCKET14 THEN 'BUCKET14'
                      END AS BUCKET_ON_RECEIPT
                ,CASE WHEN soli.RECEIPT_DATE <=      IF_BUCKET1                  THEN IF_BUCKET1
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET1  AND IF_BUCKET2  THEN IF_BUCKET2
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET2  AND IF_BUCKET3  THEN IF_BUCKET3
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET3  AND IF_BUCKET4  THEN IF_BUCKET4
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET4  AND IF_BUCKET5  THEN IF_BUCKET5
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET5  AND IF_BUCKET6  THEN IF_BUCKET6
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET6  AND IF_BUCKET7  THEN IF_BUCKET7
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET7  AND IF_BUCKET8  THEN IF_BUCKET8
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET8  AND IF_BUCKET9  THEN IF_BUCKET9
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET9  AND IF_BUCKET10 THEN IF_BUCKET10
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET10 AND IF_BUCKET11 THEN IF_BUCKET11
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET11 AND IF_BUCKET12 THEN IF_BUCKET12
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET12 AND IF_BUCKET13 THEN IF_BUCKET13
                      WHEN soli.RECEIPT_DATE BETWEEN IF_BUCKET13 AND IF_BUCKET14 THEN IF_BUCKET14
                      END AS BUCKET_DATE_ON_RECEIPT
                ,cte.IS_GT_15_BIZDAYS
                ,cte.IF_BUCKET1
                ,cte.IF_BUCKET2
                ,cte.IF_BUCKET3
                ,cte.IF_BUCKET4
                ,cte.IF_BUCKET5
                ,cte.IF_BUCKET6
                ,cte.IF_BUCKET7
                ,cte.IF_BUCKET8
                ,cte.IF_BUCKET9
                ,cte.IF_BUCKET10
                ,cte.IF_BUCKET11
                ,cte.IF_BUCKET12
                ,cte.IF_BUCKET13
                ,cte.IF_BUCKET14
        FROM DEV.BUSINESS_OPERATIONS.SOLI soli
        LEFT JOIN CTE_SETIFDATE cte
               ON soli.ORDER_NUMBER = cte.ORDER_NUMBER
        LEFT JOIN DEV.BUSINESS_OPERATIONS.DIM_FULFILLMENT_CALENDAR c
               ON soli.RECEIPT_DATE = c.RAW_DATE
    )
    
    ,CTE_CAPPING_DDA AS (
       SELECT cte.ORDER_NUMBER
             ,BUCKET_ON_RECEIPT
             ,MAX(cte.FREDD::DATE) AS CAPPING_DDA
       FROM CTE_SETBUCKETS cte
       LEFT OUTER JOIN DEV.BUSINESS_OPERATIONS.SOLI soli
         ON cte.ORDER_NUMBER    = soli.ORDER_NUMBER
        AND cte.ITEM_NO         = soli.ITEM_NO
        AND cte.FK_SPA_ID       = soli.FK_SPA_ID
       GROUP BY cte.ORDER_NUMBER, BUCKET_ON_RECEIPT
    )
    
    SELECT   bucket.ORDER_NUMBER
            ,bucket.UNIQUE_KEY
            ,bucket.NS_LINE_NUMBER
            ,bucket.ITEM_NO
            ,bucket.FK_SPA_ID
            ,bucket.RECEIPT_DATE::DATE      AS RECEIPT_DATE
            ,bucket.FREDD::DATE             AS FREDD
            ,bucket.BUCKET_ON_RECEIPT
            ,bucket.BUCKET_DATE_ON_RECEIPT
            ,cap.CAPPING_DDA
            ,bucket.IS_GT_15_BIZDAYS
            ,bucket.IF_BUCKET1
            ,bucket.IF_BUCKET2
            ,bucket.IF_BUCKET3
            ,bucket.IF_BUCKET4
            ,bucket.IF_BUCKET5
            ,bucket.IF_BUCKET6
            ,bucket.IF_BUCKET7
            ,bucket.IF_BUCKET8
            ,bucket.IF_BUCKET9
            ,bucket.IF_BUCKET10
            ,bucket.IF_BUCKET11
            ,bucket.IF_BUCKET12
            ,bucket.IF_BUCKET13
            ,bucket.IF_BUCKET14
            ,bucket.SOURCE_LOAD_DATE
    FROM CTE_SETBUCKETS bucket
    LEFT OUTER JOIN CTE_CAPPING_DDA cap
      ON bucket.ORDER_NUMBER        = cap.ORDER_NUMBER
     AND bucket.BUCKET_ON_RECEIPT   = cap.BUCKET_ON_RECEIPT
;