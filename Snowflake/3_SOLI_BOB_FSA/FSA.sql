CREATE OR REPLACE TABLE DEV.${FSA_CURRENT_SCHEMA}.FSA AS
  SELECT DISTINCT 
         SPA.*
         EXCLUDE (AVAIL_DATE) 
         RENAME (
           QTY_ON_HAND             AS "TOTAL_AVAIL_QTY",
           REMAINING_QTY_ON_HAND   AS "REMAINING_AVAIL_QTY",
           TOTAL_QTY_SOLD          AS "SUM_ROLLUP",
           QTY_ORDERED             AS "QUANTITY",
           DDA					   AS "PRELIM_EDD"
         )
        , CASE 
            WHEN NULLIF(SPA.PO_ORDER_NUMBER,'0') != NULLIF(prev.PO_ORDER_NUMBER,'0') THEN bob.RECEIPT_DATE
            WHEN NULLIF(SPA.PO_RECEIVE_BY_DATE,'2000-01-01'::DATE) != NULLIF(prev.PO_RECEIVE_BY_DATE,'2000-01-01'::DATE) THEN bob.RECEIPT_DATE
            WHEN SPA.PO_INDICATOR != prev.PO_INDICATOR THEN bob.RECEIPT_DATE
            WHEN SPA.PO_INDICATOR_ASSIGN != prev.PO_INDICATOR_ASSIGN THEN bob.RECEIPT_DATE
            WHEN prev.PREV_AVAIL_DATE IS NULL THEN bob.RECEIPT_DATE
            ELSE prev.PREV_AVAIL_DATE
          END AS "AVAIL_DATE"
        ,bob.ORDER_NUMBER                             AS BOB_ORDER_NUMBER
        ,bob.ITEM_NO                                  AS BOB_ITEM
        ,bob.FREDD                                    AS FREDD
        ,bob.BUCKET_ON_RECEIPT                        AS BUCKET_ON_RECEIVE_BY_DATE
        ,bob.BUCKET_DATE_ON_RECEIPT                   AS BUCKET_DATE_ON_RECEIVE_BY_DATE
        ,CASE
            WHEN NULLIF(SPA.PO_ORDER_NUMBER,'0') != NULLIF(prev.PO_ORDER_NUMBER,'0') THEN bob.CAPPING_DDA
            WHEN NULLIF(SPA.PO_RECEIVE_BY_DATE,'2000-01-01'::DATE) != NULLIF(prev.PO_RECEIVE_BY_DATE,'2000-01-01'::DATE) THEN bob.CAPPING_DDA
            WHEN SPA.PO_INDICATOR != prev.PO_INDICATOR THEN bob.CAPPING_DDA
            WHEN SPA.PO_INDICATOR_ASSIGN != prev.PO_INDICATOR_ASSIGN THEN bob.CAPPING_DDA
            WHEN prev.ORIG_CAP_DDA IS NULL then bob.CAPPING_DDA
            ELSE prev.ORIG_CAP_DDA
          END CAPPING_DDA
        ,bob.IS_GT_15_BIZDAYS                         AS IS_GT_15_BIZDAYS
        ,bob.IF_BUCKET1                               AS IF_BUCKET1
        ,bob.IF_BUCKET2                               AS IF_BUCKET2
        ,bob.IF_BUCKET3                               AS IF_BUCKET3
        ,bob.IF_BUCKET4                               AS IF_BUCKET4
        ,bob.IF_BUCKET5                               AS IF_BUCKET5
        ,bob.IF_BUCKET6                               AS IF_BUCKET6
        ,bob.IF_BUCKET7                               AS IF_BUCKET7
        ,bob.IF_BUCKET8                               AS IF_BUCKET8
        ,bob.IF_BUCKET9                               AS IF_BUCKET9
        ,bob.IF_BUCKET10                              AS IF_BUCKET10
        ,bob.IF_BUCKET11                              AS IF_BUCKET11
        ,bob.IF_BUCKET12                              AS IF_BUCKET12
        ,bob.IF_BUCKET13                              AS IF_BUCKET13
        ,bob.IF_BUCKET14                              AS IF_BUCKET14
        ,IFF(ORIGINAL_DDA IS NULL, DDA, ORIGINAL_DDA) AS FSA_UPDATED_ORIGINAL_DDA
        ,SPA.AVAIL_DATE                               AS "ITEM_AVAIL_DATE"
        ,NULL::TEXT									  AS FSA_OUTPUT_STATUS
  FROM DEV.${FSA_CURRENT_SCHEMA}.SEQUENCING_PO_ASSIGN SPA
  INNER JOIN DEV.${FSA_CURRENT_SCHEMA}.BOB bob
    ON SPA.ID = bob.FK_SPA_ID
  LEFT JOIN DEV.${FSA_CURRENT_SCHEMA}.DEMAND_PREV_ASSIGNED prev
    ON SPA.UNIQUE_KEY = prev.UNIQUE_KEY
    AND ZEROIFNULL(SPA.COMPONENT_ITEM_ID) = ZEROIFNULL(prev.COMPONENT_ITEM_ID)
  ORDER BY ITEM_ID, IFNULL(PO_ID, 0);
  
  UPDATE DEV.${FSA_CURRENT_SCHEMA}.FSA t1
	SET t1.FSA_OUTPUT_STATUS = CASE
        WHEN t3.HASH_FSA_OUTPUT is NULL THEN 'NEW'
        WHEN t2.HASH_FSA_OUTPUT != t3.HASH_FSA_OUTPUT THEN 'UPDATED'
        ELSE 'UNCHANGED'
      END   
    FROM (SELECT DISTINCT PK_ID,
			  HASH(AVAIL_DATE,CAPPING_DDA,PO_ORDER_NUMBER,PO_RECEIVE_BY_DATE,PO_INDICATOR,PO_INDICATOR_ASSIGN,ITEM_ID) HASH_FSA_OUTPUT
            FROM DEV.${FSA_CURRENT_SCHEMA}.FSA) t2
          ,DEV.${FSA_CURRENT_SCHEMA}.DEMAND_PREV_ASSIGNED t3
    WHERE t1.PK_ID = t2.PK_ID
        AND t1.PK_ID = t3.PK_ID;
