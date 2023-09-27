-- Error if FSA_OUTPUT_STATUS not equal to UPDATED when significant values change (i.e, should be zero rows generated in temp table)
--  values checked:
--    AVAIL_DATE
--    CAPPING_DDA
--    PO_ORDER_NUMBER
--    PO_RECEIVE_BY_DATE
--    PO_INDICATOR
--    PO_INDICATOR_ASSIGN
--    ITEM_ID
--
create temporary table ${vj_fsa_db}.${vj_fsa_schema}.CHANGES_MISMATCH_UPDATED as
WITH cur_fsa AS
(
  SELECT * 
  FROM table(${vj_fsa_db}.${vj_fsa_schema}.latest_fsa(-1))
)
, prev_fsa AS
(
  SELECT * 
  FROM table(${vj_fsa_db}.${vj_fsa_schema}.latest_fsa(-2))
)
, detect_changes AS
(
  SELECT 
    COALESCE (a.PK_ID, b.PK_ID) AS PK_ID, 
    b.FSA_OUTPUT_STATUS         AS FSA_OUTPUT_STATUS, 
    a.AVAIL_DATE                AS PREV_AVAIL_DATE, 
    b.AVAIL_DATE                AS CURR_AVAIL_DATE, 
    a.CAPPING_DDA               AS PREV_CAPPING_DDA, 
    b.CAPPING_DDA               AS CURR_CAPPING_DDA, 
    a.PO_ORDER_NUMBER           AS PREV_PO_ORDER_NUMBER, 
    b.PO_ORDER_NUMBER           AS CURR_PO_ORDER_NUMBER, 
    a.PO_RECEIVE_BY_DATE        AS PREV_PO_RECEIVE_BY_DATE, 
    b.PO_RECEIVE_BY_DATE        AS CURR_PO_RECEIVE_BY_DATE, 
    a.PO_INDICATOR              AS PREV_PO_INDICATOR, 
    b.PO_INDICATOR              AS CURR_PO_INDICATOR, 
    a.PO_INDICATOR_ASSIGN       AS PREV_PO_INDICATOR_ASSIGN, 
    b.PO_INDICATOR_ASSIGN       AS CURR_PO_INDICATOR_ASSIGN, 
    a.ITEM_ID                   AS PREV_ITEM_ID, 
    b.ITEM_ID                   AS CURR_ITEM_ID, 
    CASE 
      WHEN a.PK_ID IS NULL THEN CAST('N' AS VARCHAR(1))
      WHEN b.PK_ID IS NULL THEN CAST('D' AS VARCHAR(1)) 
      WHEN    (a.AVAIL_DATE           = b.AVAIL_DATE          OR (a.AVAIL_DATE IS NULL          AND b.AVAIL_DATE IS NULL)) 
          AND (a.CAPPING_DDA          = b.CAPPING_DDA         OR (a.CAPPING_DDA IS NULL         AND b.CAPPING_DDA IS NULL)) 
          AND (a.PO_ORDER_NUMBER      = b.PO_ORDER_NUMBER     OR (a.PO_ORDER_NUMBER IS NULL     AND b.PO_ORDER_NUMBER IS NULL)) 
          AND (a.PO_RECEIVE_BY_DATE   = b.PO_RECEIVE_BY_DATE  OR (a.PO_RECEIVE_BY_DATE IS NULL  AND b.PO_RECEIVE_BY_DATE IS NULL)) 
          AND (a.PO_INDICATOR         = b.PO_INDICATOR        OR (a.PO_INDICATOR IS NULL        AND b.PO_INDICATOR IS NULL)) 
          AND (a.PO_INDICATOR_ASSIGN  = b.PO_INDICATOR_ASSIGN OR (a.PO_INDICATOR_ASSIGN IS NULL AND b.PO_INDICATOR_ASSIGN IS NULL)) 
          AND (a.ITEM_ID              = b.ITEM_ID             OR (a.ITEM_ID IS NULL             AND b.ITEM_ID IS NULL))
        THEN CAST('I' AS VARCHAR(1)) 
      ELSE CAST('C' AS VARCHAR(1)) 
    END                         AS Indicator 
  FROM prev_fsa AS a 
  FULL JOIN cur_fsa AS b
      ON a.PK_ID = b.PK_ID 
  WHERE (a.PK_ID IS NOT NULL) 
    OR (b.PK_ID IS NOT NULL)
)
SELECT *,
  (FSA_OUTPUT_STATUS = 'UPDATED') != (Indicator = 'C') AS Mismatch 
FROM detect_changes
WHERE 
  (Mismatch = 'TRUE')
