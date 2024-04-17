CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}.DEMAND_PO AS
(
  with dpa2 as
  (
    /* 20240404 -- KBY - link transaction_ID to DEMAND_PREV_ASSIGNED */
    SELECT dpa.*,
      t2.TRANSACTION_ID
    FROM DEV.${vj_fsa_schema}.DEMAND_PREV_ASSIGNED dpa
    JOIN (SELECT DISTINCT pk_id,transaction_id FROM DEV.${vj_fsa_schema}.DEMAND_PO_ALL_HISTORICAL) t2
      on dpa.pk_id = t2.pk_id
  )
  SELECT distinct dpo.* EXCLUDE PK_ID,
  	dpo.UNIQUE_KEY||'^'||ZEROIFNULL(COMPONENT_ITEM_ID)::TEXT PK_ID,
  	prev.ROW_NO
  FROM DEV.${vj_fsa_schema}.DEMAND_PO_ALL dpo
  LEFT OUTER JOIN (
      SELECT DISTINCT
        ORDER_NUMBER, 
      /* 20230724 - KBY, RFS23-1850 - keep ROW_NO low for duplicate order numbers */
        MIN(ROW_NO) OVER (PARTITION BY TRANSACTION_ID) ROW_NO,
      /* 20240404 -- KBY - match on transaction_ID, not order_number */
        TRANSACTION_ID
      FROM dpa2
    ) prev
  on dpo.TRANSACTION_ID = prev.TRANSACTION_ID
)
