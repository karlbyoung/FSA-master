CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}.DEMAND_PO AS
(
  SELECT distinct dpo.* EXCLUDE PK_ID,
  	dpo.UNIQUE_KEY||'^'||ZEROIFNULL(COMPONENT_ITEM_ID)::TEXT PK_ID,
  	prev.ROW_NO
  FROM DEV.${vj_fsa_schema}.DEMAND_PO_ALL dpo
  LEFT OUTER JOIN (
      SELECT DISTINCT
        ORDER_NUMBER, 
      /* 20230724 - KBY, RFS23-1850 - keep ROW_NO low for duplicate order numbers */
        MIN(ROW_NO) OVER (PARTITION BY ORDER_NUMBER) ROW_NO,
      /* 20240401 -- KBY - match on unique_key, not order_number */
        UNIQUE_KEY
      FROM DEV.${vj_fsa_schema}.DEMAND_PREV_ASSIGNED
    ) prev
  on dpo.UNIQUE_KEY = prev.UNIQUE_KEY
)
