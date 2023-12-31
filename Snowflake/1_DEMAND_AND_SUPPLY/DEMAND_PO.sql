CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}.DEMAND_PO AS
(
  SELECT distinct dpo.* EXCLUDE PK_ID,
  	"UNIQUE_KEY"||'^'||ZEROIFNULL("COMPONENT_ITEM_ID")::TEXT PK_ID,
  	prev.ROW_NO
  FROM DEV.${vj_fsa_schema}.DEMAND_PO_ALL dpo
  LEFT OUTER JOIN (
      /* 20230724 - KBY, RFS23-1850 - keep ROW_NO low for duplicate order numbers */
      SELECT DISTINCT ORDER_NUMBER, MIN(ROW_NO) OVER (PARTITION BY ORDER_NUMBER) ROW_NO 
      FROM DEV.${vj_fsa_schema}.DEMAND_PREV_ASSIGNED
    ) prev
  on dpo.ORDER_NUMBER = prev.ORDER_NUMBER
)
