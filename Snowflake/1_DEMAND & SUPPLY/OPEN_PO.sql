CREATE OR REPLACE TABLE DEV.${FSA_CURRENT_SCHEMA}.OPEN_PO AS
(
  SELECT *
  FROM DEV.${FSA_CURRENT_SCHEMA}.OPEN_PO_ALL dpo
)
