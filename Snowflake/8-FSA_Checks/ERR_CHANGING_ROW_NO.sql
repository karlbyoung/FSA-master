create or replace table ${vj_fsa_db}.${vj_fsa_schema}.ERR_CHANGING_ROW_NO AS
(
  select * from ${vj_fsa_db}.${vj_fsa_schema}.CHANGING_ROW_NO err
);
