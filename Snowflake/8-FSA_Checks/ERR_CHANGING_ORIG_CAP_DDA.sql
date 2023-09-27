create or replace table ${vj_fsa_db}.${vj_fsa_schema}.ERR_CHANGING_ORIG_CAP_DDA AS
(
  select * from ${vj_fsa_db}.${vj_fsa_schema}.CHANGING_ORIG_CAP_DDA err
);
