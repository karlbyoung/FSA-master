create or replace table ${vj_fsa_db}.${vj_fsa_schema}.ERR_AVAIL_DATE_AFTER_CAPPING_DDA AS
(
  select * from ${vj_fsa_db}.${vj_fsa_schema}.AVAIL_DATE_AFTER_CAPPING_DDA err
);
