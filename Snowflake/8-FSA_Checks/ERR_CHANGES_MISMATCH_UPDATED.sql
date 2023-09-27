create or replace table ${vj_fsa_db}.${vj_fsa_schema}.ERR_CHANGES_MISMATCH_UPDATED AS
(
  select * from ${vj_fsa_db}.${vj_fsa_schema}.CHANGES_MISMATCH_UPDATED err
);
