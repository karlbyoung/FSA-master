create or replace table ${vj_fsa_db}.${vj_fsa_schema}.ERR_KEYS_NOT_CONSISTENT AS
(
  select * from ${vj_fsa_db}.${vj_fsa_schema}.KEYS_NOT_CONSISTENT err
);
