create or replace table ${vj_fsa_db}.${vj_fsa_schema}.ERR_UNEVEN_NUM_ROWS AS
(
  select * from ${vj_fsa_db}.${vj_fsa_schema}.UNEVEN_NUM_ROWS err
);
