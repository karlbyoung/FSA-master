create or replace table ${vj_fsa_db}.${vj_fsa_schema}.ERR_DUPLICATE_ORDERS AS
(
  select * from ${vj_fsa_db}.${vj_fsa_schema}.DUPLICATE_ORDERS err
);
