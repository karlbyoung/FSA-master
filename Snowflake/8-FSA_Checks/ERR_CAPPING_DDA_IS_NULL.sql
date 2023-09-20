create or replace table ${vj_fsa_db}.${vj_fsa_schema}.ERR_CAPPING_DDA_IS_NULL AS
(
  select err.*,
  	  fsa.* exclude (pk_id, capping_dda, prelim_edd)
  	from ${vj_fsa_db}.${vj_fsa_schema}.fsa
  	join ${vj_fsa_db}.${vj_fsa_schema}.CAPPING_DDA_IS_NULL err
  		on fsa.pk_id = err.pk_id
);
