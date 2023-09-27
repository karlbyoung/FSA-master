-- Error if capping DDA is incorrectly NULL (i.e, should be zero rows generated in temp table)
create temporary table ${vj_fsa_db}.${vj_fsa_schema}.CAPPING_DDA_IS_NULL as
(
  select pk_id,capping_dda,prelim_edd
      from ${vj_fsa_db}.${vj_fsa_schema}.fsa
  	  where capping_dda is null
  		and prelim_edd is not null
);