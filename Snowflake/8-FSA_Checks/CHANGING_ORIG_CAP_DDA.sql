-- Error if any changes (i.e, should be zero rows generated in temp table)
create temporary table ${vj_fsa_db}.${vj_fsa_schema}.CHANGING_ORIG_CAP_DDA as
(
  with t1 as
  (select pk_id,order_number,ns_line_number,component_item,capping_dda,orig_cap_dda,source_load_date from table(${vj_fsa_db}.${vj_fsa_schema}.latest_fsa(-1)))
  , t2 as
  (select pk_id,order_number,ns_line_number,component_item,capping_dda,orig_cap_dda,source_load_date from table(${vj_fsa_db}.${vj_fsa_schema}.latest_fsa(-2)))
  select t1.source_load_date,t1.pk_id,t1.order_number,t1.ns_line_number,t1.component_item,t1.capping_dda,t1.orig_cap_dda,t2.source_load_date prev_load_date,t2.capping_dda prev_capping_dda,t2.orig_cap_dda prev_orig_cap_dda
      from t1 join t2 on t1.pk_id = t2.pk_id
      where t2.orig_cap_dda is not null and t1.orig_cap_dda != t2.orig_cap_dda
);