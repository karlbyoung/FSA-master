-- Error if primary keys not unique (i.e, should be zero rows generated in temp table)
create temporary table ${vj_fsa_db}.${vj_fsa_schema}.KEYS_NOT_UNIQUE as
(
  select count(*) total_rows,
      count(distinct pk_id) num_pk_id,
      count(distinct unique_key,ifnull(component_item_id,0)) num_composite_ukey_compitemid,
      count(distinct order_number,ns_line_number,ifnull(component_item_id,0)) num_composit_orderno_line_compitemid
  from ${vj_fsa_db}.${vj_fsa_schema}.demand_po_all
  having total_rows != num_pk_id
  	or total_rows != num_composite_ukey_compitemid
  	or total_rows != num_composit_orderno_line_compitemid
);
