-- Error if any rows uneven (i.e, should be zero rows generated in temp table)
create temporary table ${vj_fsa_db}.${vj_fsa_schema}.UNEVEN_NUM_ROWS as
(
  select t1.num demand_num_rows,t2.num fsa_num_rows
    from (
      select count(*) num from table(${vj_fsa_db}.${vj_fsa_schema}.latest_demand_po_all())
      where quantity is not null
    ) t1
    join (select count(*) num from table(${vj_fsa_db}.${vj_fsa_schema}.latest_fsa())) t2
    where demand_num_rows != fsa_num_rows
);
