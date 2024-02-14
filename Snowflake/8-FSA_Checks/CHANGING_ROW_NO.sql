-- Error if any order changes its ROW_NO (i.e, should be zero rows generated in temp table)
create temporary table ${vj_fsa_db}.${vj_fsa_schema}.CHANGING_ROW_NO AS
with t1 as
(select distinct transaction_id,row_no,source_load_date from table(${vj_fsa_db}.${vj_fsa_schema}.latest_fsa(-1)))
, t2 as
(select distinct distinct transaction_id,row_no,source_load_date from table(${vj_fsa_db}.${vj_fsa_schema}.latest_fsa(-2)))
select t1.source_load_date,t1.transaction_id,t1.row_no,t2.source_load_date prev_load_date,t2.row_no prev_row_no
    from t1 join t2 on t1.transaction_id = t2.transaction_id
    where t1.row_no != t2.row_no;
