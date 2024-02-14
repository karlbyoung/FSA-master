-- Error if primary keys not consistent from Unique Key to Order/Line (i.e, should be zero rows generated in temp table)
create temporary table ${vj_fsa_db}.${vj_fsa_schema}.KEYS_NOT_CONSISTENT as
(
  with unique_keys as
  (
    select distinct unique_key,transaction_id,ifnull(line_id,ns_line_number) line_id
    from (select * from table(${vj_fsa_db}.${vj_fsa_schema}.latest_fsa(-1)) union select * from table(${vj_fsa_db}.${vj_fsa_schema}.latest_fsa(-2)))
  )
  select unique_key::text ukey,'Unique_Key (multiple tran_id/line_id)' key_type,count(*) num 
      from unique_keys
      group by ukey,key_type
      having num > 1
  union
  select transaction_id||'/'||line_id::text ukey,'trans_id/line_id (multiple Unique_Key)' key_type,count(*) num 
      from unique_keys
      group by ukey,key_type
      having num > 1
);


