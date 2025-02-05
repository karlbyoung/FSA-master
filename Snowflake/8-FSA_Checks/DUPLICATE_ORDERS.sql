-- Error if Order Number appears with more than one transaction ID (i.e, should be zero rows generated in temp table)
create temporary table ${vj_fsa_db}.${vj_fsa_schema}.DUPLICATE_ORDERS as

  with order_tran_pairs as
  (
    select distinct order_number,transaction_id
    from ${vj_fsa_db}.${vj_fsa_schema}.fsa
  )
  , dups as
  (
    select order_number,count(*) num
    from order_tran_pairs
    group by order_number
    having num > 1
  )
  select * from order_tran_pairs
  where order_number in
    (select distinct order_number from dups)
    order by order_number,transaction_id
;


