-- Error if avail dates after capping DDA (i.e, should be zero rows generated in temp table)
create temporary table ${vj_fsa_db}.${vj_fsa_schema}.AVAIL_DATE_AFTER_CAPPING_DDA as
(
  select PK_ID,ORDER_NUMBER,NS_LINE_NUMBER,COMPONENT_ITEM_ID,AVAIL_DATE,CAPPING_DDA
    from table(${vj_fsa_db}.${vj_fsa_schema}.latest_fsa())
    where AVAIL_DATE > CAPPING_DDA);
