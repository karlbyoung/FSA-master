CREATE OR REPLACE PROCEDURE DEV.${vj_fsa_schema}.UNDO_ONE_FSA_SESSION()
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  DECLARE
    tmp_insert TIMESTAMP_LTZ;
  BEGIN
    select fsa_insert_date into :tmp_insert 
    	from table(DEV.${vj_fsa_schema}.dates_fsa()) 
        where recent_or_inorder = -1;
    update DEV.${vj_fsa_schema}.fsa_historical 
    	set is_valid = false 
    	where fsa_insert_date = :tmp_insert;

    select fsa_insert_date into :tmp_insert 
    	from table(DEV.${vj_fsa_schema}.dates_demand_prev_assigned()) 
        where recent_or_inorder = -1;
    update DEV.${vj_fsa_schema}.demand_prev_assigned_historical 
    	set is_valid = false 
        where fsa_insert_date = :tmp_insert;

    select fsa_insert_date into :tmp_insert 
    	from table(DEV.${vj_fsa_schema}.dates_open_po_prev_assigned()) 
        where recent_or_inorder = -1;
    update DEV.${vj_fsa_schema}.open_po_prev_assigned_historical 
    	set is_valid = false 
        where fsa_insert_date = :tmp_insert;

    select fsa_insert_date into :tmp_insert 
    	from table(DEV.${vj_fsa_schema}.dates_demand_po_all()) 
        where recent_or_inorder = -1;
    update DEV.${vj_fsa_schema}.demand_po_all_historical 
    	set is_valid = false 
        where fsa_insert_date = :tmp_insert;

    select fsa_insert_date into :tmp_insert 
    	from table(DEV.${vj_fsa_schema}.dates_open_po_all()) 
        where recent_or_inorder = -1;
    update DEV.${vj_fsa_schema}.open_po_all_historical 
    	set is_valid = false 
        where fsa_insert_date = :tmp_insert;

    create or replace table DEV.${vj_fsa_schema}.demand_po_all 
    	as select * from table(DEV.${vj_fsa_schema}.latest_demand_po_all(-1));
    create or replace table DEV.${vj_fsa_schema}.open_po_all 
    	as select * from table(DEV.${vj_fsa_schema}.latest_open_po_all(-1));
    create or replace table DEV.${vj_fsa_schema}.demand_prev_assigned 
    	as select * from table(DEV.${vj_fsa_schema}.latest_demand_prev_assigned(-1));
    create or replace table DEV.${vj_fsa_schema}.open_po_prev_assigned 
    	as select * from table(DEV.${vj_fsa_schema}.latest_open_po_prev_assigned(-1));
    create or replace table DEV.${vj_fsa_schema}.fsa 
    	as select * from table(DEV.${vj_fsa_schema}.latest_fsa(-1));

    let rs RESULTSET := (select recent_or_inorder index,source_load_date 
                         from table(DEV.${vj_fsa_schema}.dates_fsa()) 
                         where index < 0 order by index desc);
    return table(rs);

  END
$$;
