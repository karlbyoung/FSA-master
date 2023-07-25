CREATE OR REPLACE PROCEDURE DEV.${FSA_PROD_SCHEMA}.UNDO_ONE_FSA_SESSION()
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
AS $$
  DECLARE
    tmp_insert TIMESTAMP_LTZ;
  BEGIN
    select fsa_insert_date into :tmp_insert 
    	from table(DEV.${FSA_PROD_SCHEMA}.dates_fsa()) 
        where recent_or_inorder = -1;
    update DEV.${FSA_PROD_SCHEMA}.fsa_historical 
    	set is_valid = false 
    	where fsa_insert_date = :tmp_insert;

    select fsa_insert_date into :tmp_insert 
    	from table(DEV.${FSA_PROD_SCHEMA}.dates_demand_prev_assigned()) 
        where recent_or_inorder = -1;
    update DEV.${FSA_PROD_SCHEMA}.demand_prev_assigned_historical 
    	set is_valid = false 
        where fsa_insert_date = :tmp_insert;

    select fsa_insert_date into :tmp_insert 
    	from table(DEV.${FSA_PROD_SCHEMA}.dates_open_po_prev_assigned()) 
        where recent_or_inorder = -1;
    update DEV.${FSA_PROD_SCHEMA}.open_po_prev_assigned_historical 
    	set is_valid = false 
        where fsa_insert_date = :tmp_insert;

    select fsa_insert_date into :tmp_insert 
    	from table(DEV.${FSA_PROD_SCHEMA}.dates_demand_po_all()) 
        where recent_or_inorder = -1;
    update DEV.${FSA_PROD_SCHEMA}.demand_po_all_historical 
    	set is_valid = false 
        where fsa_insert_date = :tmp_insert;

    select fsa_insert_date into :tmp_insert 
    	from table(DEV.${FSA_PROD_SCHEMA}.dates_open_po_all()) 
        where recent_or_inorder = -1;
    update DEV.${FSA_PROD_SCHEMA}.open_po_all_historical 
    	set is_valid = false 
        where fsa_insert_date = :tmp_insert;

    create or replace table DEV.${FSA_PROD_SCHEMA}.demand_po_all 
    	as select * from table(DEV.${FSA_PROD_SCHEMA}.latest_demand_po_all(-1));
    create or replace table DEV.${FSA_PROD_SCHEMA}.open_po_all 
    	as select * from table(DEV.${FSA_PROD_SCHEMA}.latest_open_po_all(-1));
    create or replace table DEV.${FSA_PROD_SCHEMA}.demand_prev_assigned 
    	as select * from table(DEV.${FSA_PROD_SCHEMA}.latest_demand_prev_assigned(-1));
    create or replace table DEV.${FSA_PROD_SCHEMA}.open_po_prev_assigned 
    	as select * from table(DEV.${FSA_PROD_SCHEMA}.latest_open_po_prev_assigned(-1));
    create or replace table DEV.${FSA_PROD_SCHEMA}.fsa 
    	as select * from table(DEV.${FSA_PROD_SCHEMA}.latest_fsa(-1));

    let rs RESULTSET := (select recent_or_inorder index,source_load_date 
                         from table(DEV.${FSA_PROD_SCHEMA}.dates_fsa()) 
                         where index < 0 order by index desc);
    return table(rs);

  END
$$;
