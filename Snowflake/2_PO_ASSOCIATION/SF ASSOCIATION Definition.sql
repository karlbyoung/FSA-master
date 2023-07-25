CREATE OR REPLACE PROCEDURE DEV.${FSA_PROD_SCHEMA}.ASSIGN_PO_SF()
  RETURNS TEXT
  LANGUAGE SQL
  EXECUTE AS CALLER
AS 
$$

/*  DESCRIBE VARIABLES --------------------------- */

    DECLARE
         /*  SET VARIABLES --------------------------- */
        MAX_PO_ID   number;                 -- MAX PO_ID FOR LOOP 
        PO_ID       number  default 1;      -- PO_ID COUNTER
    BEGIN
        LET cur_run_date DATE := CURRENT_DATE();
        SELECT MAX(INSERT_DATE) INTO :cur_run_date FROM DEV.${FSA_PROD_SCHEMA}.DEMAND_PO;

        SELECT MAX(PO_ID) into :MAX_PO_ID
                   FROM DEV.${FSA_PROD_SCHEMA}.UNASSIGNED_DEMAND;
        FOR PO_ID in 1 to MAX_PO_ID do
            LET found BOOLEAN := false;
    
            LET rs RESULTSET := (SELECT DISTINCT SO.PO_ID
                    ,SO.FSA_LOAD_STATUS::TEXT AS "FSA_LOAD_STATUS"
                    ,SO.ID
                    ,SO.ITEM_ID
                    ,SO.ORIGINAL_DDA
                    ,SO.SEQUENCING_DDA
                    ,SO.ORDER_NUMBER
                    ,SO.QTY_ON_HAND
                    ,SO.QTY_ORDERED
                    ,SO.REMAINING_QTY_ON_HAND
                    ,SO.TOTAL_QTY_SOLD
                    ,SO.DDA
                    ,SO.ITEM
                    ,SO.LOCATION
                    ,SO.NS_LINE_NUMBER
                    ,SO.ROW_NO
                    ,SO.PRIORITY
                    ,SO.SEQ
                    ,SO.TRANSACTION_TYPE
                    ,SO.TYPE_NAME
                    ,SO.TRANSACTION_ID
                    ,SO.LINE_ID
                    ,SO.UNIQUE_KEY
                    ,SO.PO_SLIPPAGE
                    ,SO.ITEM_ROW_NO
                    ,SO.SOURCE_TYPE
                    ,SO.COMPONENT_ITEM
                    ,SO.COMPONENT_ITEM_ID
                    ,SO.ITEM_ID_BY_TRANSACTION_TYPE
                    ,SO.SOURCE_LOAD_DATE
                    ,SO.PK_ID
                    ,SO.IS_ASSEMBLY_COMPONENT
                    ,SO.CREATE_DATE
                    ,SO.PO_INDICATOR
                    ,:cur_run_date AS "PO_UPDATE_DATETIME"
                    ,FIRST_VALUE(OP."ORDER_NUMBER")            OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP."NS_RECEIVE_BY_DATE", OP."ORDER_NUMBER") AS "PO_ORDER_NUMBER"
                    ,FIRST_VALUE(OP."QUANTITY_TO_BE_RECEIVED") OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP."NS_RECEIVE_BY_DATE", OP."ORDER_NUMBER") AS "PO_QUANTITY_TO_BE_RECEIVED"
                    ,("PO_QUANTITY_TO_BE_RECEIVED" - SO."QTY_ORDERED") AS "PO_QUANTITY_REMAINING"
                    ,FIRST_VALUE(OP."NS_RECEIVE_BY_DATE")      OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP."NS_RECEIVE_BY_DATE", OP."ORDER_NUMBER") AS "PO_RECEIVE_BY_DATE"
                    ,OP.OG_QUANTITY_TO_BE_RECEIVED
                FROM DEV.${FSA_PROD_SCHEMA}."UNASSIGNED_DEMAND" SO
                LEFT OUTER JOIN DEV.${FSA_PROD_SCHEMA}."OPEN_PO_TRACKED" OP
                    ON  SO."ITEM_ID_BY_TRANSACTION_TYPE" =  OP."ITEM_ID"
                    AND SO."QTY_ORDERED"                 <= OP."QUANTITY_TO_BE_RECEIVED"
                WHERE OP."ORDER_NUMBER" IS NOT NULL AND "PO_ID" = :PO_ID 
                ORDER BY SO."ITEM_ID", SO."ROW_NO", IFNULL(SO."PO_ID", 0));
                
            FOR record in rs DO
                found := true;
                break;
            END FOR;
            IF (found) THEN
                CREATE OR REPLACE TABLE DEV.${FSA_PROD_SCHEMA}."DEMAND_ASSIGNMENT_TRACKED" AS SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

                CREATE OR REPLACE TABLE DEV.${FSA_PROD_SCHEMA}.OPEN_PO_TRACKED AS
                    SELECT DISTINCT OP.PO_ITEM_ID
                        , OP.ITEM_ID
                        , OP.ITEM_ID_C
                        , OP.ASSEMBLY_ITEM_ID
                        , OP.ORDER_NUMBER
                        , OP."OG_QUANTITY_TO_BE_RECEIVED"
                        , IFF(SO."PO_QUANTITY_REMAINING" IS NOT NULL, SO."PO_QUANTITY_REMAINING", OP."QUANTITY_TO_BE_RECEIVED") AS "QUANTITY_TO_BE_RECEIVED"
                        , OP.NS_RECEIVE_BY_DATE
                        , OP.PO_ROW_NO
                    FROM DEV.${FSA_PROD_SCHEMA}."OPEN_PO_TRACKED" OP
                    LEFT OUTER JOIN DEV.${FSA_PROD_SCHEMA}."DEMAND_ASSIGNMENT_TRACKED" SO
                        ON  SO."ITEM_ID_BY_TRANSACTION_TYPE" = OP."ITEM_ID"
                        AND SO."PO_ORDER_NUMBER"             = OP."ORDER_NUMBER"
                        AND SO."PO_RECEIVE_BY_DATE"           = OP."NS_RECEIVE_BY_DATE";

                CREATE OR REPLACE TABLE DEV.${FSA_PROD_SCHEMA}.ASSIGNED_DEMAND AS
                    SELECT * 
                    FROM (SELECT * FROM DEV.${FSA_PROD_SCHEMA}.ASSIGNED_DEMAND
                          UNION
                          SELECT * FROM DEV.${FSA_PROD_SCHEMA}.DEMAND_ASSIGNMENT_TRACKED)
                    ORDER BY ITEM_ID, ROW_NO;
            END IF;
        END FOR;
        return 'Success: '||MAX_PO_ID::text||' iterations';
    EXCEPTION
        when statement_error then
            return 'Statement error: '||sqlerrm;
        when other then
            return 'Error: '||sqlerrm;
    END;
$$;