CREATE OR REPLACE PROCEDURE DEV.${FSA_PROD_SCHEMA}.ASSIGN_PO()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

/*  DESCRIBE VARIABLES --------------------------- */
    var MAX_PO_ID;      /* MAX PO_ID FOR LOOP */
    var PO_ID = 1;      /* PO_ID COUNTER      */
    
var crd = snowflake.execute({sqlText: `SELECT MAX(INSERT_DATE)::DATE::TEXT
                                     FROM DEV.${FSA_PROD_SCHEMA}.DEMAND_PO;`});
crd.next();
CUR_RUN_DATE = crd.getColumnValue(1);

CUR_RUN_DATE = `'` + CUR_RUN_DATE + `'`;



var x = snowflake.execute({sqlText: `SELECT MAX(PO_ID)
                                     FROM DEV.${FSA_PROD_SCHEMA}.UNASSIGNED_DEMAND;`});
x.next();
MAX_PO_ID = x.getColumnValue(1);

while (PO_ID <= MAX_PO_ID) {
    
    var foo_sql = `CREATE OR REPLACE TABLE DEV.${FSA_PROD_SCHEMA}."DEMAND_ASSIGNMENT_TRACKED" AS
                    WITH CTE_PO_ASSIGN AS 
                      (SELECT DISTINCT
                          SO.PO_ID
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
                          /*  20230608 - KBY, HyperCare 123 - When OpenSO is already assigned (has valid Location), IS_ALREADY_ASSIGNED contains TRUE */
                          ,SO.IS_ALREADY_ASSIGNED
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
                          /*  20230609 - KBY, HyperCare 113 - include values to describe using remaining available quantity for partial assignment */
                          ,SO.AVAIL_QTY_USED
                          ,SO.IS_PARTIAL_QTY
                          ,SO.PO_INDICATOR
                          ,` + CUR_RUN_DATE + ` AS "PO_UPDATE_DATETIME"
                          /* 20230717 - KBY, RFS23-1850 - Select PO's for forward-facing-only locations only and all locations (including non-forward-facing) */
                          ,FIRST_VALUE(OP."ORDER_NUMBER")            OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP."NS_RECEIVE_BY_DATE", OP."ORDER_NUMBER") AS "ALL_PO_ORDER_NUMBER"
                          ,FIRST_VALUE(OP."QUANTITY_TO_BE_RECEIVED") OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP."NS_RECEIVE_BY_DATE", OP."ORDER_NUMBER") AS "ALL_PO_QUANTITY_TO_BE_RECEIVED"
                          ,FIRST_VALUE(OP."NS_RECEIVE_BY_DATE")      OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP."NS_RECEIVE_BY_DATE", OP."ORDER_NUMBER") AS "ALL_PO_RECEIVE_BY_DATE"
                          ,FIRST_VALUE(OP_FWD."ORDER_NUMBER")            OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP_FWD."NS_RECEIVE_BY_DATE", OP_FWD."ORDER_NUMBER") AS "FWD_PO_ORDER_NUMBER"
                          ,FIRST_VALUE(OP_FWD."QUANTITY_TO_BE_RECEIVED") OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP_FWD."NS_RECEIVE_BY_DATE", OP_FWD."ORDER_NUMBER") AS "FWD_PO_QUANTITY_TO_BE_RECEIVED"
                          ,FIRST_VALUE(OP_FWD."NS_RECEIVE_BY_DATE")      OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP_FWD."NS_RECEIVE_BY_DATE", OP_FWD."ORDER_NUMBER") AS "FWD_PO_RECEIVE_BY_DATE"
                          /*  20230609 - KBY, HyperCare 113 - reduce PO qty remaining by the qty that was ordered, less any available qty that was left */
                          ,IFF(SO.IS_ASSEMBLY_COMPONENT,ALL_PO_ORDER_NUMBER,FWD_PO_ORDER_NUMBER)                               AS "PO_ORDER_NUMBER"
                          ,IFF(SO.IS_ASSEMBLY_COMPONENT,ALL_PO_QUANTITY_TO_BE_RECEIVED,FWD_PO_QUANTITY_TO_BE_RECEIVED)         AS "PO_QUANTITY_TO_BE_RECEIVED"
                          ,("PO_QUANTITY_TO_BE_RECEIVED" - SO."QTY_ORDERED" + IFF(SO."IS_PARTIAL_QTY",SO."AVAIL_QTY_USED",0))  AS "PO_QUANTITY_REMAINING"
                          ,IFF(SO.IS_ASSEMBLY_COMPONENT,ALL_PO_RECEIVE_BY_DATE,FWD_PO_RECEIVE_BY_DATE)                         AS "PO_RECEIVE_BY_DATE"
                          ,IFF(SO.IS_ASSEMBLY_COMPONENT,OP.OG_QUANTITY_TO_BE_RECEIVED,OP_FWD.OG_QUANTITY_TO_BE_RECEIVED)       AS "OG_QUANTITY_TO_BE_RECEIVED"
                      FROM DEV.${FSA_PROD_SCHEMA}."UNASSIGNED_DEMAND" SO
                      LEFT OUTER JOIN DEV.${FSA_PROD_SCHEMA}."OPEN_PO_TRACKED" OP
                        ON  SO."ITEM_ID_BY_TRANSACTION_TYPE" =  OP."ITEM_ID" 
                        AND SO."QTY_ORDERED"                 <= OP."QUANTITY_TO_BE_RECEIVED"
                        /* 20230717 - KBY, RFS23-1850 - Select PO's for forward-facing-only locations only  */
                      LEFT OUTER JOIN DEV.${FSA_PROD_SCHEMA}."OPEN_PO_TRACKED" OP_FWD
                        ON  SO."ITEM_ID_BY_TRANSACTION_TYPE" =  OP_FWD."ITEM_ID" 
                        AND SO."QTY_ORDERED"                 <= OP_FWD."QUANTITY_TO_BE_RECEIVED"
                        AND OP_FWD.IS_FWD_LOCATION                                                  -- TRUE only if a forward-facing location
                      WHERE 
                        (OP."ORDER_NUMBER" IS NOT NULL OR OP_FWD."ORDER_NUMBER" IS NOT NULL)
                        AND "PO_ID" = ` + PO_ID + `) 
                    SELECT * EXCLUDE (ALL_PO_ORDER_NUMBER,ALL_PO_QUANTITY_TO_BE_RECEIVED,ALL_PO_RECEIVE_BY_DATE,FWD_PO_ORDER_NUMBER,FWD_PO_QUANTITY_TO_BE_RECEIVED,FWD_PO_RECEIVE_BY_DATE)
                      FROM CTE_PO_ASSIGN
                      WHERE "PO_ORDER_NUMBER" IS NOT NULL
                      ORDER BY "ITEM_ID", "ROW_NO", IFNULL("PO_ID", 0)`;

    snowflake.execute({sqlText: foo_sql})

    var open_po_sql = `CREATE OR REPLACE TABLE DEV.${FSA_PROD_SCHEMA}.OPEN_PO_TRACKED AS
                       SELECT DISTINCT OP.PO_ITEM_ID
                       	             , OP.ITEM_ID
                                     , OP.ITEM_ID_C
                                     , OP.ASSEMBLY_ITEM_ID
                                     , OP.ORDER_NUMBER
                                     , OP."OG_QUANTITY_TO_BE_RECEIVED"
                                     , IFF(SO."PO_QUANTITY_REMAINING" IS NOT NULL, SO."PO_QUANTITY_REMAINING", OP."QUANTITY_TO_BE_RECEIVED") AS "QUANTITY_TO_BE_RECEIVED"
                                     , OP.NS_RECEIVE_BY_DATE
                                     , OP.PO_ROW_NO
                                     , OP.IS_FWD_LOCATION
                       FROM DEV.${FSA_PROD_SCHEMA}."OPEN_PO_TRACKED" OP
                       LEFT OUTER JOIN DEV.${FSA_PROD_SCHEMA}."DEMAND_ASSIGNMENT_TRACKED" SO
                       	 ON  SO."ITEM_ID_BY_TRANSACTION_TYPE" = OP."ITEM_ID" 
                         AND SO."PO_ORDER_NUMBER"             = OP."ORDER_NUMBER"
                         AND SO."PO_RECEIVE_BY_DATE"          = OP."NS_RECEIVE_BY_DATE"`;

    snowflake.execute({sqlText: open_po_sql})

    
    var good_demand_sql = `CREATE OR REPLACE TABLE DEV.${FSA_PROD_SCHEMA}.ASSIGNED_DEMAND AS
                           SELECT *
                           FROM (SELECT * FROM DEV.${FSA_PROD_SCHEMA}.ASSIGNED_DEMAND
                                 UNION
                                 SELECT * FROM DEV.${FSA_PROD_SCHEMA}.DEMAND_ASSIGNMENT_TRACKED)
                           ORDER BY ITEM_ID, ROW_NO`;

    snowflake.execute({sqlText: good_demand_sql});

    PO_ID = PO_ID + 1
}

return PO_ID;
$$;