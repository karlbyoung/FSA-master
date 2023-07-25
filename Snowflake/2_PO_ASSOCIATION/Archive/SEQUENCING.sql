CREATE OR REPLACE PROCEDURE DEV.BUSINESS_OPERATIONS.UPDATE_ASSIGN_PO()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

/*  SET VARIABLES --------------------------- */
    var MAX_PO_ID;      /* MAX PO_ID FOR LOOP */
    var PO_ID = 1;      /* PO_ID COUNTER      */

var demand_temp_stmt = `CREATE OR REPLACE TABLE DEV.BUSINESS_OPERATIONS.UNASSIGNED_DEMAND AS
                        SELECT ROW_NUMBER() OVER (PARTITION BY ITEM_ID ORDER BY ROW_NO, ID) AS PO_ID
                             , so.*
                             , SIGN(so.REMAINING_QTY_ON_HAND) AS PO_INDICATOR
                             , NULL AS PO_UPDATE_DATETIME
                             , NULL AS SUM_ITEM_ROLLUP
                        FROM DEV.BUSINESS_OPERATIONS.SEQ_DEMAND_PO so
                        WHERE SIGN(so.REMAINING_QTY_ON_HAND) = -1
                        ORDER BY ITEM_ID, ROW_NO`;
snowflake.execute({sqlText: demand_temp_stmt});


snowflake.execute({sqlText: `CREATE OR REPLACE TABLE DEV.BUSINESS_OPERATIONS.ASSIGNED_DEMAND AS
                             SELECT NULL AS PO_ID,
                                    *
                                  , SIGN(REMAINING_QTY_ON_HAND) AS PO_INDICATOR
                                  , NULL AS PO_UPDATE_DATETIME
                                  , NULL AS SUM_ITEM_ROLLUP
                                  , NULL AS PO_ORDER_NUMBER
                                  , NULL AS PO_QUANTITY_TO_BE_RECEIVED
                                  , NULL AS PO_QUANTITY_REMAINING
                                  , NULL AS PO_RECEIVE_BY_DATE
                             FROM DEV.BUSINESS_OPERATIONS.SEQ_DEMAND_PO
                             WHERE SIGN(REMAINING_QTY_ON_HAND) != -1
                             ORDER BY ITEM_ID, ROW_NO`});

snowflake.execute({sqlText: `CREATE OR REPLACE TABLE DEV.BUSINESS_OPERATIONS.OPEN_PO_TRACKED AS
                             SELECT  PO_ITEM_ID
                                   , ORDER_NUMBER
                                   , QUANTITY_TO_BE_RECEIVED
                                   , NS_RECEIVE_BY_DATE
                                   , PO_ROW_NO
                             FROM DEV.BUSINESS_OPERATIONS.OPEN_PO_TRACKED
                             UNION
                             SELECT DISTINCT   PO_ITEM_ID
                                             , ORDER_NUMBER
                                             , QUANTITY_TO_BE_RECEIVED
                                             , NS_RECEIVE_BY_DATE
                                             , PO_ROW_NO
                             FROM DEV.BUSINESS_OPERATIONS.OPEN_PO;`});

var max_row_cmd = `SELECT MAX(PO_ID)
                   FROM DEV.BUSINESS_OPERATIONS.UNASSIGNED_DEMAND;`;
    
var x = snowflake.execute({sqlText: max_row_cmd});
x.next();
MAX_PO_ID = x.getColumnValue(1);

while (PO_ID <= MAX_PO_ID) {

    var dat_sql = `CREATE OR REPLACE TABLE DEV.BUSINESS_OPERATIONS.DEMAND_ASSIGNMENT_TRACKED AS
                   SELECT DISTINCT
                        SO.PO_ID
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
                       ,SO.TRANSACTION_ID
                       ,SO.UNIQUE_KEY
                       ,SO.PO_SLIPPAGE
                       ,SO.ITEM_ROW_NO
                       ,SO.SOURCE_TYPE
                       --,SO.SOURCE_TYPE_DAILY
                       ,SO.COMPONENT_ITEM
                       ,SO.COMPONENT_ITEM_ID
                       ,SO.SOURCE_LOAD_DATE
                       ,SO.PO_INDICATOR
                       ,CURRENT_DATE AS "PO_UPDATE_DATETIME"
                       ,NULL AS SUM_ITEM_ROLLUP
                       ,FIRST_VALUE(OP.ORDER_NUMBER) OVER (PARTITION BY OP.PO_ITEM_ID ORDER BY OP.NS_RECEIVE_BY_DATE, OP.ORDER_NUMBER) AS "PO_ORDER_NUMBER"
                       ,FIRST_VALUE(OP.QUANTITY_TO_BE_RECEIVED) OVER (PARTITION BY OP.PO_ITEM_ID ORDER BY OP.NS_RECEIVE_BY_DATE, OP.ORDER_NUMBER) AS "PO_QUANTITY_TO_BE_RECEIVED"
                       ,(PO_QUANTITY_TO_BE_RECEIVED - SO.QTY_ORDERED) AS PO_QUANTITY_REMAINING
                       ,FIRST_VALUE(OP.NS_RECEIVE_BY_DATE) OVER (PARTITION BY OP.PO_ITEM_ID ORDER BY OP.NS_RECEIVE_BY_DATE, OP.ORDER_NUMBER) AS "PO_RECEIVE_BY_DATE"
                   FROM DEV.BUSINESS_OPERATIONS.UNASSIGNED_DEMAND SO
                   LEFT OUTER JOIN DEV.BUSINESS_OPERATIONS.OPEN_PO_TRACKED OP
                     ON IFF(SO.TRANSACTION_TYPE = 'Assembly', SO.COMPONENT_ITEM_ID, SO.ITEM_ID) = OP.PO_ITEM_ID
                    AND SO.QTY_ORDERED <= OP.QUANTITY_TO_BE_RECEIVED
                   WHERE OP.ORDER_NUMBER IS NOT NULL AND PO_ID = ` + PO_ID + ` ORDER BY SO.ITEM_ID, SO.ROW_NO, IFNULL(SO.PO_ID, 0)`;

    snowflake.execute({sqlText: dat_sql})

    var open_po_sql = `CREATE OR REPLACE TABLE DEV.BUSINESS_OPERATIONS.OPEN_PO_TRACKED AS
                       SELECT DISTINCT OP.PO_ITEM_ID
                            , OP.ORDER_NUMBER
                            , IFF(CF.PO_QUANTITY_REMAINING IS NOT NULL, CF.PO_QUANTITY_REMAINING, OP.QUANTITY_TO_BE_RECEIVED) AS QUANTITY_TO_BE_RECEIVED
                            , OP.NS_RECEIVE_BY_DATE
                            , OP.PO_ROW_NO
                       FROM DEV.BUSINESS_OPERATIONS.OPEN_PO_TRACKED OP
                       LEFT OUTER JOIN DEV.BUSINESS_OPERATIONS.DEMAND_ASSIGNMENT_TRACKED CF
                         ON CF.ITEM_ID = OP.PO_ITEM_ID
                        AND CF.PO_ORDER_NUMBER = OP.ORDER_NUMBER
                       ORDER BY PO_ITEM_ID, NS_RECEIVE_BY_DATE, PO_ROW_NO`;

    snowflake.execute({sqlText: open_po_sql})

    var assigned_demand_sql = `CREATE OR REPLACE TABLE DEV.BUSINESS_OPERATIONS.ASSIGNED_DEMAND AS
                               SELECT *
                               FROM (SELECT * FROM DEV.BUSINESS_OPERATIONS.ASSIGNED_DEMAND
                                     UNION
                                     SELECT * FROM DEV.BUSINESS_OPERATIONS.DEMAND_ASSIGNMENT_TRACKED)
                               ORDER BY ITEM_ID, ROW_NO`;

    snowflake.execute({sqlText: assigned_demand_sql});

    PO_ID = PO_ID + 1
}

return 'success'
$$;