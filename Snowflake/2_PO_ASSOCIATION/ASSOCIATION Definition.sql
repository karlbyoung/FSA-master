CREATE OR REPLACE PROCEDURE DEV.${vj_fsa_schema}.ASSIGN_PO()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

/*  DESCRIBE VARIABLES --------------------------- */
    var MAX_PO_ID;      /* MAX PO_ID FOR LOOP */
    var PO_ID = 1;      /* PO_ID COUNTER      */
    
var crd = snowflake.execute({sqlText: `SELECT MAX(INSERT_DATE)::DATE::TEXT
                                     FROM DEV.${vj_fsa_schema}.DEMAND_PO;`});
crd.next();
CUR_RUN_DATE = crd.getColumnValue(1);

CUR_RUN_DATE = `'` + CUR_RUN_DATE + `'`;



var x = snowflake.execute({sqlText: `SELECT MAX(PO_ID)
                                     FROM DEV.${vj_fsa_schema}.UNASSIGNED_DEMAND;`});
x.next();
MAX_PO_ID = x.getColumnValue(1);

/* 20230809 - KBY, RFS23-1869 - Assignment code improvement  */
/*    initial assignment for DEMAND_ASSIGNMENT_TRACKED creates table, subsequent assignments use INSERT OVERWRITE */
var insertOrCreateQuery = `CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}."DEMAND_ASSIGNMENT_TRACKED" AS `;
while (PO_ID <= MAX_PO_ID) {
    
    var foo_sql = insertOrCreateQuery + 
                    `WITH CTE_PO_ASSIGN AS 
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
                          /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment */
                          ,SO.FR_PREV_DAYS
                          /* 20230920 - KBY, RFS23-2696 Include FSA_COMPLETE */
                          ,SO.FSA_COMPLETE
                          ,SO.PO_INDICATOR
                          ,` + CUR_RUN_DATE + ` AS "PO_UPDATE_DATETIME"
                          /* 20230717 - KBY, RFS23-1850 - Select PO's for forward-facing-only locations only and all locations (including non-forward-facing) */
                          ,FIRST_VALUE(OP."ORDER_NUMBER")            
                            OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP."NS_RECEIVE_BY_DATE", OP."ORDER_NUMBER")          AS "JUST_PO_ORDER_NUMBER"
                          ,FIRST_VALUE(OP."QUANTITY_TO_BE_RECEIVED") 
                            OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP."NS_RECEIVE_BY_DATE", OP."ORDER_NUMBER")          AS "JUST_PO_QUANTITY_TO_BE_RECEIVED"
                          ,FIRST_VALUE(OP."NS_RECEIVE_BY_DATE")      
                            OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP."NS_RECEIVE_BY_DATE", OP."ORDER_NUMBER")          AS "JUST_PO_RECEIVE_BY_DATE"
                          ,FIRST_VALUE(OP_FWD."ORDER_NUMBER")            
                            OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP_FWD."NS_RECEIVE_BY_DATE", OP_FWD."ORDER_NUMBER")  AS "FWD_PO_ORDER_NUMBER"
                          ,FIRST_VALUE(OP_FWD."QUANTITY_TO_BE_RECEIVED") 
                            OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP_FWD."NS_RECEIVE_BY_DATE", OP_FWD."ORDER_NUMBER")  AS "FWD_PO_QUANTITY_TO_BE_RECEIVED"
                          ,FIRST_VALUE(OP_FWD."NS_RECEIVE_BY_DATE")      
                            OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP_FWD."NS_RECEIVE_BY_DATE", OP_FWD."ORDER_NUMBER")  AS "FWD_PO_RECEIVE_BY_DATE"
                          ,FIRST_VALUE(OP_ASM."ORDER_NUMBER")            
                            OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP_ASM."NS_RECEIVE_BY_DATE", OP_ASM."ORDER_NUMBER")  AS "ASM_PO_ORDER_NUMBER"
                          ,FIRST_VALUE(OP_ASM."QUANTITY_TO_BE_RECEIVED") 
                            OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP_ASM."NS_RECEIVE_BY_DATE", OP_ASM."ORDER_NUMBER")  AS "ASM_PO_QUANTITY_TO_BE_RECEIVED"
                          ,FIRST_VALUE(OP_ASM."NS_RECEIVE_BY_DATE")      
                            OVER (PARTITION BY "ITEM_ID_BY_TRANSACTION_TYPE" ORDER BY SO."ROW_NO", OP_ASM."NS_RECEIVE_BY_DATE", OP_ASM."ORDER_NUMBER")  AS "ASM_PO_RECEIVE_BY_DATE"
                          ,CASE WHEN SO.IS_ASSEMBLY_COMPONENT  THEN ASM_PO_ORDER_NUMBER
                                WHEN SO.SOURCE_TYPE = 'OpenSO' THEN FWD_PO_ORDER_NUMBER
                                WHEN SO.SOURCE_TYPE = 'XFER'   THEN JUST_PO_ORDER_NUMBER
                                ELSE NULL
                           END                                                                        AS PO_ORDER_NUMBER
                          ,CASE WHEN SO.IS_ASSEMBLY_COMPONENT  THEN ASM_PO_QUANTITY_TO_BE_RECEIVED
                                WHEN SO.SOURCE_TYPE = 'OpenSO' THEN FWD_PO_QUANTITY_TO_BE_RECEIVED
                                WHEN SO.SOURCE_TYPE = 'XFER'   THEN JUST_PO_QUANTITY_TO_BE_RECEIVED
                                ELSE NULL
                           END                                                                        AS PO_QUANTITY_TO_BE_RECEIVED
                          ,CASE WHEN SO.IS_ASSEMBLY_COMPONENT  THEN ASM_PO_RECEIVE_BY_DATE
                                WHEN SO.SOURCE_TYPE = 'OpenSO' THEN FWD_PO_RECEIVE_BY_DATE
                                WHEN SO.SOURCE_TYPE = 'XFER'   THEN JUST_PO_RECEIVE_BY_DATE
                                ELSE NULL
                           END                                                                        AS PO_RECEIVE_BY_DATE
                          ,CASE WHEN SO.IS_ASSEMBLY_COMPONENT  THEN OP_ASM.OG_QUANTITY_TO_BE_RECEIVED
                                WHEN SO.SOURCE_TYPE = 'OpenSO' THEN OP_FWD.OG_QUANTITY_TO_BE_RECEIVED
                                WHEN SO.SOURCE_TYPE = 'XFER'   THEN OP.OG_QUANTITY_TO_BE_RECEIVED
                                ELSE NULL
                           END                                                                        AS OG_QUANTITY_TO_BE_RECEIVED
                          ,("PO_QUANTITY_TO_BE_RECEIVED" - SO."QTY_ORDERED" + IFF(SO."IS_PARTIAL_QTY",SO."AVAIL_QTY_USED",0))                           AS "PO_QUANTITY_REMAINING"
                      FROM DEV.${vj_fsa_schema}."UNASSIGNED_DEMAND" SO
                      LEFT OUTER JOIN DEV.${vj_fsa_schema}."OPEN_PO_TRACKED" OP
                        ON  SO."ITEM_ID_BY_TRANSACTION_TYPE" =  OP."ITEM_ID" 
                        AND SO."QTY_ORDERED"                 <= OP."QUANTITY_TO_BE_RECEIVED"
                        AND OP.PO_ITEM_TYPE              NOT IN ('Fulfillment Transfer Order','Assembly Transfer Order')
                        /* 20230717 - KBY, RFS23-1850 - Select PO's for forward-facing-only locations only  */
                      LEFT OUTER JOIN DEV.${vj_fsa_schema}."OPEN_PO_TRACKED" OP_FWD
                        ON  SO."ITEM_ID_BY_TRANSACTION_TYPE" =  OP_FWD."ITEM_ID" 
                        AND SO."QTY_ORDERED"                 <= OP_FWD."QUANTITY_TO_BE_RECEIVED"
                        AND OP_FWD.PO_ITEM_TYPE              IN ('Fulfillment Transfer Order','No Assembly')
                        AND OP_FWD.IS_FWD_LOCATION                                                  -- TRUE only if a forward-facing location
                      LEFT OUTER JOIN DEV.${vj_fsa_schema}."OPEN_PO_TRACKED" OP_ASM
                        ON  SO."ITEM_ID_BY_TRANSACTION_TYPE" =  OP_ASM."ITEM_ID" 
                        AND SO."QTY_ORDERED"                 <= OP_ASM."QUANTITY_TO_BE_RECEIVED"
                        AND OP_ASM.PO_ITEM_TYPE              IN ('Assembly Transfer Order','ASSEMBLY')
                      WHERE 
                        (OP.ORDER_NUMBER IS NOT NULL OR OP_FWD.ORDER_NUMBER IS NOT NULL OR OP_ASM.ORDER_NUMBER IS NOT NULL)
                        AND PO_ID = ` + PO_ID + 
                    `) 
                    SELECT * EXCLUDE (JUST_PO_ORDER_NUMBER,JUST_PO_QUANTITY_TO_BE_RECEIVED,JUST_PO_RECEIVE_BY_DATE,
                                      FWD_PO_ORDER_NUMBER,FWD_PO_QUANTITY_TO_BE_RECEIVED,FWD_PO_RECEIVE_BY_DATE,
                                      ASM_PO_ORDER_NUMBER,ASM_PO_QUANTITY_TO_BE_RECEIVED,ASM_PO_RECEIVE_BY_DATE)
                      FROM CTE_PO_ASSIGN
                      WHERE PO_ORDER_NUMBER IS NOT NULL
                      ORDER BY ITEM_ID, ROW_NO, IFNULL(PO_ID, 0)`;

    snowflake.execute({sqlText: foo_sql})

    /* 20230809 - KBY, RFS23-1869 - Assignment code improvement  */
    /*    initial assignment for DEMAND_ASSIGNMENT_TRACKED creates table, subsequent assignments use INSERT OVERWRITE */
    insertOrCreateQuery = `INSERT OVERWRITE INTO DEV.${vj_fsa_schema}."DEMAND_ASSIGNMENT_TRACKED" `;

    /* 20230809 - KBY, RFS23-1869 - Assignment code improvement, use INSERT OVERWRITE instead of creating new table  */
    var open_po_sql = `INSERT OVERWRITE INTO DEV.${vj_fsa_schema}.OPEN_PO_TRACKED
                       SELECT DISTINCT OP.PO_ITEM_ID
                       	             , OP.ITEM_ID
                                     , OP.ITEM_ID_C
                                     , OP.ASSEMBLY_ITEM_ID
                                     , OP.ORDER_NUMBER
                                     , OP.OG_QUANTITY_TO_BE_RECEIVED
                                     , IFF(SO.PO_QUANTITY_REMAINING IS NOT NULL, SO.PO_QUANTITY_REMAINING, OP.QUANTITY_TO_BE_RECEIVED) AS QUANTITY_TO_BE_RECEIVED
                                     , OP.NS_RECEIVE_BY_DATE
                                     , OP.PO_ROW_NO
                                     , OP.IS_FWD_LOCATION
                                     , PO_ITEM_TYPE
                       FROM DEV.${vj_fsa_schema}."OPEN_PO_TRACKED" OP
                       LEFT OUTER JOIN DEV.${vj_fsa_schema}."DEMAND_ASSIGNMENT_TRACKED" SO
                       	 ON  SO."ITEM_ID_BY_TRANSACTION_TYPE" = OP."ITEM_ID" 
                         AND SO."PO_ORDER_NUMBER"             = OP."ORDER_NUMBER"
                         AND SO."PO_RECEIVE_BY_DATE"          = OP."NS_RECEIVE_BY_DATE"`;

    snowflake.execute({sqlText: open_po_sql})

    
    /* 20230809 - KBY, RFS23-1869 - Assignment code improvement, use INSERT instead of creating new table  */
    var good_demand_sql = `INSERT INTO DEV.${vj_fsa_schema}.ASSIGNED_DEMAND
                        SELECT * 
                        FROM DEV.${vj_fsa_schema}.DEMAND_ASSIGNMENT_TRACKED`;

    snowflake.execute({sqlText: good_demand_sql});

    PO_ID = PO_ID + 1
}

return PO_ID;
$$;