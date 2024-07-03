/* 20240516 - KBY, RFS-5213 Assign assembly orders with any Assembly Transfer Orders before checking inventory */
CREATE OR REPLACE PROCEDURE DEV.${vj_fsa_schema}.ASSIGN_ASM_XFER()
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
    ALTER SESSION SET QUERY_TAG='FSA_ASM';
    LET cur_run_date DATE := CURRENT_DATE();
    SELECT MAX(INSERT_DATE) INTO :cur_run_date FROM DEV.${vj_fsa_schema}.DEMAND_PO;

    -- set up quantity tracker for Assembly XFER PO's
    CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}.ASM_OPEN_PO_TRACKED AS
        SELECT PO_ITEM_ID
            , ITEM_ID
            , ITEM_ID_C
            , ASSEMBLY_ITEM_ID
            , ORDER_NUMBER
            , QUANTITY_TO_BE_RECEIVED                                         AS OG_QUANTITY_TO_BE_RECEIVED
            , QUANTITY_TO_BE_RECEIVED
            , NS_RECEIVE_BY_DATE
            , PO_ROW_NO
            , (LOCATION NOT IN ('Booksource', 'Continuum') )                  AS IS_FWD_LOCATION
            , PO_ITEM_TYPE
            , PK_ID
        FROM DEV.${vj_fsa_schema}.OPEN_PO_ALL
        WHERE PO_ITEM_TYPE = 'Assembly Transfer Order'
        ORDER BY PO_ITEM_ID, PO_ROW_NO, NS_RECEIVE_BY_DATE, ORDER_NUMBER;

    -- create empty table for Assemblies that get assigned
    CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}.ASM_ASSIGNED_DEMAND (
      PO_ID NUMBER(18,0),
      FSA_LOAD_STATUS VARCHAR(16777216),
      ID NUMBER(18,0),
      ITEM_ID NUMBER(38,0),
      ORIGINAL_DDA DATE,
      SEQUENCING_DDA DATE,
      ORDER_NUMBER VARCHAR(360),
      QTY_ORDERED NUMBER(38,0),
      DDA DATE,
      ITEM VARCHAR(4400),
      LOCATION VARCHAR(480),
      NS_LINE_NUMBER VARCHAR(16777216),
      TRANSACTION_TYPE VARCHAR(16777216),
      TYPE_NAME VARCHAR(480),
      TRANSACTION_ID VARCHAR(16777216),
      LINE_ID VARCHAR(16777216),
      UNIQUE_KEY NUMBER(38,0),
      PO_SLIPPAGE BOOLEAN,
      SOURCE_TYPE VARCHAR(8),
      COMPONENT_ITEM VARCHAR(4400),
      COMPONENT_ITEM_ID NUMBER(38,0),
      PK_ID VARCHAR(16777216),
      IS_ASSEMBLY_COMPONENT BOOLEAN,
      CREATE_DATE TIMESTAMP_TZ(9),
      FR_PREV_DAYS NUMBER(38,0),
      FSA_COMPLETE VARCHAR(16777216),
      PO_UPDATE_DATETIME DATE,
      ASM_PO_ORDER_NUMBER VARCHAR(360),
      ASM_PO_QUANTITY_TO_BE_RECEIVED NUMBER(38,0),
      ASM_PO_RECEIVE_BY_DATE DATE,
      PO_QUANTITY_REMAINING NUMBER(38,0),
      OG_QUANTITY_TO_BE_RECEIVED NUMBER(38,0)
    );

    -- Get list of  (unassigned) Assembly Orders
    CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}.ASM_UNASSIGNED AS (SELECT 
        ROW_NUMBER() OVER (PARTITION BY COMPONENT_ITEM_ID ORDER BY ROW_NO, ID) AS PO_ID
        , so.*
      FROM DEV.${vj_fsa_schema}.DEMAND_PO so
        WHERE IS_ASSEMBLY_COMPONENT
        ORDER BY PO_ID);

    -- Loop over PO_ID's (created across ROW_NO's), assign assemblies at that ID for each XFER available
    SELECT MAX(PO_ID) into :MAX_PO_ID FROM DEV.${vj_fsa_schema}.ASM_UNASSIGNED;
    FOR PO_ID in 1 to MAX_PO_ID do
      -- get results for this PO_ID
      CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}."ASM_DEMAND_ASSIGNMENT_TRACKED" AS
        SELECT DISTINCT 
          SO.PO_ID
          ,SO.FSA_LOAD_STATUS::TEXT AS "FSA_LOAD_STATUS"
          ,SO.ID
          ,SO.ITEM_ID
          ,SO.ORIGINAL_DDA
          ,SO.SEQUENCING_DDA
          ,SO.ORDER_NUMBER
          ,SO.QTY_ORDERED
          ,SO.DDA
          ,SO.ITEM
          ,SO.LOCATION
          ,SO.NS_LINE_NUMBER
          ,SO.TRANSACTION_TYPE
          ,SO.TYPE_NAME
          ,SO.TRANSACTION_ID
          ,SO.LINE_ID
          ,SO.UNIQUE_KEY
          ,SO.PO_SLIPPAGE
          ,SO.SOURCE_TYPE
          ,SO.COMPONENT_ITEM
          ,SO.COMPONENT_ITEM_ID
          ,SO.PK_ID
          ,SO.IS_ASSEMBLY_COMPONENT
          ,SO.CREATE_DATE
          ,SO.FR_PREV_DAYS
          ,SO.FSA_COMPLETE
          ,:cur_run_date AS "PO_UPDATE_DATETIME"
          ,FIRST_VALUE(po."ORDER_NUMBER")            
              OVER (PARTITION BY "COMPONENT_ITEM_ID" ORDER BY SO."ROW_NO", po."NS_RECEIVE_BY_DATE", po."ORDER_NUMBER")  AS "ASM_PO_ORDER_NUMBER"
          ,FIRST_VALUE(po."QUANTITY_TO_BE_RECEIVED") 
              OVER (PARTITION BY "COMPONENT_ITEM_ID" ORDER BY SO."ROW_NO", po."NS_RECEIVE_BY_DATE", po."ORDER_NUMBER")  AS "ASM_PO_QUANTITY_TO_BE_RECEIVED"
          ,FIRST_VALUE(po."NS_RECEIVE_BY_DATE")      
              OVER (PARTITION BY "COMPONENT_ITEM_ID" ORDER BY SO."ROW_NO", po."NS_RECEIVE_BY_DATE", po."ORDER_NUMBER")  AS "ASM_PO_RECEIVE_BY_DATE"
          /* 20240619 - KBY, Bug Fix - Handle first xfer order when multiple available */ 
          ,(ASM_PO_QUANTITY_TO_BE_RECEIVED - SO."QTY_ORDERED" )                                                         AS "PO_QUANTITY_REMAINING"
          ,FIRST_VALUE(po.OG_QUANTITY_TO_BE_RECEIVED)      
              OVER (PARTITION BY "COMPONENT_ITEM_ID" ORDER BY SO."ROW_NO", po."NS_RECEIVE_BY_DATE", po."ORDER_NUMBER")  AS "OG_QUANTITY_TO_BE_RECEIVED"
        FROM DEV.${vj_fsa_schema}.ASM_UNASSIGNED so
        JOIN DEV.${vj_fsa_schema}.ASM_OPEN_PO_TRACKED po
          ON SO.COMPONENT_ITEM_ID = PO.ITEM_ID
          AND SO.QTY_ORDERED <= PO.QUANTITY_TO_BE_RECEIVED
        WHERE po."ORDER_NUMBER" IS NOT NULL 
          AND "PO_ID" = :PO_ID;

      -- save off the tracker for Assembly XFER orders, with new quantity to be received
      INSERT OVERWRITE INTO DEV.${vj_fsa_schema}.ASM_OPEN_PO_TRACKED
        SELECT DISTINCT 
          po.PO_ITEM_ID
          , po.ITEM_ID
          , po.ITEM_ID_C
          , po.ASSEMBLY_ITEM_ID
          , po.ORDER_NUMBER
          , po.OG_QUANTITY_TO_BE_RECEIVED
          , IFF(SO.PO_QUANTITY_REMAINING IS NOT NULL, SO.PO_QUANTITY_REMAINING, po.QUANTITY_TO_BE_RECEIVED) AS QUANTITY_TO_BE_RECEIVED
          , po.NS_RECEIVE_BY_DATE
          , po.PO_ROW_NO
          , po.IS_FWD_LOCATION
          , PO_ITEM_TYPE
          , po.PK_ID
        FROM DEV.${vj_fsa_schema}.ASM_OPEN_PO_TRACKED po
        LEFT JOIN DEV.${vj_fsa_schema}.ASM_DEMAND_ASSIGNMENT_TRACKED SO
          ON  SO.component_item_id           = po.ITEM_ID
          AND SO.ASM_PO_ORDER_NUMBER         = po.order_number
          AND SO.asm_po_receive_by_date      = po.ns_receive_by_date;

      -- insert the newly assigned orders for this PO_ID
      INSERT INTO DEV.${vj_fsa_schema}.ASM_ASSIGNED_DEMAND
        SELECT * 
        FROM DEV.${vj_fsa_schema}.ASM_DEMAND_ASSIGNMENT_TRACKED;

    END FOR;
    ALTER SESSION UNSET QUERY_TAG;
    return 'Success: '||MAX_PO_ID::text||' iterations';
  END;
$$;