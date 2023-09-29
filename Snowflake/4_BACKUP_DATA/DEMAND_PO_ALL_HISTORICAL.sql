/* 2023-07-25, KBY, expand Historical to cover all columns */
INSERT INTO DEV.${vj_fsa_schema}.DEMAND_PO_ALL_HISTORICAL
SELECT 
    FK_ID, 
    ORDER_NUMBER, 
    UNIQUE_KEY, 
    DDA, 
    DDA_MODIFIED, 
    ORIGINAL_DDA, 
    SEQUENCING_DDA, 
    TRANSACTION_ID, 
    LINE_ID, 
    TRANSACTION_TYPE, 
    TYPE_NAME, 
    TOTAL_AMT, 
    PRIORITY_LEVEL, 
    NS_LINE_NUMBER, 
    ITEM, 
    ITEM_ID, 
    COMPONENT_ITEM_ID, 
    COMPONENT_ITEM, 
    QUANTITY, 
    TOTAL_AVAIL_QTY, 
    TOTAL_AVAIL_QTY_FWD, 
    TOTAL_AVAIL_QTY_NONFWD, 
    QTY_ORDERED, 
    COMPONENT_QTY_ORDERED, 
    LOCATION, 
    SOURCE_TYPE, 
    IS_ASSEMBLY_COMPONENT, 
    CREATE_DATE, 
    PO_SLIPPAGE, 
    HASH_VALUE, 
    /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment */
    FR_PREV_DAYS,
    /* 20230920 - KBY, RFS23-2696 Include FSA_COMPLETE */
    FSA_COMPLETE,
    FSA_LOAD_STATUS, 
    INSERT_DATE, 
    ID, 
    PK_ID, 
    CURRENT_TIMESTAMP() AS FSA_INSERT_DATE,
    TRUE AS IS_VALID
FROM DEV.${vj_fsa_schema}.DEMAND_PO_ALL;


/*
--    20230920 - KBY, RFS23-2696 Include FSA_COMPLETE
--    Expand DEMAND_PO_ALL_HISTORICAL to include FSA_COMPLETE
CREATE OR REPLACE TABLE DEV.NETSUITE2_FSA.DEMAND_PO_ALL_HISTORICAL_NEW AS
SELECT 
    FK_ID, 
    ORDER_NUMBER, 
    UNIQUE_KEY, 
    DDA, 
    DDA_MODIFIED, 
    ORIGINAL_DDA, 
    SEQUENCING_DDA, 
    TRANSACTION_ID, 
    LINE_ID, 
    TRANSACTION_TYPE, 
    TYPE_NAME, 
    TOTAL_AMT, 
    PRIORITY_LEVEL, 
    NS_LINE_NUMBER, 
    ITEM, 
    ITEM_ID, 
    COMPONENT_ITEM_ID, 
    COMPONENT_ITEM, 
    QUANTITY, 
    TOTAL_AVAIL_QTY, 
    TOTAL_AVAIL_QTY_FWD, 
    TOTAL_AVAIL_QTY_NONFWD, 
    QTY_ORDERED, 
    COMPONENT_QTY_ORDERED, 
    LOCATION, 
    SOURCE_TYPE, 
    IS_ASSEMBLY_COMPONENT, 
    CREATE_DATE, 
    PO_SLIPPAGE, 
    HASH_VALUE, 
    FR_PREV_DAYS,
    CASE
        WHEN SOURCE_TYPE != 'OpenSO' THEN NULL 
        WHEN 'FSA_LOAD_STATUS' = 'NEW' THEN 'F'
        ELSE 'T'
    END AS FSA_COMPLETE,
    FSA_LOAD_STATUS, 
    INSERT_DATE, 
    ID, 
    PK_ID, 
    FSA_INSERT_DATE,
    IS_VALID
FROM DEV.NETSUITE2_FSA.DEMAND_PO_ALL_HISTORICAL;
*/