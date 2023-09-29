INSERT INTO DEV.${vj_fsa_schema}.DEMAND_PREV_ASSIGNED_HISTORICAL
    SELECT 
        PK_ID, 
        UNIQUE_KEY, 
        COMPONENT_ITEM_ID, 
        ORDER_NUMBER, 
        NS_LINE_NUMBER, 
        HASH_VALUE, 
        HASH_FSA_OUTPUT, 
        INSERT_DATE, 
        LAST_MODIFIED, 
        ROW_NO, 
        PREV_AVAIL_DATE, 
        PREV_CAPPING_DDA, 
        ORIG_CAP_DDA, 
        PO_INDICATOR, 
        PO_INDICATOR_ASSIGN, 
        PO_ORDER_NUMBER, 
        PO_RECEIVE_BY_DATE, 
        /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment */
        FR_PREV_DAYS,
        TRUE IS_VALID, 
        CURRENT_TIMESTAMP() FSA_INSERT_DATE
FROM DEV.${vj_fsa_schema}.DEMAND_PREV_ASSIGNED;