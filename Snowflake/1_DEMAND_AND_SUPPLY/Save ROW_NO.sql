-- Update HASH_VALUEs for existing DEMAND PO's
--    and insert any new DEMAND PO's, with their ROW_NO
MERGE INTO DEV.${FSA_PROD_SCHEMA}.DEMAND_PREV_ASSIGNED t1
    USING DEV.${FSA_PROD_SCHEMA}.DEMAND_PO t2
        ON t1.pk_id = t2.pk_id
    WHEN matched AND t1.HASH_VALUE != t2.HASH_VALUE 
      	THEN UPDATE SET t1.HASH_VALUE = t2.HASH_VALUE, t1.LAST_MODIFIED = t2.INSERT_DATE
    WHEN NOT matched 
    	THEN INSERT VALUES
          (t2.PK_ID,
            t2.UNIQUE_KEY,
            t2.COMPONENT_ITEM_ID,
           	t2.ORDER_NUMBER,
            t2.NS_LINE_NUMBER,
            t2.HASH_VALUE,
            NULL,			-- HASH_FSA_OUTPUT
            t2.INSERT_DATE,
            t2.INSERT_DATE, -- LAST_MODIFIED
            t2.ROW_NO,
            NULL::DATE,		-- PREV_AVAIL_DATE
            NULL::DATE,     -- PREV_CAPPING_DDA
            NULL::DATE,     -- ORIG_CAP_DDA
            NULL::NUMBER,   -- PO_INDICATOR ,
            NULL::NUMBER,   -- PO_INDICATOR_ASSIGN,
            NULL::TEXT,     -- PO_ORDER_NUMBER,
            NULL::DATE,     -- PO_RECEIVE_BY_DATE DATE
              /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment */
            t2.FR_PREV_DAYS -- FR_PREV_DAYS
            );

-- Update ROW_NOs for all the new DEMAND_PO's
MERGE INTO DEV.${FSA_PROD_SCHEMA}.DEMAND_PREV_ASSIGNED t1
    USING DEV.${FSA_PROD_SCHEMA}.SEQ_DEMAND_PO t2
        ON t1.pk_id = t2.pk_id
    WHEN matched AND t1.ROW_NO IS NULL 
    	THEN UPDATE SET t1.ROW_NO = t2.ROW_NO;
