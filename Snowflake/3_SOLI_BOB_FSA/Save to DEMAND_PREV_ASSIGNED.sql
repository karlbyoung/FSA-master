-- Update fields in DEMAND_PREV_ASSIGNED from FSA
UPDATE DEV.${FSA_PROD_SCHEMA}.DEMAND_PREV_ASSIGNED t1
	SET t1.ORIG_CAP_DDA = t2.ORIG_CAP_DDA,
        t1.PO_ORDER_NUMBER = t2.PO_ORDER_NUMBER,
        t1.PO_RECEIVE_BY_DATE = t2.PO_RECEIVE_BY_DATE,
        t1.PO_INDICATOR = t2.PO_INDICATOR,
        t1.PO_INDICATOR_ASSIGN = t2.PO_INDICATOR_ASSIGN,
        t1.PREV_AVAIL_DATE = t2.AVAIL_DATE,
        t1.PREV_CAPPING_DDA = t2.CAPPING_DDA,
        t1.HASH_FSA_OUTPUT = t2.HASH_FSA_OUTPUT,
        t1.LAST_MODIFIED = t2.SOURCE_LOAD_DATE,
        /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment */ 
        /* 20230825 - KBY, RSF23-2625 - moved to production as bug fix */ 
        t1.FR_PREV_DAYS = t2.FR_PREV_DAYS
    FROM (SELECT DISTINCT 
            PK_ID,
            PO_ORDER_NUMBER,
            PO_RECEIVE_BY_DATE,
            PO_INDICATOR,
            PO_INDICATOR_ASSIGN,
            CAPPING_DDA,
            ORIG_CAP_DDA,
          	AVAIL_DATE,
          	SOURCE_LOAD_DATE,
            /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment */
            /* 20230825 - KBY, RSF23-2625 - moved to production as bug fix */ 
            FR_PREV_DAYS,
            HASH(AVAIL_DATE,CAPPING_DDA,PO_ORDER_NUMBER,PO_RECEIVE_BY_DATE,PO_INDICATOR,PO_INDICATOR_ASSIGN,ITEM_ID) HASH_FSA_OUTPUT
        FROM DEV.${FSA_PROD_SCHEMA}.FSA) t2
    WHERE t1.PK_ID = t2.PK_ID
;
