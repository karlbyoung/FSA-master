CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}."UNASSIGNED_DEMAND" AS
	SELECT ROW_NUMBER() OVER (PARTITION BY ITEM_ID_BY_TRANSACTION_TYPE ORDER BY ROW_NO, ID) AS "PO_ID"
	     , so.*
          /* 20230608 - KBY, HyperCare 123 - When OpenSO is already assigned (has valid Location), set PO_INDICATOR to 1 as indication */
	     , IFF(so.IS_ALREADY_ASSIGNED,1,SIGN(so.REMAINING_QTY_ON_HAND))::NUMBER   AS "PO_INDICATOR"
	     , NULL                                                           AS "PO_UPDATE_DATETIME"
	FROM DEV.${vj_fsa_schema}.SEQ_DEMAND_PO so
	WHERE PO_INDICATOR = -1
	ORDER BY ITEM_ID, ROW_NO;
    
CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}.ASSIGNED_DEMAND AS
    /* 20240516 - KBY, RFS-5213 - Check with already assigned Assembly Transfer orders */
    WITH ASM AS
    (
        SELECT -1                                                            AS "PO_ID",
            seq.*
            , -1                                                             AS "PO_INDICATOR"
            , assgn.PO_UPDATE_DATETIME
            , assgn.ASM_PO_ORDER_NUMBER                                      AS PO_ORDER_NUMBER
            , assgn.ASM_PO_QUANTITY_TO_BE_RECEIVED                           AS PO_QUANTITY_TO_BE_RECEIVED
            , assgn.PO_QUANTITY_REMAINING
            , assgn.ASM_PO_RECEIVE_BY_DATE                                   AS "PO_RECEIVE_BY_DATE"
            , assgn.OG_QUANTITY_TO_BE_RECEIVED
        FROM DEV.${vj_fsa_schema}.SEQ_DEMAND_PO seq
        LEFT JOIN DEV.${vj_fsa_schema}.ASM_ASSIGNED_DEMAND assgn
            ON seq.PK_ID = assgn.PK_ID
        WHERE assgn.pk_id IS NOT NULL
        ORDER BY seq.ITEM_ID, seq.ROW_NO
    )
    , NONASM AS
    (
        SELECT NULL::NUMBER                                                  AS "PO_ID",
            seq.*
            /* 20230608 - KBY, HyperCare 123 - When OpenSO is already assigned (has valid Location), set PO_INDICATOR to 1 as indication */
            , IFF(seq.IS_ALREADY_ASSIGNED,1,SIGN(seq.REMAINING_QTY_ON_HAND))::NUMBER AS "PO_INDICATOR"
            , NULL                                                           AS "PO_UPDATE_DATETIME"
            , NULL                                                           AS "PO_ORDER_NUMBER"
            , NULL                                                           AS "PO_QUANTITY_TO_BE_RECEIVED"
            , NULL                                                           AS "PO_QUANTITY_REMAINING"
            , NULL                                                           AS "PO_RECEIVE_BY_DATE"
            , NULL                                                           AS "OG_QUANTITY_TO_BE_RECEIVED"
        FROM DEV.${vj_fsa_schema}.SEQ_DEMAND_PO seq
        LEFT JOIN DEV.${vj_fsa_schema}.ASM_ASSIGNED_DEMAND assgn
            ON seq.PK_ID = assgn.PK_ID
        WHERE assgn.pk_id IS NULL
            AND PO_INDICATOR != -1
        ORDER BY seq.ITEM_ID, seq.ROW_NO
    )
    , FINAL AS
    (
        SELECT * from NONASM
        UNION
        SELECT * FROM ASM
    )
    SELECT * 
    FROM FINAL
        ORDER BY ITEM_ID, ROW_NO;


CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}.OPEN_PO_TRACKED AS
    WITH ASM AS
    (SELECT PO_ITEM_ID
            , ITEM_ID
            , ITEM_ID_C
            , ASSEMBLY_ITEM_ID
            , ORDER_NUMBER
            , OG_QUANTITY_TO_BE_RECEIVED
            , QUANTITY_TO_BE_RECEIVED
            , NS_RECEIVE_BY_DATE
            , PO_ROW_NO
            , IS_FWD_LOCATION
            , PO_ITEM_TYPE
            , PK_ID
        FROM DEV.${vj_fsa_schema}.ASM_OPEN_PO_TRACKED
    )
    , NONASM AS
    (SELECT PO_ITEM_ID
            , ITEM_ID
            , ITEM_ID_C
            , ASSEMBLY_ITEM_ID
            , ORDER_NUMBER
            , QUANTITY_TO_BE_RECEIVED                                         AS OG_QUANTITY_TO_BE_RECEIVED
            , QUANTITY_TO_BE_RECEIVED
            , NS_RECEIVE_BY_DATE
            , PO_ROW_NO
            /* 20230717 - KBY, RFS23-1850 - Distinguish forward-facing locations only */
            , (LOCATION NOT IN ('Booksource', 'Continuum') )                  AS IS_FWD_LOCATION
            , PO_ITEM_TYPE
            , PK_ID
        FROM DEV.${vj_fsa_schema}.OPEN_PO_ALL
        WHERE PK_ID NOT IN (select PK_ID from ASM)
    )
    , FINAL AS
    (
        SELECT * FROM ASM
        UNION
        SELECT * FROM NONASM
    )
    SELECT * FROM FINAL
    ORDER BY PO_ITEM_ID, PO_ROW_NO, NS_RECEIVE_BY_DATE, ORDER_NUMBER;