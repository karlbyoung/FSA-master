create or replace view V_FSA_LATEST_ENTRY(
	PK_ID,
	PO_ID,
	FSA_LOAD_STATUS,
	ID,
	ITEM_ID,
	ORIGINAL_DDA,
	SEQUENCING_DDA,
	ORDER_NUMBER,
	TOTAL_AVAIL_QTY,
	QUANTITY,
	REMAINING_AVAIL_QTY,
	SUM_ROLLUP,
	PRELIM_EDD,
	ITEM,
	LOCATION,
	NS_LINE_NUMBER,
	ROW_NO,
	PRIORITY,
	SEQ,
	TRANSACTION_TYPE,
	TYPE_NAME,
	TRANSACTION_ID,
	LINE_ID,
	UNIQUE_KEY,
	PO_SLIPPAGE,
	ITEM_ROW_NO,
	SOURCE_TYPE,
	COMPONENT_ITEM,
	COMPONENT_ITEM_ID,
	ITEM_ID_BY_TRANSACTION_TYPE,
	SOURCE_LOAD_DATE,
	IS_ASSEMBLY_COMPONENT,
	CREATE_DATE,
	PO_INDICATOR,
	PO_UPDATE_DATETIME,
	PO_ORDER_NUMBER,
	PO_QUANTITY_REMAINING,
	PO_RECEIVE_BY_DATE,
	PO_INDICATOR_ASSIGN,
	PO_TOTAL_QUANTITY_TO_BE_RECEIVED,
	SHARED_ORDER_NUMBER,
	BOB_ORDER_NUMBER,
	AVAIL_DATE,
	BOB_ITEM,
	FREDD,
	BUCKET_ON_AVAIL_DATE,
	BUCKET_DATE_ON_AVAIL_DATE,
	IS_GT_15_BIZDAYS,
	IF_BUCKET1,
	IF_BUCKET2,
	IF_BUCKET3,
	IF_BUCKET4,
	IF_BUCKET5,
	IF_BUCKET6,
	IF_BUCKET7,
	IF_BUCKET8,
	IF_BUCKET9,
	IF_BUCKET10,
	IF_BUCKET11,
	IF_BUCKET12,
	IF_BUCKET13,
	IF_BUCKET14,
	FSA_UPDATED_ORIGINAL_DDA,
	ITEM_AVAIL_DATE,
	FSA_OUTPUT_STATUS,
	CAPPING_DDA,
	ORIG_CAP_DDA,
	NEW_AVAIL_DATE,
	PREV_AVAIL_DATE,
	PREV_CAPPING_DDA,
	PREV_PO_INDICATOR,
	PREV_PO_INDICATOR_ASSIGN,
	PREV_PO_ORDER_NUMBER,
	PREV_PO_RECEIVE_BY_DATE
) as
SELECT DISTINCT 
  "PK_ID", 
  LAST_VALUE("PO_ID") OVER (PARTITION BY "PK_ID" 
                            ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                     "FSA_INSERT_DATE" ASC 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PO_ID", 
  LAST_VALUE("FSA_LOAD_STATUS") OVER (PARTITION BY "PK_ID" 
                                      ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                               "FSA_INSERT_DATE" ASC 
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "FSA_LOAD_STATUS", 
  LAST_VALUE("ID") OVER (PARTITION BY "PK_ID" 
                         ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                  "FSA_INSERT_DATE" ASC 
                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "ID", 
  LAST_VALUE("ITEM_ID") OVER (PARTITION BY "PK_ID" 
                              ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                       "FSA_INSERT_DATE" ASC 
                              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "ITEM_ID", 
  LAST_VALUE("ORIGINAL_DDA") OVER (PARTITION BY "PK_ID" 
                                   ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                            "FSA_INSERT_DATE" ASC 
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "ORIGINAL_DDA", 
  LAST_VALUE("SEQUENCING_DDA") OVER (PARTITION BY "PK_ID" 
                                     ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                              "FSA_INSERT_DATE" ASC 
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "SEQUENCING_DDA", 
  LAST_VALUE("ORDER_NUMBER") OVER (PARTITION BY "PK_ID" 
                                   ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                            "FSA_INSERT_DATE" ASC 
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "ORDER_NUMBER", 
  LAST_VALUE("TOTAL_AVAIL_QTY") OVER (PARTITION BY "PK_ID" 
                                      ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                               "FSA_INSERT_DATE" ASC 
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "TOTAL_AVAIL_QTY", 
  LAST_VALUE("QUANTITY") OVER (PARTITION BY "PK_ID" 
                               ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                        "FSA_INSERT_DATE" ASC 
                               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "QUANTITY", 
  LAST_VALUE("REMAINING_AVAIL_QTY") OVER (PARTITION BY "PK_ID" 
                                          ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                   "FSA_INSERT_DATE" ASC 
                                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "REMAINING_AVAIL_QTY", 
  LAST_VALUE("SUM_ROLLUP") OVER (PARTITION BY "PK_ID" 
                                 ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                          "FSA_INSERT_DATE" ASC 
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "SUM_ROLLUP", 
  LAST_VALUE("PRELIM_EDD") OVER (PARTITION BY "PK_ID" 
                                 ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                          "FSA_INSERT_DATE" ASC 
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PRELIM_EDD", 
  LAST_VALUE("ITEM") OVER (PARTITION BY "PK_ID" 
                           ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                    "FSA_INSERT_DATE" ASC 
                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "ITEM", 
  LAST_VALUE("LOCATION") OVER (PARTITION BY "PK_ID" 
                               ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                        "FSA_INSERT_DATE" ASC 
                               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "LOCATION", 
  LAST_VALUE("NS_LINE_NUMBER") OVER (PARTITION BY "PK_ID" 
                                     ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                              "FSA_INSERT_DATE" ASC 
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "NS_LINE_NUMBER", 
  LAST_VALUE("ROW_NO") OVER (PARTITION BY "PK_ID" 
                             ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                      "FSA_INSERT_DATE" ASC 
                             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "ROW_NO", 
  LAST_VALUE("PRIORITY") OVER (PARTITION BY "PK_ID" 
                               ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                        "FSA_INSERT_DATE" ASC 
                               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PRIORITY", 
  LAST_VALUE("SEQ") OVER (PARTITION BY "PK_ID" 
                          ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                   "FSA_INSERT_DATE" ASC 
                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "SEQ", 
  LAST_VALUE("TRANSACTION_TYPE") OVER (PARTITION BY "PK_ID" 
                                       ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                "FSA_INSERT_DATE" ASC 
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "TRANSACTION_TYPE", 
  LAST_VALUE("TYPE_NAME") OVER (PARTITION BY "PK_ID" 
                                ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                         "FSA_INSERT_DATE" ASC 
                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "TYPE_NAME", 
  LAST_VALUE("TRANSACTION_ID") OVER (PARTITION BY "PK_ID" 
                                     ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                              "FSA_INSERT_DATE" ASC 
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "TRANSACTION_ID", 
  LAST_VALUE("LINE_ID") OVER (PARTITION BY "PK_ID" 
                              ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                       "FSA_INSERT_DATE" ASC 
                              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "LINE_ID", 
  LAST_VALUE("UNIQUE_KEY") OVER (PARTITION BY "PK_ID" 
                                 ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                          "FSA_INSERT_DATE" ASC 
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "UNIQUE_KEY", 
  LAST_VALUE("PO_SLIPPAGE") OVER (PARTITION BY "PK_ID" 
                                  ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                           "FSA_INSERT_DATE" ASC 
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PO_SLIPPAGE", 
  LAST_VALUE("ITEM_ROW_NO") OVER (PARTITION BY "PK_ID" 
                                  ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                           "FSA_INSERT_DATE" ASC 
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "ITEM_ROW_NO", 
  LAST_VALUE("SOURCE_TYPE") OVER (PARTITION BY "PK_ID" 
                                  ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                           "FSA_INSERT_DATE" ASC 
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "SOURCE_TYPE", 
  LAST_VALUE("COMPONENT_ITEM") OVER (PARTITION BY "PK_ID" 
                                     ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                              "FSA_INSERT_DATE" ASC 
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "COMPONENT_ITEM", 
  LAST_VALUE("COMPONENT_ITEM_ID") OVER (PARTITION BY "PK_ID" 
                                        ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                 "FSA_INSERT_DATE" ASC 
                                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "COMPONENT_ITEM_ID", 
  LAST_VALUE("ITEM_ID_BY_TRANSACTION_TYPE") OVER (PARTITION BY "PK_ID" 
                                                  ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                           "FSA_INSERT_DATE" ASC 
                                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "ITEM_ID_BY_TRANSACTION_TYPE", 
  LAST_VALUE("SOURCE_LOAD_DATE") OVER (PARTITION BY "PK_ID" 
                                       ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                "FSA_INSERT_DATE" ASC 
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "SOURCE_LOAD_DATE", 
  LAST_VALUE("IS_ASSEMBLY_COMPONENT") OVER (PARTITION BY "PK_ID" 
                                            ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                     "FSA_INSERT_DATE" ASC 
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "IS_ASSEMBLY_COMPONENT", 
  LAST_VALUE("CREATE_DATE") OVER (PARTITION BY "PK_ID" 
                                  ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                           "FSA_INSERT_DATE" ASC 
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "CREATE_DATE", 
  LAST_VALUE("PO_INDICATOR") OVER (PARTITION BY "PK_ID" 
                                   ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                            "FSA_INSERT_DATE" ASC 
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PO_INDICATOR", 
  LAST_VALUE("PO_UPDATE_DATETIME") OVER (PARTITION BY "PK_ID" 
                                         ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                  "FSA_INSERT_DATE" ASC 
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PO_UPDATE_DATETIME", 
  LAST_VALUE("PO_ORDER_NUMBER") OVER (PARTITION BY "PK_ID" 
                                      ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                               "FSA_INSERT_DATE" ASC 
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PO_ORDER_NUMBER", 
  LAST_VALUE("PO_QUANTITY_REMAINING") OVER (PARTITION BY "PK_ID" 
                                            ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                     "FSA_INSERT_DATE" ASC 
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PO_QUANTITY_REMAINING", 
  LAST_VALUE("PO_RECEIVE_BY_DATE") OVER (PARTITION BY "PK_ID" 
                                         ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                  "FSA_INSERT_DATE" ASC 
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PO_RECEIVE_BY_DATE", 
  LAST_VALUE("PO_INDICATOR_ASSIGN") OVER (PARTITION BY "PK_ID" 
                                          ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                   "FSA_INSERT_DATE" ASC 
                                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PO_INDICATOR_ASSIGN", 
  LAST_VALUE("PO_TOTAL_QUANTITY_TO_BE_RECEIVED") OVER (PARTITION BY "PK_ID" 
                                                       ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                                "FSA_INSERT_DATE" ASC 
                                                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PO_TOTAL_QUANTITY_TO_BE_RECEIVED", 
  LAST_VALUE("SHARED_ORDER_NUMBER") OVER (PARTITION BY "PK_ID" 
                                          ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                   "FSA_INSERT_DATE" ASC 
                                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "SHARED_ORDER_NUMBER", 
  LAST_VALUE("BOB_ORDER_NUMBER") OVER (PARTITION BY "PK_ID" 
                                       ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                "FSA_INSERT_DATE" ASC 
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "BOB_ORDER_NUMBER", 
  LAST_VALUE("AVAIL_DATE") OVER (PARTITION BY "PK_ID" 
                                 ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                          "FSA_INSERT_DATE" ASC 
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "AVAIL_DATE", 
  LAST_VALUE("BOB_ITEM") OVER (PARTITION BY "PK_ID" 
                               ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                        "FSA_INSERT_DATE" ASC 
                               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "BOB_ITEM", 
  LAST_VALUE("FREDD") OVER (PARTITION BY "PK_ID" 
                            ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                     "FSA_INSERT_DATE" ASC 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "FREDD", 
  LAST_VALUE("BUCKET_ON_AVAIL_DATE") OVER (PARTITION BY "PK_ID" 
                                           ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                    "FSA_INSERT_DATE" ASC 
                                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "BUCKET_ON_AVAIL_DATE", 
  LAST_VALUE("BUCKET_DATE_ON_AVAIL_DATE") OVER (PARTITION BY "PK_ID" 
                                                ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                         "FSA_INSERT_DATE" ASC 
                                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "BUCKET_DATE_ON_AVAIL_DATE", 
  LAST_VALUE("IS_GT_15_BIZDAYS") OVER (PARTITION BY "PK_ID" 
                                       ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                "FSA_INSERT_DATE" ASC 
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "IS_GT_15_BIZDAYS", 
  LAST_VALUE("IF_BUCKET1") OVER (PARTITION BY "PK_ID" 
                                 ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                          "FSA_INSERT_DATE" ASC 
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "IF_BUCKET1", 
  LAST_VALUE("IF_BUCKET2") OVER (PARTITION BY "PK_ID" 
                                 ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                          "FSA_INSERT_DATE" ASC 
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "IF_BUCKET2", 
  LAST_VALUE("IF_BUCKET3") OVER (PARTITION BY "PK_ID" 
                                 ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                          "FSA_INSERT_DATE" ASC 
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "IF_BUCKET3", 
  LAST_VALUE("IF_BUCKET4") OVER (PARTITION BY "PK_ID" 
                                 ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                          "FSA_INSERT_DATE" ASC 
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "IF_BUCKET4", 
  LAST_VALUE("IF_BUCKET5") OVER (PARTITION BY "PK_ID" 
                                 ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                          "FSA_INSERT_DATE" ASC 
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "IF_BUCKET5", 
  LAST_VALUE("IF_BUCKET6") OVER (PARTITION BY "PK_ID" 
                                 ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                          "FSA_INSERT_DATE" ASC 
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "IF_BUCKET6", 
  LAST_VALUE("IF_BUCKET7") OVER (PARTITION BY "PK_ID" 
                                 ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                          "FSA_INSERT_DATE" ASC 
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "IF_BUCKET7", 
  LAST_VALUE("IF_BUCKET8") OVER (PARTITION BY "PK_ID" 
                                 ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                          "FSA_INSERT_DATE" ASC 
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "IF_BUCKET8", 
  LAST_VALUE("IF_BUCKET9") OVER (PARTITION BY "PK_ID" 
                                 ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                          "FSA_INSERT_DATE" ASC 
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "IF_BUCKET9", 
  LAST_VALUE("IF_BUCKET10") OVER (PARTITION BY "PK_ID" 
                                  ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                           "FSA_INSERT_DATE" ASC 
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "IF_BUCKET10", 
  LAST_VALUE("IF_BUCKET11") OVER (PARTITION BY "PK_ID" 
                                  ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                           "FSA_INSERT_DATE" ASC 
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "IF_BUCKET11", 
  LAST_VALUE("IF_BUCKET12") OVER (PARTITION BY "PK_ID" 
                                  ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                           "FSA_INSERT_DATE" ASC 
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "IF_BUCKET12", 
  LAST_VALUE("IF_BUCKET13") OVER (PARTITION BY "PK_ID" 
                                  ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                           "FSA_INSERT_DATE" ASC 
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "IF_BUCKET13", 
  LAST_VALUE("IF_BUCKET14") OVER (PARTITION BY "PK_ID" 
                                  ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                           "FSA_INSERT_DATE" ASC 
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "IF_BUCKET14", 
  LAST_VALUE("FSA_UPDATED_ORIGINAL_DDA") OVER (PARTITION BY "PK_ID" 
                                               ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                        "FSA_INSERT_DATE" ASC 
                                               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "FSA_UPDATED_ORIGINAL_DDA", 
  LAST_VALUE("ITEM_AVAIL_DATE") OVER (PARTITION BY "PK_ID" 
                                      ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                               "FSA_INSERT_DATE" ASC 
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "ITEM_AVAIL_DATE", 
  LAST_VALUE("FSA_OUTPUT_STATUS") OVER (PARTITION BY "PK_ID" 
                                        ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                 "FSA_INSERT_DATE" ASC 
                                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "FSA_OUTPUT_STATUS", 
  LAST_VALUE("CAPPING_DDA") OVER (PARTITION BY "PK_ID" 
                                  ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                           "FSA_INSERT_DATE" ASC 
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "CAPPING_DDA", 
  LAST_VALUE("ORIG_CAP_DDA") OVER (PARTITION BY "PK_ID" 
                                   ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                            "FSA_INSERT_DATE" ASC 
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "ORIG_CAP_DDA", 
  LAST_VALUE("NEW_AVAIL_DATE") OVER (PARTITION BY "PK_ID" 
                                     ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                              "FSA_INSERT_DATE" ASC 
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "NEW_AVAIL_DATE", 
  LAST_VALUE("PREV_AVAIL_DATE") OVER (PARTITION BY "PK_ID" 
                                      ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                               "FSA_INSERT_DATE" ASC 
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PREV_AVAIL_DATE", 
  LAST_VALUE("PREV_CAPPING_DDA") OVER (PARTITION BY "PK_ID" 
                                       ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                "FSA_INSERT_DATE" ASC 
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PREV_CAPPING_DDA", 
  LAST_VALUE("PREV_PO_INDICATOR") OVER (PARTITION BY "PK_ID" 
                                        ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                 "FSA_INSERT_DATE" ASC 
                                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PREV_PO_INDICATOR", 
  LAST_VALUE("PREV_PO_INDICATOR_ASSIGN") OVER (PARTITION BY "PK_ID" 
                                               ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                        "FSA_INSERT_DATE" ASC 
                                               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PREV_PO_INDICATOR_ASSIGN", 
  LAST_VALUE("PREV_PO_ORDER_NUMBER") OVER (PARTITION BY "PK_ID" 
                                           ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                    "FSA_INSERT_DATE" ASC 
                                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PREV_PO_ORDER_NUMBER", 
  LAST_VALUE("PREV_PO_RECEIVE_BY_DATE") OVER (PARTITION BY "PK_ID" 
                                              ORDER BY "SOURCE_LOAD_DATE" ASC, 
                                                       "FSA_INSERT_DATE" ASC 
                                              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "PREV_PO_RECEIVE_BY_DATE" 
FROM "DEV".NETSUITE2_FSA."FSA_HISTORICAL" 
WHERE "IS_VALID"
  AND "SOURCE_TYPE" in ('XFER','Assembly')
  AND "FSA_OUTPUT_STATUS" in ('NEW','UPDATED')
;