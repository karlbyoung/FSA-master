CREATE OR REPLACE VIEW DEV.BUSINESS_OPERATIONS.V_DEMAND_PO
            ( ORDER_NUMBER, UNIQUE_KEY, DDA, TRANSACTION_TYPE, TOTAL_AMT, ITEM, ITEM_ID, COMPONENT_ITEM_ID, COMPONENT_ITEM, QTY_ORDERED
            , COMPONENT_QTY_ORDERED, LOCATION, PRIORITY_LEVEL, SOURCETYPE, NS_LINE_NUMBER, ORG_DDA, TRANSACTION_ID, LINE_ID, SO_MULTISITE_ORDER
            , SO_MATERIAL_SUPPORT_STATUS, SO_AMPLIFY_INTEGRATION_STATUS, TOTAL_AVAIL_QTY, BO_STATUS, TYPE_NAME, NUMBER_IN_CARTON
            )
AS
(
------   1.. TRANSFER ORDER ------
WITH CTE_XFER AS (
    SELECT ABS(TOLI.QUANTITY)                                AS ABS_QUANTITY
         , TOLI.TRANSFER_ORDER_TRANSACTION_ID                AS TRANSACTION_ID
         , IFNULL(RRTL_SHP.QUANTITY_RECEIVED_IN_SHIPMENT, 0) AS QUANTITY_FULFILLED
         , TORD.*
         , TOLI.*
FROM DEV.NETSUITE.FACT_TRANSFER_ORDER_LINE_ITEM  TOLI
JOIN DEV.NETSUITE.FACT_TRANSFER_ORDER            TORD
  ON TOLI.TRANSFER_ORDER_TRANSACTION_ID = TORD.TRANSFER_ORDER_TRANSACTION_ID
//These 2 tables joins are a workaround until QUANTITY_FULFILLED is available in the FACT_TRANSFER_ORDER_LINE_ITEM table.
JOIN DEV.NETSUITE_RAW_RESTRICT.TRANSACTION_LINES RRTL
  ON TOLI.UNIQUE_KEY = RRTL.UNIQUE_KEY AND RRTL.TRANSFER_ORDER_LINE_TYPE = 'ITEM'
JOIN DEV.NETSUITE_RAW_RESTRICT.TRANSACTION_LINES RRTL_SHP
  ON TOLI.TRANSFER_ORDER_TRANSACTION_ID = RRTL_SHP.TRANSACTION_ID
 AND RRTL.TRANSACTION_LINE_ID           = RRTL_SHP.TRANSFER_ORDER_ITEM_LINE
 AND RRTL_SHP.TRANSFER_ORDER_LINE_TYPE  = 'SHIPPING'
 AND TORD.STATUS NOT IN ('Closed', 'Cancelled')
 AND LOWER(TORD.LOCATION_TO) LIKE '%depo%' -- only returns rows that are NOT vendor-to-vendor
 AND IFNULL(RRTL_SHP.QUANTITY_RECEIVED_IN_SHIPMENT, 0) < ABS(TOLI.QUANTITY)
 AND IFNULL(TOLI.QUANTITY_COMMITTED, 0) = 0
)

----- 2.  OPEN SALES ORDER JOINT  Purchas Order ------
, CTE_OPENSALES AS (
SELECT A.SO_ORDER_NUMBER
     , A.SOLI_UNIQUE_KEY               -- system-assigned line unique key
     , A.SOLI_ITEM
     , A.SOLI_ITEM_ID
     , IFNULL(A.SOLI_DDA_ORIGINAL, A._DDA_FOR_SORT)   AS SO_DDA_MODIFIED
     , A._DDA_FOR_SORT
     , A.IFFLI_DDA
     , A.SOLI_DDA
     , A.SOLI_START_DATE
     , A.SO_START_DATE
     , A.SO_TRANSACTION_TYPE
     , CASE WHEN A.SO_TRAN_FIRST_80_2ND_ORDER_CREATED = 'T'
                THEN 'PF80'
            ELSE A.SO_TRANSACTION_TYPE
       END                                            AS SO_TRANSACTION_TYPE_NEW
     , A.SO_TRAN_FIRST_80_ORDER
     , A.SO_FIRST_80_2ND               ---    This is the indicator to let us know that it is the Expanded order.   When status = 'pending approval' and this Flag is T, someone manaully goes in and approves it
     , A.SO_TRAN_FIRST_80_2ND_ORDER_CREATED
     , A.SOLI_FIRST80_LINE_ITEM
     , A.QTY_AVAILABLE
     , A.QTY_ON_HAND
     , A.SO_STATUS
     , C.TOTAL
     , CASE WHEN A.SO_TRANSACTION_TYPE = 'Sale' AND B.PRIORITY_LEVEL_ID IS NULL
                THEN 4
            WHEN A.SO_TRANSACTION_TYPE = 'Renewal' AND B.PRIORITY_LEVEL_ID IS NULL
                THEN 4
            WHEN A.SO_TRANSACTION_TYPE = 'Sale' AND B.PRIORITY_LEVEL_ID IS NOT NULL
                THEN B.PRIORITY_LEVEL_ID
            WHEN A.SO_TRANSACTION_TYPE = 'Renewal' AND B.PRIORITY_LEVEL_ID IS NOT NULL
                THEN B.PRIORITY_LEVEL_ID
            ELSE 1
       END                                            AS PRIORITY_LEVEL
     , A.SOLI_LOCATION
     , A._SOLI_IS_BIZOPS_RELEVANT
     , A.SOLI_QTY_ORDERED
     , K.COMPONENT_ITEM_QUANTITY
     , K.COMPONENT_ITEM_QUANTITY * A.SOLI_QTY_ORDERED AS COMPONENT_QTY_ORDERED
     , A.DEFERRED_REVENUE_BUCKETS
     , K.COMPONENT_ITEM
     , K.COMPONENT_ITEM_ID
     , A.SOLI_LINE_NUMBER
     , A.SOLI_DDA_ORIGINAL
     , A.SOLI_LINE_ID                  -- added 8/9/2022
     , A.SO_TRANSACTION_ID             -- added 8/9/2022
     , A.DI_TYPE_NAME                  --  added 10/27/2022 -- to indentify if the Item is Kits from dim_Item
     , A.SO_MULTISITE_ORDER            -- added 11/16/2022
     , A.SO_MATERIAL_SUPPORT_STATUS    -- added 03/14/2023 Per Emma Greenstein
     , A.SO_AMPLIFY_INTEGRATION_STATUS --  added 03/14/2023 Per Emma Greenstein

FROM DEV.BUSINESS_OPERATIONS.V_SO_MASTER A --- changed from v_so_master to v_so_master  -- have conditions to filter out some data
LEFT OUTER JOIN DEV.NETSUITE_RAW_RESTRICT.TRANSACTIONS B
    ON A.SO_TRANSACTION_ID = B.TRANSACTION_ID
LEFT OUTER JOIN (SELECT SALES_ORDER_TRANSACTION_ID, SUM(AMOUNT) AS TOTAL
                 FROM DEV.NETSUITE.FACT_SO_LINE_ITEM
                 GROUP BY SALES_ORDER_TRANSACTION_ID) C
    ON B.TRANSACTION_ID = C.SALES_ORDER_TRANSACTION_ID
LEFT OUTER JOIN DEV.BUSINESS_OPERATIONS.V_KIT K
    ON A.SO_ORDER_NUMBER = K.SO_ORDER_NUMBER
   AND A.SOLI_ITEM_ID    = K.SOLI_ITEM_ID

   WHERE (A.SOLI_LOCATION IN ('hand2mind', 'BR Printers', 'JPS Graphics', 'LSC Owensville', 'Wards VWR', 'Not Yet Assigned') OR
          A.SOLI_LOCATION IS NULL)
     AND YEAR(A._DDA_FOR_SORT) = YEAR(GETDATE())
     AND A.SOLI_IS_FULFILLED = FALSE       --line is "open"
     AND A._SOLI_IS_BIZOPS_RELEVANT = TRUE --line is a physical good, plus some other criteria
     AND A.SO_TRANSACTION_TYPE NOT IN ('Depo')
     AND A.DEFERRED_REVENUE_BUCKETS IS NULL
     AND A.IFF_REF_NO IS NULL --exclude lines that are already associated with an IFF
)

------ 3.  PO/ Assembly   ---------
, CTE_PO_DETAIL AS (
SELECT PO.ORDER_NUMBER                                                                                                     -- user-facing unique order id
                        , POLIA.UNIQUE_KEY                                                                                                    -- system-assigned line unique key
                        , POLIA.ASSEMBLY_ELSE_ITEM_ID                                                                                         -- system-assigned item id
                        , CASE WHEN I.TYPE_NAME = 'Assembly' THEN I.FULL_NAME ELSE POLI.ITEM END             AS ITEM                          -- unformatted ISBN
                        --,I.FULL_NAME
                        , I.TYPE_NAME                                                                                                         -- item type
                        , PO.LOCATION
                        , (POLIA.QUANTITY * .9) - ZEROIFNULL(QUANTITY_RECEIVED)                              AS QUANITITY_TO_BE_FULFILLED

                        , IC.COMPONENT_ITEM_ID                                                                                                -- if this is an Assembly Component, the system-assigned item id
                        , I_COMPONENT.NAME                                                                   AS COMPONENT_ITEM                -- if this is an Assembly Component, the unformatted ISBN
                        , (POLIA.QUANTITY * .9) - ZEROIFNULL(QUANTITY_RECEIVED) * IC.COMPONENT_ITEM_QUANTITY AS COMPONENT_QTY_TO_BE_FULFILLED -- if this is an Assembly Component, the qty of the component needed to fulfill the QTY of the Kit
                        , POLI.NS_LINE_NUMBER

                   FROM DEV.NETSUITE.FACT_PURCHASE_ORDER                             PO
                   JOIN      DEV.BUSINESS_OPERATIONS.NS_PURCHASE_ORDER_LINE_ITEM_AUX POLIA
                 ON PO.PURCHASE_ORDER_TRANSACTION_ID = POLIA.PURCHASE_ORDER_TRANSACTION_ID
                   JOIN      DEV.NETSUITE.FACT_PURCHASE_ORDER_LINE_ITEM              POLI
                 ON POLIA.UNIQUE_KEY = POLI.UNIQUE_KEY
                   JOIN      DEV.NETSUITE.DIM_ITEM                                   I
                 ON POLIA.ASSEMBLY_ELSE_ITEM_ID = I.ITEM_ID
                   JOIN      DEV.NETSUITE.FACT_TRANSACTION_LINE                      FTL -- Get the Main Line to get the Product Line (Class) of the PO
                 ON PO.PURCHASE_ORDER_TRANSACTION_ID = FTL.TRANSACTION_ID AND FTL.TRANSACTION_LINE_ID = 0

                   LEFT JOIN DEV.BUSINESS_OPERATIONS.NS_ITEMS_COMPONENTS             IC
                                 --    on poli.ITEM_ID = ic.ITEM_ID  --- 10/20/2022  this join is incorrect and replace with "on polia.ASSEMBLY_ELSE_ITEM_ID = ic.ITEM_ID   --- 10/20/2022"when go live.  We keep it for now for QA
                 ON POLIA.ASSEMBLY_ELSE_ITEM_ID = IC.ITEM_ID --- 10/20/2022 with Joseph's help and updated on Tech Doc
                                 AND IC.ITEM_TYPE = 'Assembly'
                   LEFT JOIN DEV.NETSUITE.DIM_ITEM                                   I_COMPONENT
                 ON IC.COMPONENT_ITEM_ID = I_COMPONENT.ITEM_ID
                   WHERE
                     --     -- po.ORDER_NUMBER = '[ORDERNUMBER]'    and
                       POLIA._POLI_IS_RECEIVED = FALSE --line is "open"
                     AND FTL.CLASS_NAME NOT IN ('General', 'Other'))

---------  Inventory

, CTE_INVENTORY AS (
SELECT INV.ITEM_ID
                        , DI.NAME                  AS ITEM
                        , DI.TYPE_NAME
                        , SUM(INV.AVAILABLE_COUNT) AS TOTAL_AVAIL_QTY --  ,L.NAME AS LOCATION
                        --,INV.AVAILABLE_COUNT as QTY_AVAILABLE
                   FROM DEV.NETSUITE_RAW_RESTRICT.ITEM_LOCATION_MAP INV
                   JOIN       DEV.NETSUITE_RAW_RESTRICT.LOCATIONS   L
                  ON INV.LOCATION_ID = L.LOCATION_ID
                   INNER JOIN DEV.NETSUITE.DIM_ITEM                 DI
                  ON INV.ITEM_ID = DI.ITEM_ID
                   WHERE INV.ON_HAND_COUNT IS NOT NULL
                     AND (L.NAME IN ('BR Printers', 'hand2mind', 'JPS Graphics', 'LSC Owensville',
                                     'Wards VWR') -- 11/9/2022 removed 'Not Yet Assigned' - verifed the Total_AVAIL_QTYs are 1
                       OR L.NAME IS NULL)
                   GROUP BY INV.ITEM_ID, DI.NAME, DI.TYPE_NAME)


---  Throughput -- 10/31/2022
-- need to qa for dups
, CTE_CARTON AS (SELECT CAST(FULL_NAME AS VARCHAR(50))    AS ITEM
                     , ITEM_ID
                     , TYPE_NAME
                     , CAST(DISPLAYNAME AS VARCHAR)      AS DISPLAYNAME
                     , CAST(PRODUCT_CATEGORY AS VARCHAR) AS PRODUCT_CATEGORY
                     , CAST(PRODUCT_FAMILY AS VARCHAR)   AS PRODUCT_FAMILY
                     , TRUE_MASTER
                     --, CAST(MASTERQTY as varchar) as MASTERQTY
                     , ZEROIFNULL(MASTERQTY)             AS MASTERQTY
                     , EST_CT_PALLET
                     , TOTAL_CARTONS_PER_1_QTY
                     , MSTR_WEIGHT
                     , MSTR_LENGTH
                     , MSTR_WIDTH
                     , MSTR_HEIGHT
                     , LSC_L_W_H
                FROM DEV.BUSINESS_OPERATIONS.V_DIM_CARTONS_LOOSE)

------------------------------------------------------------------------------------------------------------------------------------------------
--            combine  the results from 3 sources ( xfer order/ open sales / PO/ assembly --
------------------------------------------------------------------------------------------------------------------------------------------------
   
   -----------------  Xfer Order -----------------
,CTE_SOURCES_ASSIGN_PO AS (
    SELECT A.ORDER_NUMBER
         , A.UNIQUE_KEY
         , CAST(DDA_OVERRIDE_DATE AS DATE)       AS DDA
         , A.TRANSACTION_TYPE
         , 0                                     AS TOTAL_AMT
         , A.ITEM
         , A.ITEM_ID
         , NULL                                  AS COMPONENT_ITEM_ID
         , NULL                                  AS COMPONENT_ITEM
         , A.ABS_QUANTITY                        AS QTY_ORDERED
         , NULL                                  AS COMPONENT_QTY_ORDERED
         , A.LOCATION_FROM                       AS LOCATION
         , 1                                     AS PRIORITY_LEVEL
         , 'XFER'                                AS SOURCETYPE
         , TO_CHAR(A.NS_LINE_NUMBER)             AS NS_LINE_NUMBER
         , TRY_TO_DATE('')                       AS ORG_DDA
         , CAST(A.TRANSACTION_ID AS VARCHAR(50)) AS TRANSACTION_ID
         , NULL                                  AS LINE_ID
         , NULL                                  AS SO_MULTISITE_ORDER
         , NULL                                  AS SO_MATERIAL_SUPPORT_STATUS
         , NULL                                  AS SO_AMPLIFY_INTEGRATION_STATUS
FROM CTE_XFER A
GROUP BY A.ORDER_NUMBER, A.UNIQUE_KEY, TO_DATE(DDA_OVERRIDE_DATE), A.TRANSACTION_TYPE, A.ITEM, A.ITEM_ID
      , A.ABS_QUANTITY, A.LOCATION_FROM, A.NS_LINE_NUMBER, A.TRANSACTION_ID

UNION

-----------------  sales Order  -----------------
SELECT A.SO_ORDER_NUMBER
     , A.SOLI_UNIQUE_KEY
     , CAST(MIN(A.SO_DDA_MODIFIED) AS DATE) AS MIN_SO_DDA_MODIFIED
     , A.SO_TRANSACTION_TYPE_NEW
     , A.TOTAL
     , A.SOLI_ITEM
     , A.SOLI_ITEM_ID
     , A.COMPONENT_ITEM_ID
     , A.COMPONENT_ITEM
     , A.SOLI_QTY_ORDERED
     , A.COMPONENT_QTY_ORDERED
     , A.SOLI_LOCATION
     , PRIORITY_LEVEL
     , 'OpenSO'                             AS SOURCETYPE
     , TO_CHAR(A.SOLI_LINE_NUMBER)          AS NS_LINE_NUMBER
     , A.SOLI_DDA_ORIGINAL                  AS ORG_DDA
     , TO_CHAR(A.SO_TRANSACTION_ID)    -- added 8/9/2022
     , TO_CHAR(A.SOLI_LINE_ID)         -- added 8/9/2022
     , A.SO_MULTISITE_ORDER            --- added 11/16/2022
     , A.SO_MATERIAL_SUPPORT_STATUS    -- added 03/14/2023 Per Emma Greenstein
     , A.SO_AMPLIFY_INTEGRATION_STATUS -- added 03/14/2023 Per Emma Greenstein
FROM CTE_OPENSALES A
WHERE SO_TRANSACTION_TYPE_NEW != 'Sample (aka Internal fulfillment)'
  AND A.DEFERRED_REVENUE_BUCKETS IS NULL
GROUP BY A.SO_ORDER_NUMBER, A.SOLI_UNIQUE_KEY, A.SO_TRANSACTION_TYPE_NEW, TOTAL, A.SOLI_ITEM, A.SOLI_ITEM_ID
       , A.COMPONENT_ITEM_ID, A.COMPONENT_ITEM, A.SOLI_QTY_ORDERED, A.COMPONENT_QTY_ORDERED, A.SOLI_LOCATION
       , PRIORITY_LEVEL, A.SOLI_LINE_NUMBER, A.SOLI_DDA_ORIGINAL, A.SO_TRANSACTION_ID, A.SOLI_LINE_ID
       , A.SO_MULTISITE_ORDER, A.SO_MATERIAL_SUPPORT_STATUS
       , A.SO_AMPLIFY_INTEGRATION_STATUS -- added 03/14/2023 Per Emma Greenstein

UNION

-----------------  open po / Assembly  -----------------
SELECT A.ORDER_NUMBER
    , A.UNIQUE_KEY
    , CAST(MAX(RECEIVE_BY_DATE) AS DATE) AS MAX_RECEIVE_BY_DATE
    , A.TYPE_NAME
    , 0                                  AS TOTAL_AMT
    , A.ITEM                                                              --  ,B.ISBN
    , A.ASSEMBLY_ELSE_ITEM_ID
    , A.COMPONENT_ITEM_ID
    , A.COMPONENT_ITEM
    , A.QUANITITY_TO_BE_FULFILLED
    , A.COMPONENT_QTY_TO_BE_FULFILLED
    , B.LOCATION
    , 1                                AS PRIORITY_LEVEL
    , 'Assembly'                         AS SOURCETYPE
    , TO_CHAR(A.NS_LINE_NUMBER)          AS NS_LINE_NUMBER
    , TRY_TO_DATE('')                    AS ORG_DDA
    , NULL                               AS TRANSACTION_ID                -- added 8/9/2022 for NS upload
    , NULL                               AS LINE_ID                       -- added 8/9/2022 for NS upload
    , NULL                               AS SO_MULTISITE_ORDER            --- added 11/16/2022
    , NULL                               AS SO_MATERIAL_SUPPORT_STATUS    -- added 03/14/2023 Per Emma Greenstein
    , NULL                               AS SO_AMPLIFY_INTEGRATION_STATUS -- added 03/14/2023 Per Emma Greenstein
FROM CTE_PO_DETAIL                          A
INNER JOIN DEV.BUSINESS_OPERATIONS.V_OPENPO B

ON A.ORDER_NUMBER = B.ORDER_NUMBER AND A.ASSEMBLY_ELSE_ITEM_ID = B.ASSEMBLY_ELSE_ITEM_ID
   -- and a.ITEM = b.ISBN
WHERE B.LOCATION IN ('hand2mind', 'BR Printers', 'JPS Graphics', 'LSC Owensville', 'Wards VWR', 'Not Yet Assigned')
 AND A.TYPE_NAME = 'Assembly'
 AND YEAR(RECEIVE_BY_DATE) >= 2022

 --and a. ORDER_NUMBER = '110'

GROUP BY A.ORDER_NUMBER, A.UNIQUE_KEY, A.TYPE_NAME, A.ITEM --,B.ISBN
      , A.ASSEMBLY_ELSE_ITEM_ID, A.COMPONENT_ITEM_ID, A.COMPONENT_ITEM, A.QUANITITY_TO_BE_FULFILLED
      , A.COMPONENT_QTY_TO_BE_FULFILLED, B.LOCATION, A.NS_LINE_NUMBER)

------------------------------------------------------------------------------------------------------------------------------------------------
-- results --
------------------------------------------------------------------------------------------------------------------------------------------------        


-- , cte_result as (
SELECT A.*
     , B.TOTAL_AVAIL_QTY
     , CASE WHEN B.TOTAL_AVAIL_QTY >= A.QTY_ORDERED THEN 'Available' ELSE 'BackOrder' END AS BO_STATUS
     , DI.TYPE_NAME --- CANNOT USE b or C.TYPE_NAME.. IT can return component type_name base on the join 
     , CAST(C.MASTERQTY AS VARCHAR)                                                       AS NUMBER_IN_CARTON
FROM CTE_SOURCES_ASSIGN_PO            A
LEFT OUTER JOIN DEV.NETSUITE.DIM_ITEM DI
    ON A.ITEM_ID = DI.ITEM_ID
LEFT OUTER JOIN CTE_INVENTORY         B
    ON IFNULL(A.COMPONENT_ITEM_ID, A.ITEM_ID) = B.ITEM_ID
LEFT OUTER JOIN CTE_CARTON            C
    ON IFNULL(A.COMPONENT_ITEM_ID, A.ITEM_ID) = C.ITEM_ID


WHERE CAST(A.ORDER_NUMBER AS VARCHAR) NOT LIKE ('%Planning%')
      --and a.ITEM = '9781639487004'
      -- where a.item_id in ('36749')
      -- and a.ns_line_number = 17 
    );