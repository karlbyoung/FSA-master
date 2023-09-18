create or replace view V_DEMAND_PO(
	ORDER_NUMBER,
	UNIQUE_KEY,
	DDA,
	TRANSACTION_TYPE,
	TOTAL_AMT,
	ITEM,
	ITEM_ID,
	COMPONENT_ITEM_ID,
	COMPONENT_ITEM,
	QTY_ORDERED,
	COMPONENT_QTY_ORDERED,
	LOCATION,
	PRIORITY_LEVEL,
	SOURCETYPE,
	NS_LINE_NUMBER,
	ORG_DDA,
	TRANSACTION_ID,
	LINE_ID,
	SO_MULTISITE_ORDER,
	SO_MATERIAL_SUPPORT_STATUS,
	SO_AMPLIFY_INTEGRATION_STATUS,
	TOTAL_AVAIL_QTY,
	BO_STATUS,
	TYPE_NAME,
	NUMBER_IN_CARTON,
	TRANSACTION_CREATE_DATE,
	ESTIMATED_DELIVERY_DATE,
	SALES_ORDER_TYPE,
    /* 20230711 - KBY, RFS23-1850 - Include inventories for all FSA locations, and for forward-facing locations only */
    TOTAL_AVAIL_QTY_FWD,
    TOTAL_AVAIL_QTY_NONFWD,
    /* 20230912 - KBY, RFS23-2652 - include Product Line column for Sample order info */
    PO_PRODUCT_LINE,
    /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment */
    FR_PREV_DAYS
) as (
SELECT 
  "ORDER_NUMBER", 
  "UNIQUE_KEY", 
  "DDA", 
  "TRANSACTION_TYPE", 
  "TOTAL_AMT", 
  "ITEM", 
  "ITEM_ID", 
  "COMPONENT_ITEM_ID", 
  "COMPONENT_ITEM", 
  "QTY_ORDERED", 
  "COMPONENT_QTY_ORDERED", 
  "LOCATION", 
  "PRIORITY_LEVEL", 
  "SOURCETYPE", 
  "NS_LINE_NUMBER", 
  "ORG_DDA", 
  "TRANSACTION_ID", 
  "LINE_ID", 
  "SO_MULTISITE_ORDER", 
  "SO_MATERIAL_SUPPORT_STATUS", 
  "SO_AMPLIFY_INTEGRATION_STATUS", 
  "TOTAL_AVAIL_QTY", 
  "BO_STATUS", 
  "TYPE_NAME", 
  "NUMBER_IN_CARTON",
  "CREATE_DATE",
  "ESTIMATED_DELIVERY_DATE",
  "SALES_ORDER_TYPE",
    /* 20230711 - KBY, RFS23-1850 - Include inventories for all FSA locations, and for forward-facing locations only */
  "TOTAL_AVAIL_QTY_FWD",
  "TOTAL_AVAIL_QTY_NONFWD",
  /* 20230912 - KBY, RFS23-2652 - include Product Line column for Sample order info */
  "PO_PRODUCT_LINE",
  /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment */
  "FR_PREV_DAYS"
FROM (------ 1. TRANSFER ORDER ------

WITH CTE_XFER AS (
    SELECT ABS(TOLI.QUANTITY) AS Abs_QUANTITY
        , TOLI.TRANSFER_ORDER_TRANSACTION_ID AS TRANSACTION_ID
        , IFNULL(TOLI.QUANTITY_FULFILLED, 0) AS QUANTITY_FULFILLED                                   
        , TORD.* RENAME TRANSACTION_TYPE AS "HEADER_TRANSACTION_TYPE"
        , TOLI.*                       
    FROM DEV.NETSUITE2.FACT_TRANSFER_ORDER_LINE_ITEM TOLI
    JOIN DEV.NETSUITE2.FACT_TRANSFER_ORDER TORD
        ON TOLI.TRANSFER_ORDER_TRANSACTION_ID = TORD.TRANSFER_ORDER_TRANSACTION_ID
        AND TORD.STATUS NOT IN ('Closed', 'Cancelled')
        AND LOWER(TORD.LOCATION_TO) LIKE '%depo%'
        AND IFNULL(TOLI.QUANTITY_FULFILLED, 0) < ABS(TOLI.QUANTITY)
        AND IFNULL(TOLI.QUANTITY_COMMITTED, 0) = 0
)

----- 2. OPEN SALES ORDER JOINT Purchase Order ------
,  CTE_OPENSALES AS (
    SELECT A.SO_ORDER_NUMBER
        , A.SOLI_UNIQUE_KEY
        , A.SOLI_ITEM 
        , A.SOLI_ITEM_ID
        , IFNULL(A.SOLI_DDA_ORIGINAL, A._dda_for_sort ) AS SO_DDA_MODIFIED
        , A._DDA_FOR_SORT
        , A.IFFLI_DDA
        , A.SOLI_DDA
        , A.SOLI_START_DATE
        , A.SO_START_DATE
        , A.SO_TRANSACTION_TYPE 
        , CASE WHEN B.FIRST_80_2ND_ORDER_CREATED = 'T'
                THEN 'PF80'
            ELSE A.SO_TRANSACTION_TYPE
          END SO_TRANSACTION_TYPE_NEW
        , B.FIRST_80_ORDER AS SO_TRAN_FIRST_80_ORDER
        , B.FIRST_80_2ND AS SO_FIRST_80_2ND
        , B.FIRST_80_2ND_ORDER_CREATED
        , FSOLI.FIRST80_LINE_ITEM AS SOLI_FIRST80_LINE_ITEM
        , D.QTY_AVAILABLE
        , D.QTY_ON_HAND
        , A.SO_STATUS
        , C.total
        , CASE WHEN A.SO_TRANSACTION_TYPE = 'Sale' AND B.SALES_ORDER_TYPE = 104 
                 THEN 1
               WHEN A.SO_TRANSACTION_TYPE IN ('Sale', 'Renewal') AND B.PRIORITY_LEVEL_ID IS NULL 
                 THEN 4
               WHEN A.SO_TRANSACTION_TYPE IN ('Sale', 'Renewal') AND B.PRIORITY_LEVEL_ID IS NOT NULL 
                 THEN B.PRIORITY_LEVEL_ID 
            ELSE 1  
            END AS PRIORITY_LEVEl       
        , A.SOLI_LOCATION
        , A._soli_is_bizops_relevant 
        , A.SOLI_QTY_ORDERED
        , NULL AS COMPONENT_ITEM_QUANTITY
        , NULL AS COMPONENT_QTY_ORDERED
        , B.DEFERRED_REVENUE_BUCKETS
        , NULL AS COMPONENT_ITEM
        , NULL AS COMPONENT_ITEM_ID
        , A.SOLI_LINE_NUMBER
        , A.SOLI_DDA_ORIGINAL    
        , A.SOLI_LINE_ID
        , A.SO_TRANSACTION_ID
        , A.SOLI_ITEM_TYPE AS DI_TYPE_NAME
        , A.SO_MULTISITE_ORDER  
        , A.SO_MATERIAL_SUPPORT_STATUS
        , A.SO_AMPLIFY_INTEGRATION_STATUS
        , B.CREATE_DATE
        , FSOLI.ESTIMATED_DELIVERY_DATE
        , B.SALES_ORDER_TYPE
    FROM DEV.NETSUITE2_FSA.NS_SALES_ORDER_LINE_ITEM A
    JOIN DEV.NETSUITE2.FACT_SO_LINE_ITEM FSOLI
        ON A.SOLI_UNIQUE_KEY = FSOLI.UNIQUE_KEY
    LEFT OUTER JOIN DEV.NETSUITE2_FSA.NS_ITEMS_AT_LOCATIONS D
        ON A.SOLI_ITEM_ID = D.ITEM_ID  
        AND A.IFFLI_LOCATION = D.LOCATION
    LEFT OUTER JOIN DEV.NETSUITE2.FACT_SALES_ORDER B
        ON A.SO_TRANSACTION_ID = B.SALES_ORDER_TRANSACTION_ID
    LEFT OUTER JOIN (
        SELECT SALES_ORDER_TRANSACTION_ID
            , SUM(AMOUNT) AS TOTAL 
        FROM DEV.NETSUITE2.FACT_SO_LINE_ITEM 
        GROUP BY sales_order_transaction_id
        ) C
        ON B.SALES_ORDER_TRANSACTION_ID = C.SALES_ORDER_TRANSACTION_ID       
     WHERE  
        (A.SOLI_LOCATION IN ('BR Printers KY','BR Printers SJ','BR Printers CN',
                               'LSC Owensville','LSC Airwest','LSC Linn',
                               'Barrett Distribution','hand2mind','JPS Graphics',
                               'Not Yet Assigned'
                               ,'Booksource', 'Continuum')
        OR A.SOLI_LOCATION IS NULL)
        AND A.SOLI_IS_FULFILLED = 'FALSE' --line is open
        AND A._SOLI_IS_BIZOPS_RELEVANT = 'TRUE' --line is a physical good, plus some other criteria
        AND A.SO_TRANSACTION_TYPE NOT IN ('Depo') 
        AND B.DEFERRED_REVENUE_BUCKETS IS NULL  
        AND (A.IFF_REF_NO IS NULL AND FSOLI.FULFILLMENT_REQUEST_NO IS NULL)--exclude lines that are already associated with an IFF
        and a.SOLI_ITEM_TYPE != 'Non-inventory Item' //2023.04.04:JB:added this condition for RFS23-1177  
        and coalesce(b.SSR_INITIATIVE,'--') != 'Priority Order' //2023.04.04:JB:added this condition for RFS23-1175 
) 
   
------ 3.  PO/ Assembly   ---------
, CTE_PO_DETAIL AS (
    SELECT PO.ORDER_NUMBER -- user-facing unique order id
        , POLIA.UNIQUE_KEY -- system-assigned line unique key
        , POLIA.ASSEMBLY_ELSE_ITEM_ID -- system-assigned item id
        , CASE WHEN I.TYPE_NAME = 'Assembly' THEN  I.FULL_NAME ELSE POLI.ITEM END AS ITEM -- unformatted ISBN
        --, I.FULL_NAME
        , i.TYPE_NAME -- item type
        , PO.LOCATION
        /* 20230626 - KBY, HyperCare Ref #134 - set QUANITITY_TO_BE_FULFILLED[sic] based on 100% of QUANTITY */
        , POLIA.QUANTITY-ZEROIFNULL(QUANTITY_RECEIVED) AS QUANITITY_TO_BE_FULFILLED
        , IC.COMPONENT_ITEM_ID -- if this is an Assembly Component, the system-assigned item id
        , i_component.NAME AS COMPONENT_ITEM -- if this is an Assembly Component, the unformatted ISBN
        /* 20230626 - KBY, HyperCare Ref #134 - set COMPONENT_QTY_TO_BE_FULFILLED based on 100% of QUANTITY */
        , (POLIA.QUANTITY-ZEROIFNULL(QUANTITY_RECEIVED) ) * IC.COMPONENT_ITEM_QUANTITY AS COMPONENT_QTY_TO_BE_FULFILLED -- if this is an Assembly Component, the qty of the component needed to fulfill the QTY of the Kit
        , POLI.NS_LINE_NUMBER 
        , PO.CREATE_DATE
        , PO.PURCHASE_ORDER_TRANSACTION_ID  -- 20230710 - KBY, Include Transaction ID's for Assembly as well
        /* 20230912 - KBY, RFS23-2652 - include Product Line column for Sample order info */
        , POLIA.PO_PRODUCT_LINE
    FROM DEV.NETSUITE2.FACT_PURCHASE_ORDER PO
    JOIN DEV.NETSUITE2_FSA.NS_PURCHASE_ORDER_LINE_ITEM_AUX POLIA
        ON PO.PURCHASE_ORDER_TRANSACTION_ID = POLIA.PURCHASE_ORDER_TRANSACTION_ID
    JOIN DEV.NETSUITE2.FACT_PURCHASE_ORDER_LINE_ITEM POLI
        ON POLIA.UNIQUE_KEY = POLI.UNIQUE_KEY
    JOIN DEV.NETSUITE2.DIM_ITEM i
        ON POLIA.ASSEMBLY_ELSE_ITEM_ID = i.ITEM_ID 
    LEFT JOIN DEV.NETSUITE2_FSA.NS_ITEMS_COMPONENTS IC
        ON POLIA.ASSEMBLY_ELSE_ITEM_ID = IC.ITEM_ID   --- 10/20/2022 with Joseph's help and updated on Tech Doc 
        AND IC.ITEM_TYPE = 'Assembly'
    LEFT JOIN DEV.NETSUITE2.DIM_ITEM i_component
        ON IC.COMPONENT_ITEM_ID = i_component.ITEM_ID
        AND i_component.TYPE_NAME != 'Non-inventory Item'
    WHERE POLIA._POLI_IS_RECEIVED = 'FALSE' --line is open
       /* 20230912 - KBY, RFS23-2653 - allow Product Lines marked "General" or "Other" for Sample orders */
--        AND POLIA.PO_PRODUCT_LINE NOT IN ('General', 'Other')
       /* 20230816 - KBY, RFS23-2441 Exclude PO marked as closed */
        AND poli.IS_CLOSED = 'F'
)
    
---------  Inventory   
, CTE_INVENTORY_FWD AS (    -- Forward facing inventory
/*||JB.2023.03.29|
Recommendation: Use Business Operations maintained DEV.NETSUITE2_FSA.NS_ITEMS_AT_LOCATIONS table instead of tables in NETSUITE2_RAW_RESTRICT schema
=============
|*/

    SELECT NSIAL.ITEM_ID
        , I.NAME AS ITEM  
        , I.TYPE_NAME
        , SUM(NSIAL.QTY_AVAILABLE) AS TOTAL_AVAIL_QTY_FWD
    FROM DEV.NETSUITE2_FSA.NS_ITEMS_AT_LOCATIONS NSIAL
    JOIN DEV.NETSUITE2.DIM_ITEM I
        ON NSIAL.ITEM_ID = I.ITEM_ID
    WHERE 1=1
        AND NSIAL.LOCATION IN ('BR Printers KY','BR Printers SJ','BR Printers CN',
                               'LSC Owensville','LSC Airwest','LSC Linn',
                               'Barrett Distribution','hand2mind','JPS Graphics',
                               'Not Yet Assigned')
        /* 20230714 - KBY - Don't include negative numbers in sum of inventory */
        AND NSIAL.QTY_AVAILABLE >= 0
        AND IS_KIT = 'FALSE' -- exclude the Kit records. Their inventory is virtual. It does not actually exist
    GROUP BY NSIAL.ITEM_ID
        , I.NAME
        , I.TYPE_NAME
)
, CTE_INVENTORY_NONFWD AS ( -- non-forward-facing inventory, i.e., assembly only
/*||JB.2023.03.29|
Recommendation: Use Business Operations maintained DEV.NETSUITE2_FSA.NS_ITEMS_AT_LOCATIONS table instead of tables in NETSUITE2_RAW_RESTRICT schema
=============
|*/

    SELECT NSIAL.ITEM_ID
        , I.NAME AS ITEM  
        , I.TYPE_NAME
        , SUM(NSIAL.QTY_AVAILABLE) AS TOTAL_AVAIL_QTY_NONFWD
    FROM DEV.NETSUITE2_FSA.NS_ITEMS_AT_LOCATIONS NSIAL
    JOIN DEV.NETSUITE2.DIM_ITEM I
        ON NSIAL.ITEM_ID = I.ITEM_ID
    WHERE 1=1
        AND NSIAL.LOCATION IN ('Booksource', 'Continuum')
        /* 20230714 - KBY - Don't include negative numbers in sum of inventory */
        AND NSIAL.QTY_AVAILABLE >= 0
        AND IS_KIT = 'FALSE' -- exclude the Kit records. Their inventory is virtual. It does not actually exist
    GROUP BY NSIAL.ITEM_ID
        , I.NAME
        , I.TYPE_NAME
)
 
-- Throughput -- 10/31/2022 
-- need to qa for dups
, CTE_CARTON AS (
 /*||JB.2023.03.29|
I’m not familiar with the data in V_DIM_CARTONS_LOOSE. At a glance, it looks like it’s made up of some item configs provided by LSC, and some warehouse “throughput” configs. I’m not sure if the nature of that data is stable or dynamic. To the extent that any of these configs are stable attributes of the items, they potentially can get added AS attributes directly into NETSUITE2 (using either existing or new fields on Item records), and then reflected in DEV.NETSUITE2.DIM_ITEM
=============
|*/
    SELECT CAST(FULL_NAME AS varchar(50)) AS ITEM
        , ITEM_ID
        , TYPE_NAME
        , CAST(DISPLAYNAME AS varchar) AS DISPLAYNAME
        , CAST(PRODUCT_CATEGORY AS varchar) AS PRODUCT_CATEGORY
        , CAST(PRODUCT_FAMILY AS varchar) AS PRODUCT_FAMILY
        , TRUE_MASTER
        --, CAST(MASTERQTY AS varchar) AS MASTERQTY
        , ZEROIFNULL(MASTERQTY) AS MASTERQTY
        , EST_CT_PALLET
        , TOTAL_CARTONS_PER_1_QTY
        , MSTR_WEIGHT
        , MSTR_LENGTH
        , MSTR_WIDTH
        , MSTR_HEIGHT
        , LSC_L_W_H
    FROM DEV.NETSUITE2_FSA.V_DIM_CARTONS_LOOSE
)   
    
 ------------------------------------------------------------------------------------------------------------------------------------------------
 --            combine  the results from 3 sources ( xfer order/ open sales / PO/ assembly --
------------------------------------------------------------------------------------------------------------------------------------------------    
   
     -----------------  Xfer Order -----------------
, CTE_SOURCES_ASSIGN_PO AS (                 
    SELECT A.ORDER_NUMBER
        , A.UNIQUE_KEY
        , CAST(DDA_OVERRIDE_DATE AS date) AS DDA
        , A.TRANSACTION_TYPE
        , 0 AS TOTAL_AMT
        , A.ITEM 
        , A.ITEM_ID
        , NULL AS COMPONENT_ITEM_ID 
        , NULL AS COMPONENT_ITEM
        , A.Abs_QUANTITY AS QTY_ORDERED --- 0 AS QTY_ORDERED -- changed on 11/10/2022      
        , NULL AS COMPONENT_QTY_ORDERED
        , A.LOCATION_FROM AS LOCATION
        , 1 AS PRIORITY_LEVEl
        , 'XFER' AS SOURCETYPE
        , to_Char(A.NS_LINE_NUMBER) AS NS_LINE_NUMBER
        , TRY_TO_DATE('') AS ORG_DDA
        , CAST(A.TRANSACTION_ID AS VARCHAR(50)) AS TRANSACTION_ID  -- modified 11/10/2022 --   ,'' AS transaction_ID  -- added 8/9/2022 for NS upload
        , NULL AS line_ID -- added 8/9/2022 for NS upload
        --  ,'' AS DI_TYPE_NAME -- added 10/27/2022   
        , NULL AS SO_MULTISITE_ORDER --- added 11/16/2022
        , NULL AS SO_MATERIAL_SUPPORT_STATUS -- added 03/14/2023 Per Emma Greenstein   
        , NULL AS SO_AMPLIFY_INTEGRATION_STATUS -- added 03/14/2023 Per Emma Greenstein  
        , A.CREATE_DATE
        , NULL AS ESTIMATED_DELIVERY_DATE
        , NULL AS SALES_ORDER_TYPE
        /* 20230912 - KBY, RFS23-2652 - include Product Line column for Sample order info */
        , NULL::TEXT PO_PRODUCT_LINE
    FROM CTE_XFER A
    GROUP BY A.ORDER_NUMBER
        , A.UNIQUE_KEY
        , TO_DATE(DDA_OVERRIDE_DATE)
        , A.TRANSACTION_TYPE
        , A.ITEM
        , A.ITEM_ID
        , A.Abs_QUANTITY
        , A.LOCATION_FROM
        , A.NS_LINE_NUMBER
        , A.TRANSACTION_ID
        , A.CREATE_DATE
        /* 20230912 - KBY, RFS23-2652 - include Product Line column for Sample order info */
        , PO_PRODUCT_LINE

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
        , PRIORITY_LEVEl
        , 'OpenSO' AS SOURCETYPE
        , TO_CHAR(A.SOLI_LINE_NUMBER) AS NS_LINE_NUMBER
        , A.SOLI_DDA_ORIGINAL AS ORG_DDA
        , TO_CHAR(A.SO_TRANSACTION_ID)  -- added 8/9/2022 
        , TO_CHAR(A.SOLI_LINE_ID)  -- added 8/9/2022
        -- , A.DI_TYPE_NAME -- added 10/27/2022 to display if the item is Kit for validation 
        , A.SO_MULTISITE_ORDER --- added 11/16/2022    
        , A.SO_MATERIAL_SUPPORT_STATUS -- added 03/14/2023 Per Emma Greenstein   
        , A.SO_AMPLIFY_INTEGRATION_STATUS -- added 03/14/2023 Per Emma Greenstein 
        , A.CREATE_DATE
        , A.ESTIMATED_DELIVERY_DATE
        , A.SALES_ORDER_TYPE
        /* 20230912 - KBY, RFS23-2652 - include Product Line column for Sample order info */
        , NULL::TEXT PO_PRODUCT_LINE
    FROM CTE_OPENSALES A
    WHERE 
       /* 20230912 - KBY, RFS23-2652 - Allow Sample orders in to FSA */
        -- SO_TRANSACTION_TYPE_NEW != 'Sample (aka Internal fulfillment)' --   OLD: removing samples to get rid of sample demand 8/3/2022
        --- AND A.SO_DEFERRED is null -- remove all deferred revenue items 
        A.DEFERRED_REVENUE_BUCKETS IS NULL -- remove all deferred revenue items -- switch from so_deferred to DEFERRED_REVENUE_BUCKETS 8/3/2022
    GROUP BY A.SO_ORDER_NUMBER
        , A.SOLI_UNIQUE_KEY
        , A.SO_TRANSACTION_TYPE_NEW
        , TOTAL
        , A.soli_item
        , A.SOLI_ITEM_ID
        , A.COMPONENT_ITEM_ID
        , A.COMPONENT_ITEM
        , A.SOLI_QTY_ORDERED
        , A.COMPONENT_QTY_ORDERED
        , A.SOLI_LOCATION 
        , PRIORITY_LEVEl
        , A.SOLI_LINE_NUMBER
        , A.SOLI_DDA_ORIGINAL
        , A.SO_TRANSACTION_ID
        , A.SOLI_LINE_ID
        , A.SO_MULTISITE_ORDER 
        , A.SO_MATERIAL_SUPPORT_STATUS
        , A.SO_AMPLIFY_INTEGRATION_STATUS -- added 03/14/2023 Per Emma Greenstein  
        , A.CREATE_DATE
        , A.ESTIMATED_DELIVERY_DATE
        , A.SALES_ORDER_TYPE
        /* 20230912 - KBY, RFS23-2652 - include Product Line column for Sample order info */
        , PO_PRODUCT_LINE
      
UNION
   
-----------------  open po / Assembly  -----------------   

    SELECT A.ORDER_NUMBER 
        , A.UNIQUE_KEY
        , CAST(MAX(RECEIVE_BY_DATE) AS DATE) AS MAX_RECEIVE_BY_DATE
        , A.TYPE_NAME  
        , 0 AS TOTAL_AMT 
        , A.ITEM
        , A.ASSEMBLY_ELSE_ITEM_ID
        , A.COMPONENT_ITEM_ID
        , A.COMPONENT_ITEM
        , A.QUANITITY_TO_BE_FULFILLED
        , A.COMPONENT_QTY_TO_BE_FULFILLED
        , B.LOCATION
        , 1 AS PRIORITY_LEVEl
        , 'Assembly' AS SOURCETYPE
        , TO_CHAR(A.NS_LINE_NUMBER) AS NS_LINE_NUMBER
        , TRY_TO_DATE('') AS ORG_DDA
        /* 20230710 - KBY, Include Transaction ID's for Assembly as well */
        /* , NULL AS TRANSACTION_ID  -- added 8/9/2022 for NS upload */
        , CAST(A.PURCHASE_ORDER_TRANSACTION_ID AS VARCHAR(50)) AS TRANSACTION_ID
        , NULL AS LINE_ID -- added 8/9/2022 for NS upload
        --,'' AS DI_TYPE_NAME -- added 10/27/2022    
        , NULL AS SO_MULTISITE_ORDER --- added 11/16/2022
        , NULL AS SO_MATERIAL_SUPPORT_STATUS -- added 03/14/2023 Per Emma Greenstein   
        , NULL AS SO_AMPLIFY_INTEGRATION_STATUS -- added 03/14/2023 Per Emma Greenstein 
        , A.CREATE_DATE
        , NULL AS ESTIMATED_DELIVERY_DATE
        , NULL AS SALES_ORDER_TYPE
        /* 20230912 - KBY, RFS23-2652 - include Product Line column for Sample order info */
        , A.PO_PRODUCT_LINE
    FROM CTE_PO_DETAIL A 
    INNER JOIN DEV.NETSUITE2_FSA.V_OPENPO B
        ON A.ORDER_NUMBER = B.ORDER_NUMBER
        AND A.ASSEMBLY_ELSE_ITEM_ID = B.ASSEMBLY_ELSE_ITEM_ID
    WHERE 
        -- B.LOCATION IN ('hand2mind', 'BR Printers', 'JPS Graphics', 'LSC Owensville', 'Wards VWR', 'Not Yet Assigned')
        B.LOCATION IN ('BR Printers KY','BR Printers SJ','BR Printers CN',
                               'LSC Owensville','LSC Airwest','LSC Linn',
                               'Barrett Distribution','hand2mind','JPS Graphics',
                               'Not Yet Assigned'
                               ,'Booksource', 'Continuum') // 2023.05.18 Alex: FSA
        AND A.TYPE_NAME = 'Assembly'
        AND YEAR(RECEIVE_BY_DATE) >= 2022
    GROUP BY A.ORDER_NUMBER
        , A.PURCHASE_ORDER_TRANSACTION_ID -- 20230710 - KBY, Include Transaction ID's for Assembly as well
        , A.UNIQUE_KEY
        , A.TYPE_NAME
        , A.ITEM --, B.ISBN 
        , A.ASSEMBLY_ELSE_ITEM_ID
        , A.COMPONENT_ITEM_ID
        , A.COMPONENT_ITEM
        , A.QUANITITY_TO_BE_FULFILLED
        , A.COMPONENT_QTY_TO_BE_FULFILLED
        , B.LOCATION
        , A.NS_LINE_NUMBER
        , A.CREATE_DATE
        /* 20230912 - KBY, RFS23-2652 - include Product Line column for Sample order info */
        , A.PO_PRODUCT_LINE
) 
/* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment */
, CTE_SOURCES_ASSIGN_PO_FR AS
(
    SELECT *,
        CUSTBODY_FRPRDAYS AS FR_PREV_DAYS
      FROM CTE_SOURCES_ASSIGN_PO
      JOIN (
        SELECT CUSTBODY_FRPRDAYS,count(*) NUM
            FROM DEV.NETSUITE2_RAW_RESTRICT.TRANSACTION
            WHERE CUSTBODY_FRPRDAYS IS NOT NULL
            GROUP BY CUSTBODY_FRPRDAYS
            ORDER BY NUM DESC
            LIMIT 1
      )

)

 ------------------------------------------------------------------------------------------------------------------------------------------------
 -- results --
------------------------------------------------------------------------------------------------------------------------------------------------        

    SELECT A.*
        /* 20230711 - KBY, RFS23-1850 - Include inventories for all FSA locations, and for forward-facing locations only */
         , ZEROIFNULL(B.Total_AVAIL_QTY_NONFWD) + ZEROIFNULL(INV_FWD.TOTAL_AVAIL_QTY_FWD)    AS TOTAL_AVAIL_QTY
         , ZEROIFNULL(B.TOTAL_AVAIL_QTY_NONFWD)                                              AS TOTAL_AVAIL_QTY_NONFWD
         , ZEROIFNULL(INV_FWD.TOTAL_AVAIL_QTY_FWD)                                           AS TOTAL_AVAIL_QTY_FWD
         , CASE WHEN TOTAL_AVAIL_QTY >= A.QTY_ORDERED 
                THEN 'Available' 
                ELSE 'BackOrder' 
            END                                                             AS BO_STATUS
         , DI.TYPE_NAME                                                     AS TYPE_NAME
         , CAST(C.MASTERQTY AS varchar)                                     AS NUMBER_IN_CARTON
    /* 20230728 - KBY, RSF23-2033 - Include global parameter FR_PREV_DAYS for adjustment */
    FROM CTE_SOURCES_ASSIGN_PO_FR A
    LEFT OUTER JOIN DEV.NETSUITE2.DIM_ITEM DI 
        ON A.ITEM_ID = DI.ITEM_ID
    /* 20230711 - KBY, RFS23-1850 - Include inventories for all FSA locations, and for forward-facing locations only */
    LEFT OUTER JOIN CTE_INVENTORY_NONFWD B
        ON IFNULL(A.COMPONENT_ITEM_ID, A.ITEM_ID) = B.ITEM_ID 
    LEFT OUTER JOIN CTE_INVENTORY_FWD INV_FWD
        ON IFNULL(A.COMPONENT_ITEM_ID, A.ITEM_ID) = INV_FWD.ITEM_ID 
    LEFT OUTER JOIN CTE_CARTON C
        ON IFNULL(A.COMPONENT_ITEM_ID, A.ITEM_ID) = C.ITEM_ID     
    WHERE CAST(A.ORDER_NUMBER AS varchar) NOT LIKE ('%Planning%')
    AND IFNULL(A.COMPONENT_ITEM::TEXT, '0') NOT IN (SELECT COMPONENT_ITEM::TEXT FROM DEV.NETSUITE2_FSA.COMPONENT_ITEMS_TO_EXCLUDE)
    /* 20230614 - KBY, HyperCare Ref #129 - also exclude ITEMs that appear on COMPONENT_ITEM exclusion list */
    AND IFNULL(A.ITEM::TEXT, '0') NOT IN (SELECT COMPONENT_ITEM::TEXT FROM DEV.NETSUITE2_FSA.COMPONENT_ITEMS_TO_EXCLUDE)
    ) AS "v_0000003085_0015756651"
)
;