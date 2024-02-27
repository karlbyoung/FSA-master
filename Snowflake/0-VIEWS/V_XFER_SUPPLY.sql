WITH cte_xfer AS (
    SELECT ABS(TOLI.QUANTITY) AS Abs_QUANTITY
        , TOLI.TRANSFER_ORDER_TRANSACTION_ID AS TRANSACTION_ID
        , IFNULL(TOLI.QUANTITY_FULFILLED, 0) AS QUANTITY_FULFILLED                                   
        , TORD.ORDER_NUMBER
        , TORD.TRANSFER_ORDER_TRANSACTION_ID
        , TORD.STATUS
        , TORD.CREATE_DATE
        , TORD.TRANSACTION_DATE
        , TORD.LOCATION_FROM
        , TORD.LOCATION_TO
        , TORD.MEMO
        , TORD.IS_DELETED
        , TORD.IS_SERVICE_PO
        , TORD.DDA_OVERRIDE_DATE
        , TORD.TRANSACTION_TYPE AS HEADER_TRANSACTION_TYPE
        , TORD.TRANSFER_ORDER_TYPE_ID
        , TORD.TRANSFER_ORDER_TYPE
        , TORD.REQUESTED_DDA
        , TOLI.UNIQUE_KEY
        , TOLI.ITEM
        , TOLI.ITEM_ID
        , TOLI.ITEM_DISPLAY_NAME
        , TOLI.QUANTITY
        , TOLI.DDA TOLI_DDA
        , TOLI.DDB
        , TOLI.TRANSACTION_TYPE
        , TOLI.IS_CLOSED
        , TOLI.TRANSACTION_LINE_TYPE
        , TOLI.DATE_CREATED
        , TOLI.DATE_LAST_MODIFIED
        , TOLI.DATE_DELETED
        , TOLI.QUANTITY_COMMITTED
        , TOLI.QUANTITY_PACKED
        , TOLI.QUANTITY_PICKED
        , TOLI.TRANSACTION_LINE_ID
        , TOLI.NS_LINE_NUMBER
        /* 20240117 - KBY, RFS23-3950 Temporary CASE statement until TRANSFER_ORDER_TYPES are filled in */
        , CASE
            WHEN TORD.TRANSFER_ORDER_TYPE IS NULL AND TORD.LOCATION_TO ILIKE '%depo%' THEN 'Depo Stock Request'
            ELSE TORD.TRANSFER_ORDER_TYPE
          END AS TRANSFER_ORDER_TYPE_MOD
        /* 20240216 - KBY, RFS23-3951,3952 Adjust DDA of qualifying Transfer orders */
        , TORD.REQUESTED_DDA::DATE                                         AS DDA
    FROM DEV.${vj_ns2_schema}.FACT_TRANSFER_ORDER_LINE_ITEM TOLI
    JOIN DEV.${vj_ns2_schema}.FACT_TRANSFER_ORDER TORD
        ON TOLI.TRANSFER_ORDER_TRANSACTION_ID = TORD.TRANSFER_ORDER_TRANSACTION_ID
    WHERE
        TORD.STATUS NOT IN ('Closed', 'Cancelled')
        AND TRANSFER_ORDER_TYPE_MOD in ('Fulfillment','Assembly')
        AND IFNULL(TOLI.QUANTITY_FULFILLED, 0) < ABS(TOLI.QUANTITY)
        AND IFNULL(TOLI.QUANTITY_COMMITTED, 0) > 0
        AND TOLI.IS_CLOSED = 'F'
)
, xfer_composite as (
    SELECT cte_xfer.*
        , ic.bill_of_materials_id
        , i_component.full_name as item_c
        , ic.component_item_id as item_id_c
        , i_component.item_id as assembly_item_id
        , i_component.full_name as assembly_item
        , i_component.display_name as assembly_item_display_name
    FROM cte_xfer
    LEFT JOIN DEV.${vj_fsa_schema}.NS_ITEMS_COMPONENTS ic
        on cte_xfer.ITEM_ID = ic.ITEM_ID
        and ic.ITEM_TYPE = 'Assembly'
    LEFT JOIN DEV.${vj_ns2_schema}.DIM_ITEM i_component
        on ic.COMPONENT_ITEM_ID = i_component.ITEM_ID
        and i_component.TYPE_NAME NOT IN ('Non-inventory Item', 'Non-inventory Item for Resale', 'Kit Part')
)
select * from xfer_composite