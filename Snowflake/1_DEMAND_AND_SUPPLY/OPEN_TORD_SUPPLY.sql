/* Create table representing Transfer Orders for supply */
CREATE OR REPLACE TABLE DEV.${vj_fsa_schema}.OPEN_TORD_SUPPLY as
WITH cte_xfer AS (
    SELECT 
        ABS_QUANTITY
        , TRANSACTION_ID
        , QUANTITY_FULFILLED                                   
        , ORDER_NUMBER
        , TRANSFER_ORDER_TRANSACTION_ID
        , STATUS
        , CREATE_DATE
        , TRANSACTION_DATE
        , LOCATION_FROM
        , LOCATION_TO
        , MEMO
        , IS_DELETED
        , IS_SERVICE_PO
        , DDA_OVERRIDE_DATE
        , HEADER_TRANSACTION_TYPE
        , TRANSFER_ORDER_TYPE_ID
        , TRANSFER_ORDER_TYPE
        , REQUESTED_DDA
        , UNIQUE_KEY
        , ITEM
        , ITEM_ID
        , ITEM_DISPLAY_NAME
        , QUANTITY
        , TOLI_DDA
        , DDB
        , TRANSACTION_TYPE
        , IS_CLOSED
        , TRANSACTION_LINE_TYPE
        , DATE_CREATED
        , DATE_LAST_MODIFIED
        , DATE_DELETED
        , QUANTITY_COMMITTED
        , QUANTITY_PACKED
        , QUANTITY_PICKED
        , TRANSACTION_LINE_ID
        , NS_LINE_NUMBER
        , TRANSFER_ORDER_TYPE_MOD
        , DDA
    FROM DEV.${vj_fsa_schema}.OPEN_TORD_ALL
    WHERE
        TRANSFER_ORDER_TYPE_MOD in ('Fulfillment','Assembly')
        AND IFNULL(QUANTITY_FULFILLED, 0) < ABS_QUANTITY
        AND IFNULL(QUANTITY_COMMITTED, 0) > 0
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