create or replace view V_OPENPO(
	ITEM_ID,
	ITEM,
	BILL_OF_MATERIALS_ID,
	BOM_NAME,
	ITEM_C,
	ITEM_ID_C,
	ITEM_DISPLAY_NAME,
	TRANSACTION_TYPE,
	ASSEMBLY_ELSE_ITEM_ID,
	ASSEMBLY_ITEM_ID,
	ASSEMBLY_ITEM,
	ASSEMBLY_ITEM_DISPLAY_NAME,
	ORDER_NUMBER,
	PURCHASE_ORDER_TRANSACTION_ID,
	STATUS,
	LOCATION,
	RECEIVE_BY_DATE,
	UNIQUE_KEY,
	QUANTITY,
	QUANTITY_RECEIVED,
	QUANITITY_TO_BE_RECEIVED,
	QUANITITY_TO_BE_RECEIVED_90,
	ITEM_TYPE,
	COMPONENT_TYPE
) as (SELECT 
  "ITEM_ID", 
  "ITEM", 
  "BILL_OF_MATERIALS_ID", 
  "BOM_NAME", 
  "ITEM_C", 
  "ITEM_ID_C", 
  "ITEM_DISPLAY_NAME", 
  "TRANSACTION_TYPE", 
  "ASSEMBLY_ELSE_ITEM_ID", 
  "ASSEMBLY_ITEM_ID", 
  "ASSEMBLY_ITEM", 
  "ASSEMBLY_ITEM_DISPLAY_NAME", 
  "ORDER_NUMBER", 
  "PURCHASE_ORDER_TRANSACTION_ID", 
  "STATUS", 
  "LOCATION", 
  "RECEIVE_BY_DATE", 
  "UNIQUE_KEY", 
  "QUANTITY", 
  "QUANTITY_RECEIVED", 
  "QUANITITY_TO_BE_RECEIVED", 
  "QUANITITY_TO_BE_RECEIVED_90",
  "ITEM_TYPE",
  "COMPONENT_TYPE"
  
FROM (
SELECT  
    i.item_id
    ,i.full_name as Item 
    ,ic.bill_of_materials_ID
    ,ic.bom_name
    ,i_component.full_name  as Item_C
    ,ic.component_item_id as Item_ID_C  
    ,i.display_name as ITEM_DISPLAY_NAME
    ,'Purchase Order' as TRANSACTION_TYPE
    ,polia.ASSEMBLY_ELSE_ITEM_ID
    ,i_component.item_id as assembly_item_id
    ,i_component.full_name as assembly_item
    ,i_component.display_name as assembly_item_display_name
    ,po.order_number
    ,po.PURCHASE_ORDER_TRANSACTION_ID
    ,po.status 
    ,po.LOCATION
    ,polia.RECEIVE_BY_DATE   
    ,polia.UNIQUE_KEY    
    ,polia.QUANTITY 
    ,QUANTITY_RECEIVED
    ,(polia.QUANTITY*.9)-ZEROIFNULL(QUANTITY_RECEIVED) as QUANITITY_TO_BE_RECEIVED
    ,(polia.QUANTITY*.9) as QUANITITY_TO_BE_RECEIVED_90
    ,i.TYPE_NAME AS "ITEM_TYPE"
    ,i_component.TYPE_NAME AS "COMPONENT_TYPE"
    
FROM NETSUITE2_FSA.NS_PURCHASE_ORDER_LINE_ITEM_AUX polia 
   JOIN DEV.NETSUITE2.FACT_PURCHASE_ORDER_LINE_ITEM poli 
       on polia.UNIQUE_KEY = poli.UNIQUE_KEY 
   JOIN DEV.NETSUITE2.FACT_PURCHASE_ORDER po 
       on polia.PURCHASE_ORDER_TRANSACTION_ID = po.PURCHASE_ORDER_TRANSACTION_ID 
   JOIN "DEV"."NETSUITE2"."DIM_ITEM" i 
       on polia.ASSEMBLY_ELSE_ITEM_ID = i.ITEM_ID
   JOIN "DEV"."NETSUITE2"."FACT_TRANSACTION_LINE" ftl -- Get the Main Line to get the Product Line (Class) of the PO
       on po.PURCHASE_ORDER_TRANSACTION_ID = ftl.TRANSACTION_ID      
       and ftl.TRANSACTION_LINE_ID = 0    
   LEFT JOIN DEV.NETSUITE2_FSA.NS_ITEMS_COMPONENTS ic
        on polia.ASSEMBLY_ELSE_ITEM_ID = ic.ITEM_ID
        and ic.ITEM_TYPE = 'Assembly'
   LEFT JOIN DEV.NETSUITE2.DIM_ITEM i_component
        on ic.COMPONENT_ITEM_ID = i_component.ITEM_ID
        and i_component.type_name NOT IN ('Non-inventory Item', 'Non-inventory Item for Resale', 'Kit Part') // 2023.05.15 Alex: FSA
   JOIN DEV.NETSUITE2_RAW_RESTRICT.LOCATION loc //join added for RFS23-1189. Restrict to certain locations.
        on po.LOCATION = loc.NAME
        and (CUSTRECORD_FSA_LOCATION_RELEVANT = 'T'
             or loc.NAME IN ('Booksource', 'Continuum')) // 2023.05.18 Alex: FSA                             
WHERE polia._POLI_IS_RECEIVED = 'FALSE'
       and po.order_number  not like  ('Planning%') --10/27/2022
       and po.status not in ('Closed','Fully Billed') //2023.04.04:JB:added this condition for RFS23-1190
       and ftl.CLASS_NAME not in ('General','Other')
ORDER BY item_Id, polia.RECEIVE_BY_DATE

) AS "v_0000003085_0015756644");