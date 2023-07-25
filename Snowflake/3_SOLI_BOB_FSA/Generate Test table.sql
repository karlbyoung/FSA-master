CREATE OR REPLACE TABLE DEV.${FSA_PROD_SCHEMA}.FSA_BOOMI_TEST AS
  WITH some_customers AS    -- Select customers with SOLI_START_DATE in the last four days
  (
    SELECT distinct so_customer_id
      FROM DEV.${FSA_PROD_SCHEMA}.V_SO_MASTER
      WHERE SOLI_START_DATE >= DATEADD('day',-4,CURRENT_DATE())
  )
  , some_orders as          -- Select two orders from above customers that are in FSA
  (
    SELECT distinct o.SO_ORDER_NUMBER,o.so_customer_id 
      FROM DEV.${FSA_PROD_SCHEMA}.V_SO_MASTER o
      JOIN DEV.${FSA_PROD_SCHEMA}.FSA f
        ON o.SO_ORDER_NUMBER = f.ORDER_NUMBER
    WHERE SO_CUSTOMER_ID in (select so_customer_id from some_customers)
    LIMIT 2
  )
  , test_orders as         -- Find any ORDER_NUMBERS for special Test Customer
  (
   	SELECT distinct SO_ORDER_NUMBER,so_customer_id
    	FROM DEV.${FSA_PROD_SCHEMA}.V_SO_MASTER
	 WHERE SO_CUSTOMER_ID = 866912
  )
  SELECT *
    FROM DEV.${FSA_PROD_SCHEMA}.FSA
      WHERE order_number IN (SELECT so_order_number FROM test_orders UNION SELECT so_order_number from some_orders)
;
