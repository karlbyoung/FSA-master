
ALTER PROCEDURE [dbo].[USP_INSERT_Sequencing_PO_Assign]

AS
BEGIN

truncate table dbo.Sequencing_PO_Assign
;
 insert into dbo.sequencing_PO_Assign

			([id]
           ,[ordernumber]
		   ,UNIQUE_KEY
           ,[Priority]
           ,[seq]
           ,[DDA]
		   ,Org_DDA
           ,[Transaction_Type]
           ,[Rowno]
		   ,NS_LINE_NUMBER
           ,[Item]
           ,[ITEM_ID]
		    ,[COMPONENT_ITEM_ID]
           ,[COMPONENT_ITEM]
           ,[LOCATION]
           ,[Item_RowNo]
          -- ,[BO_STATUS]
		   ,[QUANTITY]
           ,[TOTAL_AVAIL_QTY]
             ,[sum_Rollup]
           ,[remaining_Total_QTY]
           ,[SourceType]
		   ,SourceType_Daily
		   ,sourceLoadDate
		   ,Po_slippage
           ,[PO_Indicator]
           ,[PO_Borrow]
		   
           )
Select 
*
,SIGN( remaining_Total_QTY) as PO_Indicator
,case when SIGN( remaining_Total_QTY) < 0   then  'PO_BackOrder'
else 'No backorder' end  as PO_Borrow
--, SUM(QUANTITY ) OVER (partition by item_rowno,  SIGN( remaining_Total_QTY) 
--						ORDER BY RowNo 
--                      ROWS BETWEEN UNBOUNDED PRECEDING 
--                      AND CURRENT ROW) AS sum_Item_Rollup


from [dbo].[V_Sequencing_PO]
--where Item_Id = '20220'
--and SIGN( remaining_Total_QTY) = -1

order by rowno

;

----------------------------------------


;with cte as (select ROW_NUMBER() OVER(PARTITION BY Item_ID  ORDER BY rowno, id  ) as PO_ID_1,* 
, SUM(QUANTITY ) OVER (partition by item_rowno, po_rowno, SIGN( remaining_Total_QTY) 
						ORDER BY RowNo 
                      ROWS BETWEEN UNBOUNDED PRECEDING 
                      AND CURRENT ROW) AS sum_Item_Rollup1
from dbo.Sequencing_PO_Assign
--where ITEM_ID = '20220'
--AND ID = '150638'
where  po_indicator  = -1 
--and ITEM_ID = '20220'
--and po_rowno is null 
--order by item_id,rowno asc  , id asc;

)

update a
set a.po_ID = b.PO_ID_1

from Sequencing_PO_Assign a join cte b 
on a.id = b.id 



END
