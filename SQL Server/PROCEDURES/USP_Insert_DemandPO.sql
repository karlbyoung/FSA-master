

create PROCEDURE [dbo].[USP_INSERT_DemandPO]

AS
BEGIN

TRUNCATE TABLE dbo.Demand_PO
;

INSERT INTO [dbo].[Demand_PO]
           (OrderNumber, 
		   UNIQUE_KEY,
		   [DDA]
		   ,Org_DDA
           ,[TRANS_TYPE]
		   ,TOTAL_AMT
		   ,Priority_Level
		   ,[NS_LINE_NUMBER]
		   ,[ITEM]
		   ,[ITEM_ID]
		   ,COMPONENT_ITEM_ID
		  ,[COMPONENT_ITEM]
		  ,[QUANTITY]
		  ,[TOTAL_AVAIL_QTY]
		  ,[COMPONENT_QTY_ORDERED]
		   ,[LOCATION]
		   ,[BO_STATUS]
		   ,[SOURCETYPE]
		   ,[SourceLoadDate_Current]
		   )

  SELECT [ORDER_NUMBER]
	,UNIQUE_KEY
      ,[DDA]
	  ,Org_DDA
      ,[TRANSACTION_TYPE]
      ,[TOTAL_AMT]
      ,[PRIORITY_LEVEL]
	  ,[NS_LINE_NUMBER]
      ,[ITEM]
      ,[ITEM_ID]
	  ,COMPONENT_ITEM_ID
      ,[COMPONENT_ITEM]
      ,[QUANTITY]
      ,[TOTAL_AVAIL_QTY]
      ,[COMPONENT_QTY_ORDERED]
      ,[LOCATION]
      ,[BO_STATUS]
      ,[SOURCETYPE]
	  ,InsertDate
  FROM [FSA_DB].[dbo].[Demand_PO_LOAD]
  order by [TRANSACTION_TYPE], [DDA]
;

update a 
set a.FK_ID = b.ID
from [dbo].[Demand_PO] a inner join [Demand_Priority] b
on a.TRANS_TYPE = b.Seq_Desc
and a.[Priority_Level] = b.Priority
;

update a 
set a.[Priority_Level] = 4
from [dbo].[Demand_PO] a 
where a.fk_id is null 
;
update a 
set a.FK_ID = b.ID
--select a.*, b.id 
from [dbo].[Demand_PO] a inner join [Demand_Priority] b
on a.TRANS_TYPE = b.Seq_Desc
and a.[Priority_Level] = b.Priority
and a.fk_ID is  null 
;
update a 
set a.FK_ID = b.ID
from [dbo].[Demand_PO] a inner join [Demand_Priority] b
on a.TRANS_TYPE = b.Seq_Desc
where FK_ID is null 
and TRANS_TYPE = 'PF80' 
;

;with cte as (
select ordernumber, min(dda_modified) as Min_DDA
from  [dbo].[Demand_PO]
--where OrderNumber in ('PQ 191008-132384', 'PQ 210608-179093')
group by ordernumber
) 
update a 

set a.[Sequencing_DDA] = b.Min_DDA

from [dbo].[Demand_PO] a 
inner join cte b
on a.OrderNumber = b.OrderNumber
;

END
GO


