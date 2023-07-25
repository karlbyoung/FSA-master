


CREATE View [dbo].[V_FSA] as (select a.*, b.Order_Number as bob_Order_Number, b.item as BOB_Item
      ,[FREDD]
      ,[Bucket_on_Receive_By_Date]
      ,[BucketDT_on_Receive_By_Date]
      ,[capping_DDA]
	  ,[is_15_bizdays]
      ,[IF_bucket1]
      ,[IF_bucket2]
      ,[IF_bucket3]
      ,[IF_bucket4]
      ,[IF_bucket5]
      ,[IF_bucket6]
      ,[IF_bucket7]
      ,[IF_bucket8]
      ,[IF_bucket9]
      ,[IF_bucket10]
      ,[IF_bucket11]
      ,[IF_bucket12]
      ,[IF_bucket13]
      ,[IF_bucket14]
	  , case when [Org_DDA] is null then [DDA]
	  else [Org_DDA] end as [FSA Updated Original DDA]
from dbo.[Sequencing_PO_Assign] a inner join  bob b
on  a.id = b.[FK_SPA_ID]
--order by  rowno
)
GO


