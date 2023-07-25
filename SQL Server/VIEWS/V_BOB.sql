

ALTER view [dbo].[V_BOB] 

as with cte_SetIFDate as (select orderno
--, FK_SPA_ID,SourceLoadDate,
					,min(receiptdate) as Min_ReceiptDt, max(receiptdate) as Max_ReceiptDt
					--[dbo].[AddBusinessDays](receiptdate, 15)
					,datediff( dd, min(receiptdate), max(receiptdate)) as DayRangebyOrderNO
					,[dbo].[fn_WorkDays] (min(receiptdate), max(receiptdate)) as BizDayRangebyOrderNO
					,case when [dbo].[fn_WorkDays] ( min(receiptdate), max(receiptdate)) > 14
					 then 'Y'
					 Else 'N' end as is_15_bizdays
					 ,min(receiptdate) IF_bucket1

					 ,case when [dbo].[fn_WorkDays] ( min(receiptdate), max(receiptdate)) > 14
					 then    [dbo].[AddBusinessDays](min(receiptdate), 15)
						 Else  [dbo].[AddBusinessDays](min(receiptdate), 7)
						End IF_bucket2
					,case when [dbo].[fn_WorkDays] ( min(receiptdate), max(receiptdate)) > 14
						   then  [dbo].[AddBusinessDays](min(receiptdate), 30)
						 Else  [dbo].[AddBusinessDays](min(receiptdate), 14)
						 End IF_bucket3

					,case when [dbo].[fn_WorkDays] ( min(receiptdate), max(receiptdate)) > 14
						   then  [dbo].[AddBusinessDays](min(receiptdate), 45)
						 Else  [dbo].[AddBusinessDays](min(receiptdate), 21)
						 End IF_bucket4
					,case when [dbo].[fn_WorkDays] ( min(receiptdate), max(receiptdate)) > 14
						   then  [dbo].[AddBusinessDays](min(receiptdate), 60)
						 Else  [dbo].[AddBusinessDays](min(receiptdate), 28)
						 End IF_bucket5
					,case when [dbo].[fn_WorkDays] ( min(receiptdate), max(receiptdate)) > 14
						   then  [dbo].[AddBusinessDays](min(receiptdate), 75)
						 Else  [dbo].[AddBusinessDays](min(receiptdate), 35)
						 End IF_bucket6
					,case when [dbo].[fn_WorkDays] ( min(receiptdate), max(receiptdate)) > 14
						   then  [dbo].[AddBusinessDays](min(receiptdate), 90)
						 Else  [dbo].[AddBusinessDays](min(receiptdate), 42)
						 End IF_bucket7
					,case when [dbo].[fn_WorkDays] ( min(receiptdate), max(receiptdate)) > 14
						   then  [dbo].[AddBusinessDays](min(receiptdate), 105)
						 Else  [dbo].[AddBusinessDays](min(receiptdate), 49)
						 End IF_bucket8
					,case when [dbo].[fn_WorkDays] ( min(receiptdate), max(receiptdate)) > 14
						   then  [dbo].[AddBusinessDays](min(receiptdate), 120)
						 Else  [dbo].[AddBusinessDays](min(receiptdate), 56)
						 End IF_bucket9
					,case when [dbo].[fn_WorkDays] ( min(receiptdate), max(receiptdate)) > 14
						   then  [dbo].[AddBusinessDays](min(receiptdate), 135)
						 Else  [dbo].[AddBusinessDays](min(receiptdate), 63)
						 End IF_bucket10
					,case when [dbo].[fn_WorkDays] ( min(receiptdate), max(receiptdate)) > 14
						   then  [dbo].[AddBusinessDays](min(receiptdate), 150)
						 Else  [dbo].[AddBusinessDays](min(receiptdate), 70)
						 End IF_bucket11
					,case when [dbo].[fn_WorkDays] ( min(receiptdate), max(receiptdate)) > 14
						   then  [dbo].[AddBusinessDays](min(receiptdate), 165)
						 Else  [dbo].[AddBusinessDays](min(receiptdate), 77)
						 End IF_bucket12
					,case when [dbo].[fn_WorkDays] ( min(receiptdate), max(receiptdate)) > 14
						   then  [dbo].[AddBusinessDays](min(receiptdate), 180)
						 Else  [dbo].[AddBusinessDays](min(receiptdate), 84)
						 End IF_bucket13	
					,case when [dbo].[fn_WorkDays] ( min(receiptdate), max(receiptdate)) > 14
						   then  [dbo].[AddBusinessDays](min(receiptdate), 195)
						 Else  [dbo].[AddBusinessDays](min(receiptdate), 91)
						 End IF_bucket14
					 
					FROM[dbo].[SOLI]
					--where orderno = 'PQ 180921-110470-2022'
					--where active = 'Y'
					group by orderno
					--,FK_SPA_ID,SourceLoadDate
					)
,cte_Setbuckets as (select 
a.orderno,a.UNIQUE_KEY, a.NS_LINE_NUMBER, a.itemno,a.receiptdate,a.FREDD,a.FK_SPA_ID,a.SourceLoadDate,
--[dbo].[AddBusinessDays](receiptdate, 12) as receiptDate_add_10biz 
[dbo].[AddBusinessDays](receiptdate, 12) as receiptDate_add_12biz 
,CASE
  WHEN [dbo].[AddBusinessDays](receiptdate, 12) between IF_bucket1 and IF_bucket2 
		Then 'bucket1'
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket2 and IF_bucket3 
		Then 'bucket2'
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket3 and IF_bucket4  
	    Then 'bucket3'
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket4 and IF_bucket5 
	    Then 'bucket4'
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket5 and IF_bucket6 
	    Then 'bucket5'
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket6 and IF_bucket7 
	    Then 'bucket6'
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket7 and IF_bucket8 
	    Then 'bucket7'
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket8 and IF_bucket9 
	    Then 'bucket8'
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket9 and IF_bucket10 
	    Then 'bucket9'
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket10 and IF_bucket11 
	    Then 'bucket10'
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket11 and IF_bucket12
	Then 'bucket11'
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket12 and IF_bucket13
	Then 'bucket12'
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket13 and IF_bucket14
	Then 'bucket13'
	end as Bucket_on_FREDD 
,CASE
  WHEN [dbo].[AddBusinessDays](receiptdate, 12) between IF_bucket1 and IF_bucket2 
		Then IF_bucket1
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket2 and IF_bucket3 
		Then IF_bucket2
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket3 and IF_bucket4  
	    Then IF_bucket3
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket4 and IF_bucket5 
	    Then IF_bucket4
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket5 and IF_bucket6 
	    Then IF_bucket5
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket6 and IF_bucket7 
	    Then IF_bucket6
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket7 and IF_bucket8 
	    Then  IF_bucket7
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket8 and IF_bucket9 
	    Then IF_bucket8
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket9 and IF_bucket10 
	    Then IF_bucket9
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket10 and IF_bucket11 
	    Then IF_bucket10
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket11 and IF_bucket12
	Then IF_bucket11
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket12 and IF_bucket13
	Then IF_bucket12
	WHEN [dbo].[AddBusinessDays](receiptdate, 12)  between IF_bucket13 and IF_bucket14
	Then IF_bucket13
	end as bucket_Date 

,CASE
  --WHEN receiptdate <=   IF_bucket1 
		--Then 'bucket1'
	WHEN receiptdate  between IF_bucket1 and IF_bucket2 
		Then 'bucket2'
	WHEN receiptdate  between IF_bucket2 and IF_bucket3  
	    Then 'bucket3'
	WHEN receiptdate between IF_bucket3 and IF_bucket4  
	    Then 'bucket4'
	WHEN receiptdate between IF_bucket4 and IF_bucket5 
	    Then 'bucket5'
	WHEN receiptdate  between IF_bucket5 and IF_bucket6 
	    Then 'bucket6'
	WHEN receiptdate between IF_bucket6 and IF_bucket7 
	    Then 'bucket7'
	WHEN receiptdate  between IF_bucket7 and IF_bucket8 
	    Then 'bucket8'
	WHEN receiptdate between IF_bucket8 and IF_bucket9 
	    Then 'bucket9'
	WHEN receiptdate between IF_bucket9 and IF_bucket10 
	    Then 'bucket10'
	WHEN receiptdate between IF_bucket10 and IF_bucket11 
	Then 'bucket11'
	WHEN receiptdate between IF_bucket11 and IF_bucket12
	Then 'bucket12'
	WHEN receiptdate  between IF_bucket12 and IF_bucket13
	Then 'bucket13'
	WHEN receiptdate between IF_bucket13 and IF_bucket14
	Then 'bucket14'
	end as Bucket_on_Receipt
,CASE
  --WHEN receiptdate <=   IF_bucket1 
		--Then IF_bucket1
	WHEN receiptdate  between IF_bucket1 and IF_bucket2 
		Then IF_bucket2
	WHEN receiptdate  between IF_bucket2 and IF_bucket3  
	    Then IF_bucket3
	WHEN receiptdate between IF_bucket3 and IF_bucket4  
	    Then IF_bucket4
	WHEN receiptdate between IF_bucket4 and IF_bucket5 
	    Then IF_bucket5
	WHEN receiptdate  between IF_bucket5 and IF_bucket6 
	    Then IF_bucket6
	WHEN receiptdate between IF_bucket6 and IF_bucket7 
	    Then IF_bucket7
	WHEN receiptdate  between IF_bucket7 and IF_bucket8 
	    Then IF_bucket8
	WHEN receiptdate between IF_bucket8 and IF_bucket9 
	    Then IF_bucket9
	WHEN receiptdate between IF_bucket9 and IF_bucket10 
	    Then IF_bucket10
	WHEN receiptdate between IF_bucket10 and IF_bucket11 
	Then IF_bucket11
	WHEN receiptdate between IF_bucket11 and IF_bucket12
	Then IF_bucket12
	WHEN receiptdate  between IF_bucket12 and IF_bucket13
	Then IF_bucket13
	WHEN receiptdate between IF_bucket13 and IF_bucket14
	Then IF_bucket14
	end as BucketDT_on_Receipt
	,b.is_15_bizdays
	,b.IF_bucket1,b.IF_bucket2, b.IF_bucket3, b.IF_bucket4, b.IF_bucket5, b.IF_bucket6
	, b.IF_bucket7, b.IF_bucket8, b.IF_bucket9, b.IF_bucket10, b.IF_bucket11, b.IF_bucket12, b.IF_bucket13, b.IF_bucket14
--	,a.active
from [dbo].[SOLI] a left outer join cte_SetIFDate b 
on a.orderno = b.orderno 
--and a.FK_SPA_ID = b.FK_SPA_ID
--where a.orderno = 'PQ 180921-110470-2022'
--and a.active = 'Y'
)
,cte_capping_DDA as (
select A.OrderNo--A.ItemNo,a.FREDD,a.receiptdate, a.New_DDA ,

,Bucket_on_Receipt, max(A.FREDD) as capping_DDA
from cte_Setbuckets a  LEFT OUTER JOIN[dbo].[SOLI]  B
ON A.OrderNo  = B.OrderNo AND  A.ITEMNO = B.ITEMNO and  a.FK_SPA_ID = b.FK_SPA_ID
--GROUP BY A.OrderNo, A.ItemNo,a.FREDD,a.receiptdate, a.New_DDA ,A.BUCKET 
group by  A.OrderNo, Bucket_on_Receipt
)


select A.OrderNo, a.UNIQUE_KEY,a.NS_LINE_NUMBER, A.ItemNo,a.FK_SPA_ID,a.receiptdate, a.FREDD,A.Bucket_on_Receipt , BucketDT_on_Receipt, b.capping_DDA
--,[dbo].[AddBusinessDays](BucketDT_on_Receipt, 12) as New_DDA_base_BucketDt_12

,a.is_15_bizdays
,a.IF_bucket1,a.IF_bucket2, a.IF_bucket3, a.IF_bucket4, a.IF_bucket5, a.IF_bucket6
	, a.IF_bucket7, a.IF_bucket8, a.IF_bucket9, a.IF_bucket10, a.IF_bucket11, a.IF_bucket12, a.IF_bucket13, a.IF_bucket14,a.SourceLoadDate
from cte_Setbuckets a left outer join cte_capping_DDA b
on a.OrderNo = b.OrderNo and a.Bucket_on_Receipt = b.Bucket_on_Receipt 
--where a.orderno =  'PQ 190423-119762'
---where a.orderno = 'PQ 190423-119762'
--order by a.orderno, a.receiptdate
GO


