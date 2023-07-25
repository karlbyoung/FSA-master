


CREATE View [dbo].[V_Sequencing_PO] as 
 with cte as ( select a.id, a.ordernumber,a.UNIQUE_KEY, b.Priority ,	DDA, 					
 Org_DDA, b.seq, a.TRANS_TYPE AS [Transaction_Type], a.TOTAL_AMT
,DENSE_RANK()  OVER(ORDER BY b.Priority, a.Sequencing_DDA, b.seq ,  a.ordernumber,a.TOTAL_AMT desc) AS Rowno
	,NS_LINE_NUMBER
      ,[Item]
      ,[ITEM_ID]
	  ,[COMPONENT_ITEM_ID]
      ,[COMPONENT_ITEM]
      ,[QUANTITY]
      ,[TOTAL_AVAIL_QTY]
      ,[COMPONENT_QTY_ORDERED]
      ,[LOCATION]
      ,[BO_STATUS]
       ,[SourceType]
	   ,SourceType_Daily
	   ,a.SourceLoadDate_Current as sourceLoadDate
	   ,a.PO_Splippage
  from FSA_DB.[dbo].[Demand_PO] a left outer join [Demand_Priority] b
  on a.fk_ID = b.id 
 --order by b.Priority asc,a.DDA_Modified asc ,  b.seq asc, a.OrderNumber asc ,  a.TOTAL_AMT desc 
-- order by Row#
 )
 select id, ordernumber,UNIQUE_KEY, Priority , seq, DDA,Org_DDA,[Transaction_Type]
 ,Rowno,NS_LINE_NUMBER,[Item],[ITEM_ID],[COMPONENT_ITEM_ID],[COMPONENT_ITEM],[LOCATION]
		--,row_number()  OVER (partition by Item order by rowno) as [Item_RowNo]
		--,DENSE_RANK()  OVER (ORDER BY  Item) as [Item_RowNo]
		--,[BO_STATUS]
  --     ,[TOTAL_AVAIL_QTY] 
	
  --    , SUM(case when [BO_STATUS] = 'Available' then  QUANTITY else 0 end ) OVER (partition by Item ,bo_status
		--			ORDER BY item, RowNo ,bo_status desc
  --                    ROWS BETWEEN UNBOUNDED PRECEDING 
  --                    AND CURRENT ROW) AS sum_Rollup


,DENSE_RANK()  OVER (ORDER BY  Item) as [Item_RowNo]
		--,[BO_STATUS]
		,[QUANTITY]
       ,[TOTAL_AVAIL_QTY] 
	 
      , SUM(QUANTITY  ) OVER (partition by Item --,bo_status
					ORDER BY item, RowNo 
                      ROWS BETWEEN UNBOUNDED PRECEDING 
                      AND CURRENT ROW) AS sum_Rollup 

  --    ,case when [BO_STATUS] = 'Available' then 
		--[TOTAL_AVAIL_QTY] - SUM(case when [BO_STATUS] = 'Available' then  QUANTITY else 0 end ) OVER (partition by Item ,bo_status 
		--			ORDER BY item, RowNo ,bo_status desc
  --                    ROWS BETWEEN UNBOUNDED PRECEDING 
  --                    AND CURRENT ROW)
		--else 0 end as remaining_Total_QTY

 ,[TOTAL_AVAIL_QTY] - SUM( QUANTITY ) OVER (partition by Item
	  --,bo_status 
					ORDER BY item, RowNo ,bo_status desc
                      ROWS BETWEEN UNBOUNDED PRECEDING 
                      AND CURRENT ROW)
		 as remaining_Total_QTY
		 ,[SourceType] 
		 ,SourceType_Daily
		,sourceLoadDate
		,PO_Splippage
      FROM cte

GO


