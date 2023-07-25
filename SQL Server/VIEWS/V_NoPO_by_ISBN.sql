
create view [FF].[V_NoPO_by_ISBN] as 

with cte as (Select 	 a.item_id, a.item,  max(b.po_rowno) as max_OpenPO_Row, max(a.po_id) as max_PO_Line_Row, sum([QUANTITY]) as SUMP_QTY
	from dbo.Sequencing_PO_Assign a left outer join [dbo].[OpenPO]  b
	on a.item_id = b.PO_ITEM_ID_C 
	where    a.po_indicator  = -1 	
	
--	and a.PO_UpdateDT is  null
group by  a.Item_id , a.item 

) select *
from cte
where max_OpenPO_Row is null
GO


