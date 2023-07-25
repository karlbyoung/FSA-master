

CREATE view [FF].[V_FSA_Xfer_Assembly_Output] as 
select [NS_LINE_NUMBER] as [Line ID], ordernumber as [PQ]
,item as [ISBN]
,convert(varchar, [capping_DDA], 101) as DDA
,convert(varchar, [FSA Updated Original DDA], 101) as [Original DDA]
,'FALSE' as DNAAL
,'' as [Location]
,'Complete Qty' as [Commit Status]
,'FALSE' as [Ship Complete]

from fsa 
where SourceType != 'OpenSO'

GO


