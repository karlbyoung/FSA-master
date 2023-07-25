create view [FF].[V_FSA_ALL_Output] as 
select [NS_LINE_NUMBER] as [Line ID], ordernumber as [PQ]
,item as [ISBN]
,[capping_DDA] as [DDA]
,[FSA Updated Original DDA] as [Original DDA]
,'N' as DNAAL
,'' as [Location]
,'Complete Qty' as [Commit Status]
,'N' as [Ship Complete]

from fsa 

GO


