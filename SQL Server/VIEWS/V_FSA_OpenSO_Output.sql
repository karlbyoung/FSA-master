
CREATE view [FF].[V_FSA_OpenSO_Output] as 
select transaction_ID as [Internal ID]
, ordernumber as [PQ]
,line_ID as [Line ID]
, [NS_LINE_NUMBER] as [Internal Line ID]
,item as [ISBN]

, convert(varchar, [capping_DDA], 101) as DDA

, convert(varchar, [FSA Updated Original DDA], 101) as [Original DDA]
,'FALSE' as DNAAL
,'' as [Location]
,'Complete Qty' as [Commit Status]
,'FALSE' as [Ship Complete]

from fsa 
where SourceType = 'OpenSO'
--order by ordernumber, [NS_LINE_NUMBER]
group BY transaction_ID 
, ordernumber 
,line_ID 
, [NS_LINE_NUMBER] 
,item 
,[capping_DDA]
,[FSA Updated Original DDA]
GO


