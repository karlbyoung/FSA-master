-- =============================================
-- Author:		JW	
-- Create date: July 2022
-- Description:	This is the source to feed BoB logic 
-- =============================================
ALTER  PROCEDURE [dbo].[USP_INSERT_SOLI]

@InsertCnt INT OUTPUT

AS
BEGIN
  
set Nocount on; 

TRUNCATE TABLE dbo.soli	
;
Insert into soli  (OrderNo,UNIQUE_KEY, NS_LINE_NUMBER, itemno, ReceiptDate, [FK_SPA_ID],sourceloaddate, Item_ID) 

  select ordernumber
  ,UNIQUE_KEY
  ,NS_LINE_NUMBER
	, Item
	, avail_date 
	,ID
	,sourceloaddate
	,Item_ID
  from [dbo].[Sequencing_PO_Assign]
  order by ordernumber, item, UNIQUE_KEY

  ;
  select  @InsertCnt = count (*) 
  from dbo.soli
  ;
END
GO


