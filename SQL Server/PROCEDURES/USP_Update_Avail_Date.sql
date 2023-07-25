


-- =============================================
-- Author:		JW
-- Create date: 7/1/2022
-- Description:Availability_Date is the "receivebyDate" to use for BOB
-- Merge 2 dates into avail_date column .. if line is avaiable, then use the DDA data otherwise use receive_by_date 

--- 7/29/2022 - when no po assigned to the line then recieve_by_date is null, then we check DDA date.  If dda is >= getdate, dda + 90 days - 12 biz days.  

/*  8/2/2022
PO indicator 1
DDA Past due = Todays date

DDA NOT Past Due = DDA - 12 Business days
PO indicator -1
DDA = receive by date on PO it is assigned to
If NO PO is assigned and the indicator is -1 then take DAD + 90 days - 12 business days

If DDA is past due OR <= todays date + 12 business days then Avail_date = todays date
If DDA is not past due AND > todays date + 12 business days then do DDA - 12 business days

*/

-- =============================================
ALTER PROCEDURE [dbo].[USP_Update_Avail_Date]

AS
BEGIN


	--Update Sequencing_PO_Assign
	--			set Avail_Date = 
	--			--Select DDA, RECEIVE_BY_DATE
	--			 case when RECEIVE_BY_DATE is null 
	--				Then case when DDA < cast(getdate() as date) then cast(getdate() as date) 
	--						else ([dbo].[AddBusinessDays](dateadd(dd, 90, DDA) ,(-12))) end 
	--	          Else RECEIVE_BY_DATE
	--			  End 
	--			  where Avail_Date is null 
		Update Sequencing_PO_Assign
				set Avail_Date = 

							case when PO_Indicator > -1 Then 
										case when (DDA < cast(getdate() as date) or DDA <= [dbo].[AddBusinessDays](getdate() ,(+12))) 
									then cast(getdate() as date) 
									Else  [dbo].[AddBusinessDays](DDA ,(-12)) 
										End  
			else 
				 case when RECEIVE_BY_DATE is null 
								--Then 	 ([dbo].[AddBusinessDays](dateadd(dd, 90, DDA) ,(-12))) 
								Then 	 ([dbo].[AddBusinessDays](dateadd(dd, 90, getdate()) ,(-12))) 
							  Else RECEIVE_BY_DATE
							  End 
				End


;

/*

ONLY for ones that  avail Date is past due  and have a -1 indicator and NO PO assigned to it 
, update Avail Date with today's date + 90 days - 12 business days
*/ 



update  [dbo].[Sequencing_PO_Assign]
	set Avail_Date = case when Avail_Date < cast(getdate() as date)  and PO_UpdateDT is null 
						then ([dbo].[AddBusinessDays](dateadd(dd, 90, getdate()) ,(-12))) end
	,Was_AvailDT_PastDue = 'Y'
		where  Avail_Date < cast(getdate() as date)  and PO_UpdateDT is null 

	;


End 

 
 




GO


