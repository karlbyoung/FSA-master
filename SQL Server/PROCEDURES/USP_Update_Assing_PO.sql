
ALTER PROCEDURE [dbo].[USP_Update_Assing_PO]

AS
BEGIN


DECLARE @MAX_PO_rowno INT; -- max of PO per Item_ID that can use QTY calucation 
DECLARE @MAX_PO_Line_No INT; -- MAX # NUMBER OF BY iTEM_ID that need to calucate against PO
DECLARE @itemID VARCHAR(50);
DECLARE @QTY_RECEIVED int;
DECLARE @QTY_ordered int;
DECLARE @PO_ordernumber varchar(50) ;
DECLARE @RECEIVE_BY_DATE DATE;
DECLARE @startCnt int = 1; 
DECLARE @ROWID int; 
DECLARE @RowMaxID int; 
DECLARE @PO_rowno int = 1; 
DECLARE @PO_ID int = 1;   --- the row_nummber() to assign the sequence of each item_ID that need to get PO assigned
DECLARE @PO_assign_indicator int; 
DECLARE @PO_QTY_REMAINING int;
DECLARE @RowCount int ;
DECLARE @i int  = 1 ;
DECLARE @Tbl_Max_measure TABLE(MaxRowID int, Item_ID int, item varchar(50), MaxPoRowNo int, MaxPOLineNo int   )


select @RowCount = count ( distinct a.item_id) from dbo.Sequencing_PO_Assign a left outer join [dbo].[OpenPO]  b
	on a.item_id = b.PO_ITEM_ID_C 
	where    a.po_indicator  = -1 
	and PO_UpdateDT is  null  -- add 7/22.22
	; 



	Insert Into  @Tbl_Max_measure (MaxRowID , Item_ID ,item, MaxPoRowNo , MaxPOLineNo)
	 select row_number () over( order by a.item_id) as ID,a.*
	from (select 	a.item_id, a.item,  max(b.po_rowno) as MaxPoRowNo, max(a.po_id) as MaxPOLineNo
				from dbo.Sequencing_PO_Assign a left outer join [dbo].[OpenPO]  b
				on a.item_id = b.PO_ITEM_ID_C 
				where    a.po_indicator  = -1 	
				and a.PO_UpdateDT is  null
	--	and a.ITEM_ID in (20583,23003) 
				group by a.item_id,a.item) a
	where MaxPoRowNo is not null
--	select 	row_number () over( order by a.item_id),a.item_id, a.item,  max(b.po_rowno), max(a.po_id) 
--	from dbo.Sequencing_PO_Assign a left outer join [dbo].[OpenPO]  b
--	on a.item_id = b.PO_ITEM_ID_C 
--	where    a.po_indicator  = -1 	
--	and a.PO_UpdateDT is  null
----and a.ITEM_ID = '9740' 
--	group by a.item_id,a.item

select * from @Tbl_Max_measure 
 
  PRINT  ' *****  ' + CAST(@i AS VARCHAR)

  print '***1 while begin'
WHILE (@I <= @RowCount)


	BEGIN
		select @RowMaxID = MaxRowID, @itemID= item_id, @MAX_PO_rowno = MaxPoRowNo, @MAX_PO_Line_No =  MaxPOLineNo 
		from @Tbl_Max_measure
		where MaxRowID  = @i
		and MaxPoRowNo is not null --- added on 7/26
		
		If (@itemID = '') Break;
		Print ' 1 break @itemID is null '+ + CAST(@itemID AS VARCHAR)
		
		Set @startCnt = 1 -- reset to start next Item 
		Set @PO_rowno = 1 
		set @PO_rowno  = 1

		PRINT  ' 1 While -  @i  : itemNo: @startCnt:@PO_rowno ' + CAST(@i AS VARCHAR) +' : '+ CAST(@itemID AS VARCHAR)+' : '+ CAST(@startCnt AS VARCHAR)+' : '+ CAST(@PO_rowno AS VARCHAR)
		
		print '***2 while begin @startCnt:@MAX_PO_rowno:  ' + CAST(@startCnt AS VARCHAR)+' : '
		+ CAST(@MAX_PO_rowno AS VARCHAR)
		  -- 2 while 
		WHILE @startCnt <=  @MAX_PO_rowno 
  
			BEGIN
				
				; with cte_Calc_Sum_Rollup as ( 
					select  ID , 
					SUM(QUANTITY ) OVER (partition by item_rowno, po_rowno, SIGN( remaining_Total_QTY) 
									ORDER BY RowNo 
								  ROWS BETWEEN UNBOUNDED PRECEDING 
								  AND CURRENT ROW)  as sum_Item_Rollup
				from Sequencing_PO_Assign
					-- where ITEM_ID =  '20220' -- @itemID 
					where ITEM_ID = @itemID
							and   po_indicator  = -1 	
							and PO_UpdateDT is null -- added 7/26/2022
							)
				update a
				set a.sum_Item_Rollup = b.sum_Item_Rollup
				from Sequencing_PO_Assign a join cte_Calc_Sum_Rollup b 
				on a.id = b.id 
				where a.po_rowno is null ;

				select @ROWID= ID, @PO_assign_indicator = PO_Indicator_assign  ,@PO_ID = ROW_NUMBER() OVER(PARTITION BY Item_ID  ORDER BY rowno, id  ) 
				, @QTY_ordered = sum_Item_Rollup
						from dbo.Sequencing_PO_Assign
						--where ITEM_ID = '20220' -- @itemID
						where ITEM_ID = @itemID
						and   po_indicator  = -1 
						order by rowno desc  , id desc;

				SELECT @PO_ordernumber = order_number,@RECEIVE_BY_DATE = RECEIVE_BY_DATE,@QTY_RECEIVED= QUANITITY_TO_BE_RECEIVED
				from [dbo].[OpenPO]
				where PO_ITEM_ID_C =   @itemID AND PO_rowno = @startCnt;
				

				PRINT  ' 2 while - startcnt Item no  :  ' + CAST(@startCnt AS VARCHAR)+' : '+ CAST(@itemID AS VARCHAR)

				-- 3 while 
				print '***3 while begin : ordered :  @itemID ' +CAST(@QTY_RECEIVED AS VARCHAR) +'  |@startCnt ' + ':'+CAST(@QTY_ordered AS VARCHAR) + ':'+CAST(@itemID AS VARCHAR)

				WHILE   ISNULL(@QTY_RECEIVED, 0) - ISNULL(@QTY_ordered,0) >= 0
				     
					Begin 
					Print ' enough Qty'
		
						 UPDATE a
						set  [PO_RowNo]= @PO_rowno
						,[PO_Qty_To_Be_Received] = @QTY_RECEIVED
						, PO_QTY_Remaining = ISNULL(@QTY_RECEIVED, 0) - ISNULL(@QTY_ordered,0)
						--,[PO_Indicator_assign] = Sign(ISNULL(@QTY_RECEIVED, 0) - ISNULL(@QTY_ordered,0))
						,PO_OrderNumber = @PO_ordernumber
						,RECEIVE_BY_DATE = @RECEIVE_BY_DATE
						,[PO_UpdateDT] = getdate()
						FROM dbo.Sequencing_PO_Assign A LEFT OUTER JOIN [dbo].[OpenPO] B
						ON A.ITEM_ID = B.PO_ITEM_ID_C
						where A.Item_Id = @itemID
						AND B.PO_ROWNO = @startCnt
						---and Sign(ISNULL(@QTY_RECEIVED, 0) - ISNULL(@QTY_ordered,0)) = 0
						--and @PO_assign_indicator = 1
						and PO_ID = @PO_ID
						and a.[PO_RowNo] is Null 
	
					PRINT  ' 3 while  after update :PO_ID: startCnt: Item no  :  ' + CAST(@PO_ID AS VARCHAR)+' : '+ CAST(@startCnt AS VARCHAR)+' : '+ CAST(@itemID AS VARCHAR)
	
		
		
				--set  @PO_QTY_REMAINING =  ISNULL(@QTY_RECEIVED, 0) - ISNULL(@QTY_ordered,0)
				set @PO_ID = @PO_ID+1;
		
				select @PO_QTY_REMAINING = ISNULL(@QTY_RECEIVED, 0) - ISNULL(@QTY_ordered,0)
				,@ROWID= ID,  @PO_assign_indicator  = PO_Indicator_assign
				, @QTY_ordered = sum_Item_Rollup
						from dbo.Sequencing_PO_Assign
						--where ITEM_ID = '20220' -- @itemID
						where ITEM_ID = @itemID
						and  PO_ID = @PO_ID 
	
			
				

			
				IF(@PO_ID > @MAX_PO_Line_No)  Break;
				print ' 3 while end itemID -  po_ID >  max_po_line '+' : '+ CAST(@itemID AS VARCHAR) + ' --  '+ CAST(@PO_ID AS VARCHAR)+' : '+ CAST(@MAX_PO_Line_No AS VARCHAR)

				
				IF @PO_QTY_REMAINING = 0 Break; 

				print ' 3 while QTY Remaning: '+ CAST(@PO_QTY_REMAINING AS VARCHAR)
			--IF (@PO_ID >  @MAX_PO_Line_No) Break 

			END   -- end 3 while 
				--IF (@PO_ID> @MAX_PO_Line_No) Break 
			    

			IF ( @startCnt >= @PO_rowno) Break; 
		  print  ' Break on 2 while  @startCnt > @PO_rowno  : ' +CAST( @startCnt AS VARCHAR) + ': ' +CAST( @PO_rowno AS VARCHAR) 

			SET @startCnt = @startCnt + 1;
			set @PO_rowno  = @PO_rowno+1;
			 PRINT '  ELSE in while 2:  QTY_remining: STARTCNT: po_rowno: PO_ID :  '+  CAST(@PO_QTY_REMAINING AS VARCHAR) +CAST( @startCnt AS VARCHAR)+  ' :' +CAST( @PO_rowno AS VARCHAR) + ':' +CAST( @PO_ID AS VARCHAR) 
		  
		 
		  IF (@PO_ID >  @MAX_PO_Line_No)  Break;  
		   print  ' Break on 2 while  po_ID >MAX_PO_Line_No  : ' +CAST( @PO_ID AS VARCHAR) + '  po max line: ' +CAST( @MAX_PO_Line_No AS VARCHAR) 

		 
		END 


	SET @I = @I + 1
	SET @itemID = ''
	PRINT ' @I    '+  CAST(@I AS VARCHAR)

	END
 


  
End ;

 


GO


