/*  SET VARIABLES ------------------------------------------------------------------------------------------------------------------- */
DECLARE @MAX_PO_ROWNO INT;
DECLARE @MAX_PO_LINE_NO INT;
DECLARE @ITEMID VARCHAR(50);
DECLARE @QTY_RECEIVED INT;
DECLARE @QTY_ORDERED INT;
DECLARE @PO_ORDERNUMBER VARCHAR(50) ;
DECLARE @RECEIVE_BY_DATE DATE;
DECLARE @STARTCNT INT = 1; 
DECLARE @ROWID INT; 
DECLARE @ROWMAXID INT; 
DECLARE @PO_ROWNO INT = 1; 
DECLARE @PO_ID INT = 1; 
DECLARE @PO_ASSIGN_INDICATOR INT; 
DECLARE @PO_QTY_REMAINING INT;
DECLARE @ROWCOUNT INT ;
DECLARE @I INT  = 1 ;
DECLARE @TBL_MAX_MEASURE TABLE(MAXROWID INT, ITEM_ID INT, ITEM VARCHAR(50), MAXPOROWNO INT, MAXPOLINENO INT)


/*  ASSIGN ROW COUNT ---------------------------------------------------------------------------------------------------------------- */
SELECT @ROWCOUNT = COUNT(DISTINCT A.ITEM_ID) 
FROM DBO.SEQUENCING_PO_ASSIGN A 
LEFT OUTER JOIN [DBO].[OPENPO] B
  ON A.ITEM_ID = B.PO_ITEM_ID_C 
WHERE    A.PO_INDICATOR  = -1 
  AND PO_UPDATEDT IS NULL; 


/*  CREATE MAX MEASURE TABLE -------------------------------------------------------------------------------------------------------- */
INSERT INTO @TBL_MAX_MEASURE (MAXROWID, ITEM_ID, ITEM, MAXPOROWNO, MAXPOLINENO)
SELECT ROW_NUMBER () OVER( ORDER BY A.ITEM_ID) AS ID,A.*
FROM (SELECT A.ITEM_ID
			,A.ITEM
			,MAX(B.PO_ROWNO) AS MAXPOROWNO
			,MAX(A.PO_ID)    AS MAXPOLINENO
	  FROM DBO.SEQUENCING_PO_ASSIGN A 
	  LEFT OUTER JOIN [DBO].[OPENPO] B
	  	ON A.ITEM_ID = B.PO_ITEM_ID_C 
	  WHERE A.PO_INDICATOR  = -1 	
	    AND A.PO_UPDATEDT IS NULL
	  GROUP BY A.ITEM_ID,A.ITEM) A
WHERE MAXPOROWNO IS NOT NULL;

 
/* ################################################################################################################################# */
-- Start Loop 1
WHILE (@I <= @RowCount)
	BEGIN; -- 1
		select @RowMaxID = MaxRowID, @itemID = item_id, @MAX_PO_rowno = MaxPoRowNo, @MAX_PO_Line_No = MaxPOLineNo 
		from @Tbl_Max_measure
		where MaxRowID  = @i
		and MaxPoRowNo is not null --- added on 7/26
		
		If (@itemID = '') Break;
		
		Set @startCnt = 1 -- reset to start next Item 
		Set @PO_rowno = 1 

		/* ######################################################################################################################### */		
				-- Start Loop 2 
				WHILE @startCnt <=  @MAX_PO_rowno 
					BEGIN; -- 2
                        /* ################################################################# */
						with cte_Calc_Sum_Rollup as ( 
							select  ID, 
							SUM(QUANTITY ) OVER (partition by item_rowno, po_rowno, SIGN(remaining_Total_QTY) 
												ORDER BY RowNo 
												ROWS BETWEEN UNBOUNDED PRECEDING 
													AND CURRENT ROW) as sum_Item_Rollup
							from Sequencing_PO_Assign
							where ITEM_ID = @itemID
							and   po_indicator  = -1 	
							and PO_UpdateDT is null -- added 7/26/2022
						)
						
						update a
						set a.sum_Item_Rollup = b.sum_Item_Rollup
						from Sequencing_PO_Assign a 
                        join cte_Calc_Sum_Rollup b 
						on a.id = b.id 
						where a.po_rowno is null;


                        /* ################################################################# */
						select @ROWID 				= ID
							,@PO_assign_indicator = PO_Indicator_assign
							,@PO_ID				= ROW_NUMBER() OVER (PARTITION BY Item_ID ORDER BY rowno, id) 
							,@QTY_ordered 		= sum_Item_Rollup
						from dbo.Sequencing_PO_Assign
						where ITEM_ID = @itemID
						and   po_indicator = -1 
						order by rowno desc, id desc;

						SELECT @PO_ordernumber 	= order_number
							,@RECEIVE_BY_DATE = RECEIVE_BY_DATE
							,@QTY_RECEIVED 	= QUANITITY_TO_BE_RECEIVED
						from [dbo].[OpenPO]
						where PO_ITEM_ID_C = @itemID 
						AND PO_rowno = @startCnt;
						
						/* ################################################################# */				
								-- Start Loop 3 
								WHILE ISNULL(@QTY_RECEIVED, 0) - ISNULL(@QTY_ordered, 0) >= 0
									BEGIN; -- 3

										UPDATE a
										set [PO_RowNo] 				 	= @PO_rowno
											,[PO_Qty_To_Be_Received] 	= @QTY_RECEIVED
											,PO_QTY_Remaining 			= ISNULL(@QTY_RECEIVED, 0) - ISNULL(@QTY_ordered, 0)
											,PO_OrderNumber 			= @PO_ordernumber
											,RECEIVE_BY_DATE 			= @RECEIVE_BY_DATE
											,[PO_UpdateDT] 				= getdate()
										FROM dbo.Sequencing_PO_Assign A 
										LEFT OUTER JOIN [dbo].[OpenPO] B
										  ON A.ITEM_ID   = B.PO_ITEM_ID_C
										where A.Item_Id  = @itemID
										AND B.PO_ROWNO 	 = @startCnt
										and PO_ID 	     = @PO_ID
										and a.[PO_RowNo] is Null 

										set @PO_ID = @PO_ID + 1;
						
										select @PO_QTY_REMAINING 	= ISNULL(@QTY_RECEIVED, 0) - ISNULL(@QTY_ordered,0)
											  ,@ROWID				= ID
											  ,@PO_assign_indicator = PO_Indicator_assign
											  , @QTY_ordered 		= sum_Item_Rollup
										from dbo.Sequencing_PO_Assign
										where ITEM_ID = @itemID
										  and  PO_ID  = @PO_ID 
					
										IF(@PO_ID > @MAX_PO_Line_No) Break;
								
										IF @PO_QTY_REMAINING = 0 Break; 

									END; -- End Loop 3 			    
						/* ######################################################################################################### */


						IF ( @startCnt >= @PO_rowno) Break; 

						SET @startCnt = @startCnt + 1;
						set @PO_rowno = @PO_rowno + 1;		  
				
						IF (@PO_ID >  @MAX_PO_Line_No)  Break;  

					END; -- End Loop 2
		/* ######################################################################################################################### */

	SET @I 		= @I + 1
	SET @itemID = ''

END; -- End Loop 1
/* ################################################################################################################################# */ 