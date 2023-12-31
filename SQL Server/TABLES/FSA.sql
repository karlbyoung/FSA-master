CREATE TABLE [dbo].[FSA](
	[id] [int] NOT NULL,
	[Transaction_ID] [int] NULL,
	[ordernumber] [varchar](50) NULL,
	[UNIQUE_KEY] [int] NULL,
	[Priority] [int] NULL,
	[seq] [int] NULL,
	[DDA] [date] NULL,
	[Org_DDA] [date] NULL,
	[Transaction_Type] [varchar](50) NULL,
	[Rowno] [bigint] NULL,
	[Line_ID] [int] NULL,
	[NS_LINE_NUMBER] [int] NULL,
	[Item] [varchar](50) NULL,
	[ITEM_ID] [int] NULL,
	[COMPONENT_ITEM_ID] [int] NULL,
	[COMPONENT_ITEM] [varchar](50) NULL,
	[LOCATION] [varchar](50) NULL,
	[Item_RowNo] [bigint] NULL,
	[BO_STATUS] [varchar](10) NULL,
	[TOTAL_AVAIL_QTY] [int] NULL,
	[QUANTITY] [int] NULL,
	[sum_Rollup] [int] NULL,
	[remaining_Total_QTY] [int] NULL,
	[SourceType] [varchar](50) NULL,
	[SourceType_Daily] [varchar](50) NULL,
	[PO_Indicator] [int] NULL,
	[PO_Borrow] [varchar](50) NULL,
	[sum_Item_Rollup] [int] NULL,
	[PO_ID] [int] NULL,
	[PO_RowNo] [int] NULL,
	[PO_OrderNumber] [varchar](50) NULL,
	[PO_Qty_To_Be_Received] [int] NULL,
	[PO_QTY_Remaining] [int] NULL,
	[PO_Indicator_assign] [int] NULL,
	[RECEIVE_BY_DATE] [date] NULL,
	[PO_Insert_Date] [datetime] NULL,
	[PO_UpdateDT] [datetime] NULL,
	[Avail_Date] [date] NULL,
	[Was_AvailDT_PastDue] [char](1) NULL,
	[bob_Order_Number] [varchar](50) NULL,
	[BOB_Item] [varchar](50) NULL,
	[FREDD] [date] NULL,
	[Bucket_on_Receive_By_Date] [varchar](8) NULL,
	[BucketDT_on_Receive_By_Date] [date] NULL,
	[capping_DDA] [date] NULL,
	[IF_Generation_Date] [date] NULL,
	[Is_15_days] [char](1) NULL,
	[IF_bucket1] [date] NULL,
	[IF_bucket2] [date] NULL,
	[IF_bucket3] [date] NULL,
	[IF_bucket4] [date] NULL,
	[IF_bucket5] [date] NULL,
	[IF_bucket6] [date] NULL,
	[IF_bucket7] [date] NULL,
	[IF_bucket8] [date] NULL,
	[IF_bucket9] [date] NULL,
	[IF_bucket10] [date] NULL,
	[IF_bucket11] [date] NULL,
	[IF_bucket12] [date] NULL,
	[IF_bucket13] [date] NULL,
	[IF_bucket14] [date] NULL,
	[FSA Updated Original DDA] [date] NULL,
	[SourceLoadDate] [date] NULL,
	[insertDate] [date] NULL,
	[insertDateTime] [datetime] NULL)
GO
