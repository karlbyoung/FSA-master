

CREATE TABLE [dbo].[Sequencing_PO_Assign](
	[id] [int] NOT NULL,
	[ordernumber] [varchar](50) NULL,
	[UNIQUE_KEY] [int] NULL,
	[Priority] [int] NULL,
	[seq] [int] NULL,
	[DDA] [date] NULL,
	[Org_DDA] [date] NULL,
	[Transaction_Type] [varchar](50) NULL,
	[Rowno] [bigint] NULL,
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
	[PO_Borrow] [varchar](12) NOT NULL,
	[sum_Item_Rollup] [int] NULL,
	[PO_ID] [int] NULL,
	[PO_RowNo] [int] NULL,
	[PO_OrderNumber] [varchar](50) NULL,
	[PO_Qty_To_Be_Received] [int] NULL,
	[PO_QTY_Remaining] [int] NULL,
	[PO_Indicator_assign]  AS (sign(isnull([PO_QTY_Remaining],(0)))),
	[RECEIVE_BY_DATE] [nchar](10) NULL,
	[PO_Insert_Date] [datetime] NULL,
	[PO_UpdateDT] [datetime] NULL,
	[Avail_Date] [date] NULL,
	[Was_AvailDT_PastDue] [char](1) NULL,
	[PO_Slippage] [char](1) NULL,
	[SourceLoadDate] [date] NULL,
	[insertDate] [date] NULL,
	[insertDateTime] [datetime] NULL)