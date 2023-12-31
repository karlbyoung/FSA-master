CREATE TABLE [dbo].[Demand_PO_LOAD](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Transaction_ID] [varchar](50) NULL,
	[ORDER_NUMBER] [varchar](50) NULL,
	[UNIQUE_KEY] [int] NULL,
	[DDA] [date] NULL,
	[Org_DDA] [date] NULL,
	[TRANSACTION_TYPE] [varchar](50) NULL,
	[TOTAL_AMT] [int] NULL,
	[LINE_ID] [varchar](10) NULL,
	[NS_LINE_NUMBER] [int] NULL,
	[ITEM] [varchar](50) NULL,
	[ITEM_ID] [int] NULL,
	[COMPONENT_ITEM_ID] [int] NULL,
	[COMPONENT_ITEM] [varchar](50) NULL,
	[QUANTITY] [int] NULL,
	[COMPONENT_QTY_ORDERED] [int] NULL,
	[LOCATION] [varchar](50) NULL,
	[PRIORITY_LEVEL] [int] NULL,
	[SOURCETYPE] [varchar](8) NULL,
	[TOTAL_AVAIL_QTY] [int] NULL,
	[BO_STATUS] [varchar](9) NULL,
	[SourceLoadDate] [date] NULL,
	[InsertDate] [date] NULL,
	[InserDateTime] [datetime] NULL) 