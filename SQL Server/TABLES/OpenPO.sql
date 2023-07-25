
CREATE TABLE [dbo].[OpenPO](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[PO_ROWNO] [int] NULL,
	[PO_ITEM_ID_C] [int] NULL,
	[PO_Item_TYPE_C] [varchar](50) NULL,
	[PO_TYPE] [varchar](50) NULL,
	[ITEM_ID] [int] NULL,
	[ITEM] [varchar](50) NULL,
	[ITEM_DISPLAY_NAME] [varchar](100) NULL,
	[ASSEMBLY_ITEM_ID] [int] NULL,
	[ASSEMBLY_ELSE_ITEM_ID] [int] NULL,
	[ASSEMBLY_ITEM] [varchar](50) NULL,
	[ASSEMBLY_ITEM_DISPLAY_NAME] [varchar](100) NULL,
	[ORDER_NUMBER] [varchar](50) NULL,
	[PURCHASE_ORDER_TRANSACTION_ID] [int] NULL,
	[STATUS] [varchar](50) NULL,
	[LOCATION] [varchar](50) NULL,
	[RECEIVE_BY_DATE]  AS (case when [NS_RECEIVE_BY_DATE]<CONVERT([date],getdate()) then [dbo].[AddBusinessDays](getdate(),(5)) else [NS_RECEIVE_BY_DATE] end),
	[NS_RECEIVE_BY_DATE] [date] NULL,
	[UNIQUE_KEY] [int] NULL,
	[QUANITITY_TO_BE_RECEIVED] [int] NULL,
	[SourceLoadDate] [date] NULL,
	[InsertDate] [date] NULL,
	[InsertDateTime] [datetime] NULL)
