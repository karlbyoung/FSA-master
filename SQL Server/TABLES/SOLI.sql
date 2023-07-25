Alter TABLE [dbo].[SOLI](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[FK_SPA_ID] [int] NULL,
	[OrderNo] [varchar](50) NULL,
	[UNIQUE_KEY] [int] NULL,
	[NS_LINE_NUMBER] [int] NULL,
	[ItemNo] [varchar](50) NULL,
	[Item_ID] [int] NULL,
	[ReceiptDate] [date] NULL,
	[FREDD]  AS ([dbo].[AddBusinessDays]([receiptdate],(12))),
	[SourceLoadDate] [date] NULL,
	[InsertDate] [date] NULL,
	[InsertDateTime] [datetime] NOT NULL,
 CONSTRAINT [PK_SOLI1] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[SOLI] ADD  CONSTRAINT [DF_SOLI_SourceLoadDate]  DEFAULT (getdate()) FOR [SourceLoadDate]
GO

ALTER TABLE [dbo].[SOLI] ADD  CONSTRAINT [DF_SOLI1_InsertDate_1]  DEFAULT (getdate()) FOR [InsertDate]
GO

ALTER TABLE [dbo].[SOLI] ADD  CONSTRAINT [DF_SOLI1_InsertDateTime]  DEFAULT (getdate()) FOR [InsertDateTime]
GO

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'ID of squencing_po_assign' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'SOLI', @level2type=N'COLUMN',@level2name=N'FK_SPA_ID'
GO

