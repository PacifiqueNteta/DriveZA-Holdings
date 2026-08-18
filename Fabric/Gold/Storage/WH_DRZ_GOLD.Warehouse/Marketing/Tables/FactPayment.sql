CREATE TABLE [Marketing].[FactPayment] (

	[PaymentKey] bigint IDENTITY NOT NULL, 
	[PaymentID] varchar(10) NOT NULL, 
	[RentalKey] bigint NOT NULL, 
	[CustomerKey] bigint NOT NULL, 
	[PaymentDateKey] int NOT NULL, 
	[AmountZAR] decimal(10,2) NULL, 
	[PaymentMethod] varchar(30) NULL, 
	[Status] varchar(20) NULL
);


GO
ALTER TABLE [Marketing].[FactPayment] ADD CONSTRAINT PK_FactPayment primary key NONCLUSTERED ([PaymentKey]);