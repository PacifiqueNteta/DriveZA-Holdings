CREATE TABLE [Marketing].[DimCustomer] (

	[CustomerKey] bigint IDENTITY NOT NULL, 
	[CustomerID] varchar(10) NOT NULL, 
	[FirstName] varchar(50) NULL, 
	[LastName] varchar(50) NULL, 
	[City] varchar(50) NULL, 
	[Province] varchar(50) NULL, 
	[LoyaltyTier] varchar(20) NULL, 
	[IsCorporateAccount] bit NULL, 
	[EffectiveDate] date NOT NULL, 
	[ExpiryDate] date NULL, 
	[IsCurrent] bit NOT NULL
);


GO
ALTER TABLE [Marketing].[DimCustomer] ADD CONSTRAINT PK_DimCustomer primary key NONCLUSTERED ([CustomerKey]);