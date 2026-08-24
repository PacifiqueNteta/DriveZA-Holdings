CREATE TABLE [Marketing].[DimCustomer] (

	[CustomerKey] bigint IDENTITY NOT NULL, 
	[CustomerID] varchar(10) NOT NULL, 
	[FirstName] varchar(50) NULL, 
	[LastName] varchar(50) NULL, 
	[DOB] date NULL, 
	[Gender] varchar(20) NULL, 
	[IdNumber] varchar(50) NULL, 
	[PassportNumber] varchar(50) NULL, 
	[Nationality] varchar(50) NULL, 
	[City] varchar(50) NULL, 
	[Province] varchar(50) NULL, 
	[LoyaltyTier] varchar(20) NULL, 
	[LoyaltyPoints] int NULL, 
	[TotalRentals] int NULL, 
	[IsCorporateAccount] bit NULL, 
	[CompanyName] varchar(100) NULL, 
	[EffectiveDate] date NOT NULL, 
	[ExpiryDate] date NULL, 
	[IsCurrent] bit NOT NULL
);


GO
ALTER TABLE [Marketing].[DimCustomer] ADD CONSTRAINT PK_DimCustomer primary key NONCLUSTERED ([CustomerKey]);