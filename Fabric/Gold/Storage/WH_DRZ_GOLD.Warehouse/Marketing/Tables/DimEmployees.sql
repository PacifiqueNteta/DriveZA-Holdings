CREATE TABLE [Marketing].[DimEmployees] (

	[EmployeeKey] bigint IDENTITY NOT NULL, 
	[EmployeeID] varchar(10) NOT NULL, 
	[FirstName] varchar(50) NULL, 
	[LastName] varchar(50) NULL, 
	[Role] varchar(30) NULL, 
	[BranchID] varchar(10) NULL, 
	[IsActive] bit NULL, 
	[EffectiveDate] date NOT NULL, 
	[ExpiryDate] date NULL, 
	[IsCurrent] bit NOT NULL
);


GO
ALTER TABLE [Marketing].[DimEmployees] ADD CONSTRAINT PK_DimEmployees primary key NONCLUSTERED ([EmployeeKey]);