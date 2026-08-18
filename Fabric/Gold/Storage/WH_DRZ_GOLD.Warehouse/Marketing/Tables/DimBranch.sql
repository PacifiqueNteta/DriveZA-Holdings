CREATE TABLE [Marketing].[DimBranch] (

	[BranchKey] bigint IDENTITY NOT NULL, 
	[BranchID] varchar(10) NOT NULL, 
	[BranchName] varchar(100) NULL, 
	[Province] varchar(50) NULL, 
	[City] varchar(50) NULL, 
	[ManagerName] varchar(100) NULL, 
	[FleetCapacity] int NULL, 
	[IsActive] bit NULL
);


GO
ALTER TABLE [Marketing].[DimBranch] ADD CONSTRAINT PK_DimBranch primary key NONCLUSTERED ([BranchKey]);