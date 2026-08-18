CREATE TABLE [Marketing].[FactMaintenance] (

	[MaintenanceKey] bigint IDENTITY NOT NULL, 
	[MaintenanceID] varchar(10) NOT NULL, 
	[VehicleKey] bigint NOT NULL, 
	[BranchKey] bigint NOT NULL, 
	[ScheduledDateKey] int NOT NULL, 
	[CostZAR] decimal(10,2) NULL, 
	[PartsCostZAR] decimal(10,2) NULL, 
	[LabourCostZAR] decimal(10,2) NULL, 
	[Status] varchar(20) NULL
);


GO
ALTER TABLE [Marketing].[FactMaintenance] ADD CONSTRAINT PK_FactMaintenance primary key NONCLUSTERED ([MaintenanceKey]);