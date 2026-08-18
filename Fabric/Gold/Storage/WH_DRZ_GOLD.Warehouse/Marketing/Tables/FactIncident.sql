CREATE TABLE [Marketing].[FactIncident] (

	[IncidentKey] bigint IDENTITY NOT NULL, 
	[IncidentID] varchar(10) NOT NULL, 
	[RentalKey] bigint NOT NULL, 
	[VehicleKey] bigint NOT NULL, 
	[CustomerKey] bigint NOT NULL, 
	[IncidentDateKey] int NOT NULL, 
	[EstimatedDamageZAR] decimal(10,2) NULL, 
	[ActualRepairCostZAR] decimal(10,2) NULL, 
	[InsurancePayoutZAR] decimal(10,2) NULL, 
	[ExcessChargedZAR] decimal(10,2) NULL, 
	[Status] varchar(20) NULL
);


GO
ALTER TABLE [Marketing].[FactIncident] ADD CONSTRAINT PK_FactIncident primary key NONCLUSTERED ([IncidentKey]);