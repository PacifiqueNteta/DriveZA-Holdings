CREATE TABLE [Marketing].[DimVehicle] (

	[VehicleKey] bigint IDENTITY NOT NULL, 
	[VehicleID] varchar(10) NOT NULL, 
	[Make] varchar(50) NULL, 
	[Model] varchar(50) NULL, 
	[Category] varchar(30) NULL, 
	[Status] varchar(20) NULL, 
	[Color] varchar(30) NULL, 
	[FuelType] varchar(30) NULL, 
	[Transmission] varchar(30) NULL, 
	[Year] float NULL, 
	[BranchID] varchar(10) NULL, 
	[CurrentOdometerKM] int NULL, 
	[DailyRateZAR] decimal(10,2) NULL, 
	[PurchaseDate] date NOT NULL, 
	[EffectiveDate] date NOT NULL, 
	[ExpiryDate] date NULL, 
	[IsCurrent] bit NOT NULL
);


GO
ALTER TABLE [Marketing].[DimVehicle] ADD CONSTRAINT PK_DimVehicle primary key NONCLUSTERED ([VehicleKey]);