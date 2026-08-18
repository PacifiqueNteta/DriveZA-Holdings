CREATE TABLE [Marketing].[FactRental] (

	[RentalKey] bigint IDENTITY NOT NULL, 
	[RentalID] varchar(10) NOT NULL, 
	[CustomerKey] bigint NOT NULL, 
	[VehicleKey] bigint NOT NULL, 
	[PickupBranchKey] bigint NOT NULL, 
	[DropoffBranchKey] bigint NOT NULL, 
	[EmployeeKey] bigint NOT NULL, 
	[PromotionKey] bigint NULL, 
	[BookingDateKey] int NOT NULL, 
	[PickupDateKey] int NOT NULL, 
	[DropoffDateKey] int NULL, 
	[RentalDays] int NULL, 
	[DailyRateZAR] decimal(10,2) NULL, 
	[BaseRentalAmountZAR] decimal(10,2) NULL, 
	[InsuranceAmountZAR] decimal(10,2) NULL, 
	[ExtrasAmountZAR] decimal(10,2) NULL, 
	[DiscountAmountZAR] decimal(10,2) NULL, 
	[TotalAmountZAR] decimal(10,2) NULL, 
	[DepositAmountZAR] decimal(10,2) NULL, 
	[RentalStatus] varchar(20) NULL
);


GO
ALTER TABLE [Marketing].[FactRental] ADD CONSTRAINT PK_FactRental primary key NONCLUSTERED ([RentalKey]);
GO
ALTER TABLE [Marketing].[FactRental] ADD CONSTRAINT FK_FactRental_Customer FOREIGN KEY ([CustomerKey]) REFERENCES [Marketing].[DimCustomer]([CustomerKey]);
GO
ALTER TABLE [Marketing].[FactRental] ADD CONSTRAINT FK_FactRental_DropoffBranch FOREIGN KEY ([DropoffBranchKey]) REFERENCES [Marketing].[DimBranch]([BranchKey]);
GO
ALTER TABLE [Marketing].[FactRental] ADD CONSTRAINT FK_FactRental_Employee FOREIGN KEY ([EmployeeKey]) REFERENCES [Marketing].[DimEmployees]([EmployeeKey]);
GO
ALTER TABLE [Marketing].[FactRental] ADD CONSTRAINT FK_FactRental_PickupBranch FOREIGN KEY ([PickupBranchKey]) REFERENCES [Marketing].[DimBranch]([BranchKey]);
GO
ALTER TABLE [Marketing].[FactRental] ADD CONSTRAINT FK_FactRental_Promotion FOREIGN KEY ([PromotionKey]) REFERENCES [Marketing].[DimPromotion]([PromotionKey]);
GO
ALTER TABLE [Marketing].[FactRental] ADD CONSTRAINT FK_FactRental_Vehicle FOREIGN KEY ([VehicleKey]) REFERENCES [Marketing].[DimVehicle]([VehicleKey]);