CREATE TABLE [Marketing].[DimDate] (

	[DateKey] int NOT NULL, 
	[FullDate] date NOT NULL, 
	[DayOfWeek] varchar(20) NULL, 
	[DayOfMonth] smallint NULL, 
	[MonthNumber] smallint NULL, 
	[MonthName] varchar(20) NULL, 
	[Quarter] smallint NULL, 
	[Year] smallint NULL, 
	[FiscalMonth] smallint NULL, 
	[FiscalQuarter] smallint NULL, 
	[FiscalYear] smallint NULL, 
	[IsWeekend] smallint NULL, 
	[IsSAPublicHoliday] smallint NULL
);


GO
ALTER TABLE [Marketing].[DimDate] ADD CONSTRAINT PK_DimDate primary key NONCLUSTERED ([DateKey]);