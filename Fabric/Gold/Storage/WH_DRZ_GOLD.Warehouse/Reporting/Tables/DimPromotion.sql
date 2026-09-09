CREATE TABLE [Reporting].[DimPromotion] (

	[PromotionKey] bigint IDENTITY NOT NULL, 
	[PromotionID] varchar(10) NOT NULL, 
	[PromoCode] varchar(20) NULL, 
	[PromoName] varchar(100) NULL, 
	[PromoDescription] varchar(100) NULL, 
	[DiscountValue] decimal(5,2) NULL, 
	[DiscountType] varchar(20) NULL, 
	[ApplicableCategories] varchar(100) NULL, 
	[TimesUsed] int NULL, 
	[UsageLimit] int NULL, 
	[StartDate] date NULL, 
	[EndDate] date NULL, 
	[IsActive] bit NULL
);


GO
ALTER TABLE [Reporting].[DimPromotion] ADD CONSTRAINT PK_DimPromotion primary key NONCLUSTERED ([PromotionKey]);