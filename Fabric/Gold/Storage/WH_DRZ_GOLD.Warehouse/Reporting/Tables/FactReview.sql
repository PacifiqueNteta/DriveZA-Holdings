CREATE TABLE [Reporting].[FactReview] (
    
    ReviewKey BIGINT IDENTITY NOT NULL,
    ReviewID VARCHAR(10) NOT NULL,

    RentalKey BIGINT NOT NULL,
    CustomerKey BIGINT NOT NULL,
    BranchKey BIGINT NOT NULL,

    ReviewDateKey INT NOT NULL,
    ResponseDateKey INT NULL,

    OverallRating DECIMAL(3,1) NULL,
    VehicleRating DECIMAL(3,1) NULL,
    ServiceRating DECIMAL(3,1) NULL,
    ValueRating DECIMAL(3,1) NULL,
    CleanlinessRating DECIMAL(3,1) NULL,

    ReviewText VARCHAR(8000) NULL,
    ReviewChannel VARCHAR(50) NULL,
    ResponseText VARCHAR(8000) NULL,

    VerifiedRental BIT NULL
);

ALTER TABLE [Reporting].[FactReview] ADD CONSTRAINT PK_FactReview PRIMARY KEY NONCLUSTERED (ReviewID);