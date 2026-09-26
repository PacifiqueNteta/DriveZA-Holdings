CREATE PROCEDURE Reporting.uspLoadFactReview
    @pipeline_run_id VARCHAR(100),
    @pipeline_name VARCHAR(100)
AS
BEGIN

    DECLARE @run_start DATETIME2(6)=SYSUTCDATETIME();
    DECLARE @rows_read BIGINT=0;
    DECLARE @inserted BIGINT=0;

    BEGIN TRY

        SELECT @rows_read=COUNT(*)
        FROM LH_DRZ_SILVER.crm.reviews;

        INSERT INTO Reporting.FactReview
        (
            ReviewID,
            RentalKey,
            CustomerKey,
            BranchKey,
            ReviewDateKey,
            ResponseDateKey,
            OverallRating,
            VehicleRating,
            ServiceRating,
            ValueRating,
            CleanlinessRating,
            ReviewText,
            ReviewChannel,
            ResponseText,
            VerifiedRental
        )
        SELECT
            r.review_id,
            fr.RentalKey,
            dc.CustomerKey,
            db.BranchKey,
            YEAR(r.review_date)*10000+
            MONTH(r.review_date)*100+
            DAY(r.review_date),
            CASE
                WHEN r.response_date IS NULL
                THEN NULL
                ELSE
                    YEAR(r.response_date)*10000+
                    MONTH(r.response_date)*100+
                    DAY(r.response_date)
            END,
            r.overall_rating,
            r.vehicle_rating,
            r.service_rating,
            r.value_rating,
            r.cleanliness_rating,
            r.review_text,
            r.review_channel,
            r.response_text,
            r.verified_rental
        FROM LH_DRZ_SILVER.crm.reviews r
        INNER JOIN Reporting.FactRental fr
            ON r.rental_id = fr.RentalID
        INNER JOIN Reporting.DimCustomer dc
            ON r.customer_id = dc.CustomerID
           AND dc.IsCurrent = 1
        INNER JOIN Reporting.DimBranch db
            ON r.branch_id = db.BranchID
        LEFT JOIN Reporting.FactReview existing
            ON r.review_id = existing.ReviewID
        WHERE existing.ReviewID IS NULL;

        SET @inserted = @@ROWCOUNT;

        EXEC metadata.usp_LogSuccess
            @pipeline_run_id,
            @pipeline_name,
            'FactReview',
            @rows_read,
            @inserted,
            @inserted,
            0,
            @run_start;

    END TRY
    BEGIN CATCH

        DECLARE @ErrorMessage VARCHAR(4000);
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC metadata.usp_LogFailure
            @pipeline_run_id,
            @pipeline_name,
            'FactReview',
            @run_start,
            @ErrorMessage;

        THROW;

    END CATCH

END