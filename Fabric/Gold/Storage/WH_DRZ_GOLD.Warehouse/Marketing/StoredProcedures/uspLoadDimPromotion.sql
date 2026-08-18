-- DimPromotion
CREATE   PROCEDURE Marketing.uspLoadDimPromotion
    @pipeline_run_id VARCHAR(100),
    @pipeline_name VARCHAR(100)
AS
BEGIN

    DECLARE @run_start DATETIME2(6) = SYSUTCDATETIME();

    DECLARE @rows_read BIGINT = 0;
    DECLARE @updated BIGINT = 0;
    DECLARE @inserted BIGINT = 0;
    DECLARE @rows_written BIGINT = 0;

    BEGIN TRY

        SELECT @rows_read = COUNT(*)
        FROM LH_DRZ_SILVER.crm.promotions;

        UPDATE target
        SET
            PromoCode = source.promo_code,
            PromoName = source.promo_name,
            DiscountValue = source.discount_value,
            DiscountType = source.discount_type,
            StartDate = source.start_date,
            EndDate = source.end_date,
            IsActive = source.is_active
        FROM Marketing.DimPromotion target
        INNER JOIN LH_DRZ_SILVER.crm.promotions source
            ON target.PromotionID = source.promotion_id;

        SET @updated = @@ROWCOUNT;

        INSERT INTO Marketing.DimPromotion
        (
            PromotionID,
            PromoCode,
            PromoName,
            DiscountValue,
            DiscountType,
            StartDate,
            EndDate,
            IsActive
        )
        SELECT
            source.promotion_id,
            source.promo_code,
            source.promo_name,
            source.discount_value,
            source.discount_type,
            source.start_date,
            source.end_date,
            source.is_active
        FROM LH_DRZ_SILVER.crm.promotions source
        LEFT JOIN Marketing.DimPromotion existing
            ON source.promotion_id = existing.PromotionID
        WHERE existing.PromotionID IS NULL;

        SET @inserted = @@ROWCOUNT;
        SET @rows_written = @updated + @inserted;

        EXEC metadata.usp_LogSuccess
            @pipeline_run_id,
            @pipeline_name,
            'DimPromotion',
            @rows_read,
            @rows_written,
            @inserted,
            @updated,
            @run_start;

    END TRY
    BEGIN CATCH

        DECLARE @ErrorMessage VARCHAR(4000);
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC metadata.usp_LogFailure
            @pipeline_run_id,
            @pipeline_name,
            'DimPromotion',
            @run_start,
            @ErrorMessage;

        THROW;

    END CATCH

END;