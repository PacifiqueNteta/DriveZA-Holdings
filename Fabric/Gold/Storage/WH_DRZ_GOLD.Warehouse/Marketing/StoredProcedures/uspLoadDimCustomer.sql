-- DimCustomer
CREATE     PROCEDURE Marketing.uspLoadDimCustomer
    @pipeline_run_id VARCHAR(100),
    @pipeline_name VARCHAR(100)
AS
BEGIN

    DECLARE @run_start DATETIME2(6)=SYSUTCDATETIME();

    DECLARE @rows_read BIGINT=0;
    DECLARE @expired BIGINT=0;
    DECLARE @inserted BIGINT=0;
    DECLARE @rows_written BIGINT=0;

    BEGIN TRY

        SELECT @rows_read = COUNT(*)
        FROM LH_DRZ_SILVER.crm.customers;

        UPDATE target
        SET
            ExpiryDate = CAST(SYSUTCDATETIME() AS DATE),
            IsCurrent = 0
        FROM Marketing.DimCustomer target
        INNER JOIN LH_DRZ_SILVER.crm.customers source
            ON target.CustomerID = source.customer_id
        WHERE target.IsCurrent = 1
          AND (
                ISNULL(target.City,'') <> ISNULL(source.city,'')
             OR ISNULL(target.Province,'') <> ISNULL(source.province,'')
             OR ISNULL(target.LoyaltyTier,'') <> ISNULL(source.loyalty_tier,'')
             OR ISNULL(target.IsCorporateAccount,0)
                <> ISNULL(source.is_corporate_account,0)
          );

        SET @expired = @@ROWCOUNT;

        INSERT INTO Marketing.DimCustomer
        (
            CustomerID,
            FirstName,
            LastName,
            DOB,
            Gender,
            IdNumber,
            PassportNumber,
            Nationality,
            City,
            Province,
            LoyaltyTier,
            LoyaltyPoints,
            TotalRentals,            
            IsCorporateAccount,
            CompanyName,
            EffectiveDate,
            ExpiryDate,
            IsCurrent
        )
        SELECT
            source.customer_id,
            source.first_name,
            source.last_name,
            source.date_of_birth,
            source.gender,
            source.id_number,
            source.passport_number,
            source.nationality,
            source.city,
            source.province,
            source.loyalty_tier,
            source.loyalty_points,
            source.total_rentals,
            source.is_corporate_account,
            source.company_name,
            CAST(SYSUTCDATETIME() AS DATE),
            NULL,
            1
        FROM LH_DRZ_SILVER.crm.customers source
        LEFT JOIN Marketing.DimCustomer current_rec
            ON source.customer_id = current_rec.CustomerID
           AND current_rec.IsCurrent = 1
        WHERE current_rec.CustomerID IS NULL
        
        UNION ALL
        
        SELECT
            source.customer_id,
            source.first_name,
            source.last_name,
            source.date_of_birth,
            source.gender,
            source.id_number,
            source.passport_number,
            source.nationality,
            source.city,
            source.province,
            source.loyalty_tier,
            source.loyalty_points,
            source.total_rentals,
            source.is_corporate_account,
            source.company_name,
            CAST(SYSUTCDATETIME() AS DATE),
            NULL,
            1
        FROM LH_DRZ_SILVER.crm.customers source
        INNER JOIN Marketing.DimCustomer expired_rec
            ON source.customer_id = expired_rec.CustomerID
        WHERE expired_rec.IsCurrent = 0
          AND expired_rec.ExpiryDate = CAST(SYSUTCDATETIME() AS DATE);

        SET @inserted = @@ROWCOUNT;

        SET @rows_written = @expired + @inserted;

        EXEC metadata.usp_LogSuccess
            @pipeline_run_id,
            @pipeline_name,
            'DimCustomer',
            @rows_read,
            @rows_written,
            @inserted,
            @expired,
            @run_start;

    END TRY
    BEGIN CATCH

        DECLARE @ErrorMessage VARCHAR(4000);

        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC metadata.usp_LogFailure
            @pipeline_run_id,
            @pipeline_name,
            'DimCustomer',
            @run_start,
            @ErrorMessage;

        THROW;

    END CATCH

END;