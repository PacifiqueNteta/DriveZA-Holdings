-- Payment
CREATE   PROCEDURE Marketing.uspLoadFactPayment
    @pipeline_run_id VARCHAR(100),
    @pipeline_name VARCHAR(100)
AS
BEGIN

    DECLARE @run_start DATETIME2(6)=SYSUTCDATETIME();
    DECLARE @rows_read BIGINT=0;
    DECLARE @inserted BIGINT=0;

    BEGIN TRY

        SELECT @rows_read=COUNT(*)
        FROM LH_DRZ_SILVER.crm.payments;

        INSERT INTO Marketing.FactPayment
        (
            PaymentID,
            RentalKey,
            CustomerKey,
            PaymentDateKey,
            AmountZAR,
            PaymentMethod,
            Status
        )
        SELECT
            p.payment_id,
            fr.RentalKey,
            dc.CustomerKey,
            YEAR(p.payment_date)*10000+
            MONTH(p.payment_date)*100+
            DAY(p.payment_date),
            p.amount_zar,
            p.payment_method,
            p.status
        FROM LH_DRZ_SILVER.crm.payments p
        INNER JOIN Marketing.FactRental fr
            ON p.rental_id = fr.RentalID
        INNER JOIN Marketing.DimCustomer dc
            ON p.customer_id = dc.CustomerID
           AND dc.IsCurrent = 1
        LEFT JOIN Marketing.FactPayment existing
            ON p.payment_id = existing.PaymentID
        WHERE existing.PaymentID IS NULL;

        SET @inserted = @@ROWCOUNT;

        EXEC metadata.usp_LogSuccess
            @pipeline_run_id,
            @pipeline_name,
            'FactPayment',
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
            'FactPayment',
            @run_start,
            @ErrorMessage;

        THROW;

    END CATCH

END;