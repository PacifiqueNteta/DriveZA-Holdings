-- Rental
CREATE   PROCEDURE Marketing.uspLoadFactRental
    @pipeline_run_id VARCHAR(100),
    @pipeline_name VARCHAR(100)
AS
BEGIN

    DECLARE @run_start DATETIME2(6) = SYSUTCDATETIME();

    DECLARE @rows_read BIGINT = 0;
    DECLARE @inserted BIGINT = 0;

    BEGIN TRY

        SELECT @rows_read = COUNT(*)
        FROM LH_DRZ_SILVER.crm.rentals;

        INSERT INTO Marketing.FactRental
        (
            RentalID,
            CustomerKey,
            VehicleKey,
            PickupBranchKey,
            DropoffBranchKey,
            EmployeeKey,
            PromotionKey,
            BookingDateKey,
            PickupDateKey,
            DropoffDateKey,
            RentalDays,
            DailyRateZAR,
            BaseRentalAmountZAR,
            InsuranceAmountZAR,
            ExtrasAmountZAR,
            DiscountAmountZAR,
            TotalAmountZAR,
            DepositAmountZAR,
            RentalStatus
        )
        SELECT
            r.rental_id,

            dc.CustomerKey,
            dv.VehicleKey,

            pb.BranchKey,
            db.BranchKey,

            de.EmployeeKey,

            dp.PromotionKey,

            YEAR(r.booking_date) * 10000 +
            MONTH(r.booking_date) * 100 +
            DAY(r.booking_date),

            YEAR(r.pickup_date) * 10000 +
            MONTH(r.pickup_date) * 100 +
            DAY(r.pickup_date),

            CASE
                WHEN r.actual_dropoff_date IS NULL
                THEN NULL
                ELSE
                    YEAR(r.actual_dropoff_date) * 10000 +
                    MONTH(r.actual_dropoff_date) * 100 +
                    DAY(r.actual_dropoff_date)
            END,

            r.rental_days,
            r.daily_rate_zar,
            r.base_rental_amount_zar,
            r.insurance_amount_zar,
            r.extras_amount_zar,
            r.discount_amount_zar,
            r.total_amount_zar,
            r.deposit_amount_zar,
            r.rental_status

        FROM LH_DRZ_SILVER.crm.rentals r

        INNER JOIN Marketing.DimCustomer dc
            ON r.customer_id = dc.CustomerID
           AND dc.IsCurrent = 1

        INNER JOIN Marketing.DimVehicle dv
            ON r.vehicle_id = dv.VehicleID
           AND dv.IsCurrent = 1

        INNER JOIN Marketing.DimEmployees de
            ON r.staff_id = de.EmployeeID
           AND de.IsCurrent = 1

        INNER JOIN Marketing.DimBranch pb
            ON r.pickup_branch_id = pb.BranchID

        INNER JOIN Marketing.DimBranch db
            ON r.dropoff_branch_id = db.BranchID

        LEFT JOIN Marketing.DimPromotion dp
            ON r.promo_code = dp.PromoCode

        LEFT JOIN Marketing.FactRental existing
            ON r.rental_id = existing.RentalID

        WHERE existing.RentalID IS NULL;

        SET @inserted = @@ROWCOUNT;

        EXEC metadata.usp_LogSuccess
            @pipeline_run_id,
            @pipeline_name,
            'FactRental',
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
            'FactRental',
            @run_start,
            @ErrorMessage;

        THROW;

    END CATCH

END;