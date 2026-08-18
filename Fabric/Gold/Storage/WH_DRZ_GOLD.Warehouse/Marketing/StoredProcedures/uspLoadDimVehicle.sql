CREATE   PROCEDURE Marketing.uspLoadDimVehicle
    @pipeline_run_id VARCHAR(100),
    @pipeline_name VARCHAR(100)
AS
BEGIN

    DECLARE @run_start DATETIME2(6) = SYSUTCDATETIME();

    DECLARE @rows_read BIGINT = 0;
    DECLARE @expired BIGINT = 0;
    DECLARE @inserted BIGINT = 0;
    DECLARE @rows_written BIGINT = 0;

    BEGIN TRY

        SELECT @rows_read = COUNT(*)
        FROM LH_DRZ_SILVER.fleet.vehicles;

        UPDATE target
        SET
            ExpiryDate = CAST(SYSUTCDATETIME() AS DATE),
            IsCurrent = 0
        FROM Marketing.DimVehicle target
        INNER JOIN LH_DRZ_SILVER.fleet.vehicles source
            ON target.VehicleID = source.vehicle_id
        WHERE target.IsCurrent = 1
          AND (
                ISNULL(target.Status,'') <> ISNULL(source.status,'')
             OR ISNULL(target.BranchID,'') <> ISNULL(source.branch_id,'')
             OR ISNULL(target.CurrentOdometerKM,0) <> ISNULL(source.current_odometer_km,0)
             OR ISNULL(target.DailyRateZAR,0) <> ISNULL(source.daily_rate_zar,0)
          );

        SET @expired = @@ROWCOUNT;

        INSERT INTO Marketing.DimVehicle
        (
            VehicleID,
            Make,
            Model,
            Category,
            Status,
            BranchID,
            CurrentOdometerKM,
            DailyRateZAR,
            EffectiveDate,
            ExpiryDate,
            IsCurrent
        )
        SELECT
            source.vehicle_id,
            source.make,
            source.model,
            source.category,
            source.status,
            source.branch_id,
            source.current_odometer_km,
            source.daily_rate_zar,
            CAST(SYSUTCDATETIME() AS DATE),
            NULL,
            1
        FROM LH_DRZ_SILVER.fleet.vehicles source
        LEFT JOIN Marketing.DimVehicle current_rec
            ON source.vehicle_id = current_rec.VehicleID
           AND current_rec.IsCurrent = 1
        WHERE current_rec.VehicleID IS NULL

        UNION ALL

        SELECT
            source.vehicle_id,
            source.make,
            source.model,
            source.category,
            source.status,
            source.branch_id,
            source.current_odometer_km,
            source.daily_rate_zar,
            CAST(SYSUTCDATETIME() AS DATE),
            NULL,
            1
        FROM LH_DRZ_SILVER.fleet.vehicles source
        INNER JOIN Marketing.DimVehicle expired_rec
            ON source.vehicle_id = expired_rec.VehicleID
        WHERE expired_rec.IsCurrent = 0
          AND expired_rec.ExpiryDate = CAST(SYSUTCDATETIME() AS DATE);

        SET @inserted = @@ROWCOUNT;

        SET @rows_written = @expired + @inserted;

        EXEC metadata.usp_LogSuccess
            @pipeline_run_id,
            @pipeline_name,
            'DimVehicle',
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
            'DimVehicle',
            @run_start,
            @ErrorMessage;

        THROW;

    END CATCH

END;