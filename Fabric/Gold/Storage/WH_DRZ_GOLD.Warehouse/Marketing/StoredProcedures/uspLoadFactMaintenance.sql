CREATE   PROCEDURE Marketing.uspLoadFactMaintenance
    @pipeline_run_id VARCHAR(100),
    @pipeline_name VARCHAR(100)
AS
BEGIN

    DECLARE @run_start DATETIME2(6)=SYSUTCDATETIME();
    DECLARE @rows_read BIGINT=0;
    DECLARE @inserted BIGINT=0;

    BEGIN TRY

        SELECT @rows_read=COUNT(*)
        FROM LH_DRZ_SILVER.fleet.maintenance;

        INSERT INTO Marketing.FactMaintenance
        (
            MaintenanceID,
            VehicleKey,
            BranchKey,
            ScheduledDateKey,
            CostZAR,
            PartsCostZAR,
            LabourCostZAR,
            Status
        )
        SELECT
            m.maintenance_id,
            dv.VehicleKey,
            db.BranchKey,
            YEAR(m.scheduled_date)*10000+
            MONTH(m.scheduled_date)*100+
            DAY(m.scheduled_date),
            m.cost_zar,
            m.parts_cost_zar,
            m.labour_cost_zar,
            m.status
        FROM LH_DRZ_SILVER.fleet.maintenance m
        INNER JOIN Marketing.DimVehicle dv
            ON m.vehicle_id = dv.VehicleID
           AND dv.IsCurrent = 1
        INNER JOIN Marketing.DimBranch db
            ON m.branch_id = db.BranchID
        LEFT JOIN Marketing.FactMaintenance existing
            ON m.maintenance_id = existing.MaintenanceID
        WHERE existing.MaintenanceID IS NULL;

        SET @inserted = @@ROWCOUNT;

        EXEC metadata.usp_LogSuccess
            @pipeline_run_id,
            @pipeline_name,
            'FactMaintenance',
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
            'FactMaintenance',
            @run_start,
            @ErrorMessage;

        THROW;

    END CATCH

END;