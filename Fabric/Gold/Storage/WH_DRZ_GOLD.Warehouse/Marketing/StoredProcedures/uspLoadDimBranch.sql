-- DimBranch
CREATE   PROCEDURE Marketing.uspLoadDimBranch

    @pipeline_run_id VARCHAR(100),
    @pipeline_name VARCHAR(100)

AS
BEGIN

    DECLARE @run_start DATETIME2(6);

    DECLARE @rows_read BIGINT = 0;
    DECLARE @updated BIGINT = 0;
    DECLARE @inserted BIGINT = 0;
    DECLARE @rows_written BIGINT = 0;

    SET @run_start = SYSUTCDATETIME();

    BEGIN TRY

        SELECT @rows_read = COUNT(*)
        FROM LH_DRZ_SILVER.admn.branches;

        UPDATE target
        SET
            BranchName = source.branch_name,
            Province = source.province,
            City = source.city,
            ManagerName = source.manager_name,
            FleetCapacity = source.fleet_capacity,
            IsActive = source.is_active
        FROM Marketing.DimBranch target
        INNER JOIN LH_DRZ_SILVER.admn.branches source
            ON target.BranchID = source.branch_id;

        SET @updated = @@ROWCOUNT;

        INSERT INTO Marketing.DimBranch
        (
            BranchID,
            BranchName,
            Province,
            City,
            ManagerName,
            FleetCapacity,
            IsActive
        )
        SELECT
            source.branch_id,
            source.branch_name,
            source.province,
            source.city,
            source.manager_name,
            source.fleet_capacity,
            source.is_active
        FROM LH_DRZ_SILVER.admn.branches source
        LEFT JOIN Marketing.DimBranch existing
            ON source.branch_id = existing.BranchID
        WHERE existing.BranchID IS NULL;

        SET @inserted = @@ROWCOUNT;

        SET @rows_written = @updated + @inserted;

        EXEC metadata.usp_LogSuccess
            @pipeline_run_id,
            @pipeline_name,
            'DimBranch',
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
            'DimBranch',
            @run_start,
            @ErrorMessage;

        THROW;

    END CATCH

END;