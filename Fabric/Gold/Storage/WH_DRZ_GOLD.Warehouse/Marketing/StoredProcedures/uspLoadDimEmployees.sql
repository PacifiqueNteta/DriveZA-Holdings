-- DimEmployees
CREATE   PROCEDURE Marketing.uspLoadDimEmployees
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
        FROM LH_DRZ_SILVER.admn.employees;

        UPDATE target
        SET
            ExpiryDate = CAST(SYSUTCDATETIME() AS DATE),
            IsCurrent = 0
        FROM Marketing.DimEmployees target
        INNER JOIN LH_DRZ_SILVER.admn.employees source
            ON target.EmployeeID = source.employee_id
        WHERE target.IsCurrent = 1
          AND (
                ISNULL(target.Role,'') <> ISNULL(source.role,'')
             OR ISNULL(target.BranchID,'') <> ISNULL(source.branch_id,'')
             OR ISNULL(target.IsActive,0) <> ISNULL(source.is_active,0)
          );

        SET @expired = @@ROWCOUNT;

        INSERT INTO Marketing.DimEmployees
        (
            EmployeeID,
            FirstName,
            LastName,
            Role,
            BranchID,
            IsActive,
            EffectiveDate,
            ExpiryDate,
            IsCurrent
        )
        SELECT
            source.employee_id,
            source.first_name,
            source.last_name,
            source.role,
            source.branch_id,
            source.is_active,
            CAST(SYSUTCDATETIME() AS DATE),
            NULL,
            1
        FROM LH_DRZ_SILVER.admn.employees source
        LEFT JOIN Marketing.DimEmployees current_rec
            ON source.employee_id = current_rec.EmployeeID
           AND current_rec.IsCurrent = 1
        WHERE current_rec.EmployeeID IS NULL

        UNION ALL

        SELECT
            source.employee_id,
            source.first_name,
            source.last_name,
            source.role,
            source.branch_id,
            source.is_active,
            CAST(SYSUTCDATETIME() AS DATE),
            NULL,
            1
        FROM LH_DRZ_SILVER.admn.employees source
        INNER JOIN Marketing.DimEmployees expired_rec
            ON source.employee_id = expired_rec.EmployeeID
        WHERE expired_rec.IsCurrent = 0
          AND expired_rec.ExpiryDate = CAST(SYSUTCDATETIME() AS DATE);

        SET @inserted = @@ROWCOUNT;

        SET @rows_written = @expired + @inserted;

        EXEC metadata.usp_LogSuccess
            @pipeline_run_id,
            @pipeline_name,
            'DimEmployees',
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
            'DimEmployees',
            @run_start,
            @ErrorMessage;

        THROW;

    END CATCH

END;