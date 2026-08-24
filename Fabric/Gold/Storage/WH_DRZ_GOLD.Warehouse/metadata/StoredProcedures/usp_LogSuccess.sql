-- Success Log Stored Procedure
CREATE   PROCEDURE metadata.usp_LogSuccess

    @pipeline_run_id VARCHAR(100),
    @pipeline_name VARCHAR(100),
    @object_name VARCHAR(100),

    @rows_read BIGINT,
    @rows_written BIGINT,

    @inserted_rows BIGINT,
    @updated_rows BIGINT,

    @run_start DATETIME2(6)

AS
BEGIN

    DECLARE @run_end DATETIME2(6);

    SET @run_end = SYSUTCDATETIME();

    INSERT INTO metadata.pipeline_run_log
    (
        pipeline_run_id,
        pipeline_name,
        object_name,
        run_status,
        rows_read,
        rows_written,
        inserted_rows,
        updated_rows,
        run_start,
        run_end,
        duration_seconds
    )
    VALUES
    (
        @pipeline_run_id,
        @pipeline_name,
        @object_name,
        'Succeeded',
        @rows_read,
        @rows_written,
        @inserted_rows,
        @updated_rows,
        @run_start,
        @run_end,
        DATEDIFF(SECOND,@run_start,@run_end)
    );

END;