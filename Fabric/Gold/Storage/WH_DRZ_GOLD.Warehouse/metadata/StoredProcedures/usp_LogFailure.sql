CREATE   PROCEDURE metadata.usp_LogFailure

    @pipeline_run_id VARCHAR(100),
    @pipeline_name VARCHAR(100),

    @object_name VARCHAR(100),

    @run_start DATETIME2(6),

    @error_message VARCHAR(4000)

AS
BEGIN

    DECLARE @run_end DATETIME2(6);

    SET @run_end = SYSUTCDATETIME();

    UPDATE metadata.pipeline_control
    SET
        last_run_status='Failed',
        last_run_end=@run_end,
        updated_date=@run_end
    WHERE object_name=@object_name;

    INSERT INTO metadata.pipeline_run_log
    (
        pipeline_run_id,
        pipeline_name,
        object_name,
        run_status,
        error_message,
        run_start,
        run_end,
        duration_seconds
    )
    VALUES
    (
        @pipeline_run_id,
        @pipeline_name,
        @object_name,
        'Failed',
        @error_message,
        @run_start,
        @run_end,
        DATEDIFF(SECOND,@run_start,@run_end)
); 
END;