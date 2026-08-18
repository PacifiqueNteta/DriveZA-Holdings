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

    IF EXISTS
    (
        SELECT 1
        FROM metadata.pipeline_control
        WHERE object_name = @object_name
    )
    BEGIN

        UPDATE metadata.pipeline_control
        SET
            last_run_status = 'Succeeded',
            last_run_start = @run_start,
            last_run_end = @run_end,
            rows_read = @rows_read,
            rows_written = @rows_written,
            inserted_rows = @inserted_rows,
            updated_rows = @updated_rows,
            updated_date = @run_end
        WHERE object_name = @object_name;

    END
    ELSE
    BEGIN

        INSERT INTO metadata.pipeline_control
        (
            object_name,
            last_run_status,
            last_run_start,
            last_run_end,
            rows_read,
            rows_written,
            inserted_rows,
            updated_rows,
            updated_date
        )
        VALUES
        (
            @object_name,
            'Succeeded',
            @run_start,
            @run_end,
            @rows_read,
            @rows_written,
            @inserted_rows,
            @updated_rows,
            @run_end
        );

    END

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