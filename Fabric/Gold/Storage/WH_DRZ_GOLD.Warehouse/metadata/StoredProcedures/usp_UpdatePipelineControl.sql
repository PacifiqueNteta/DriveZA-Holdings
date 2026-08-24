CREATE   PROCEDURE metadata.usp_UpdatePipelineControl
AS
BEGIN

    ;WITH LatestRun AS
    (
        SELECT
            object_name,
            run_status,
            rows_read,
            rows_written,
            inserted_rows,
            updated_rows,
            run_start,
            run_end,
            ROW_NUMBER() OVER
            (
                PARTITION BY object_name
                ORDER BY run_end DESC
            ) AS rn
        FROM metadata.pipeline_run_log
    )

    MERGE metadata.pipeline_control tgt
    USING
    (
        SELECT
            object_name,
            run_status,
            rows_read,
            rows_written,
            inserted_rows,
            updated_rows,
            run_start,
            run_end
        FROM LatestRun
        WHERE rn = 1
    ) src

    ON tgt.object_name = src.object_name

    WHEN MATCHED THEN
        UPDATE SET
            last_run_status = src.run_status,
            last_run_start = src.run_start,
            last_run_end = src.run_end,
            rows_read = src.rows_read,
            rows_written = src.rows_written,
            inserted_rows = src.inserted_rows,
            updated_rows = src.updated_rows,
            updated_date = SYSUTCDATETIME()

    WHEN NOT MATCHED THEN
        INSERT
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
            src.object_name,
            src.run_status,
            src.run_start,
            src.run_end,
            src.rows_read,
            src.rows_written,
            src.inserted_rows,
            src.updated_rows,
            SYSUTCDATETIME()
        );

END;