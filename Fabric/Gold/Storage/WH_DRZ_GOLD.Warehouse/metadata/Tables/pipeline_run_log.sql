CREATE TABLE [metadata].[pipeline_run_log] (

	[pipeline_run_id] varchar(100) NULL, 
	[pipeline_name] varchar(100) NULL, 
	[object_name] varchar(100) NULL, 
	[run_status] varchar(20) NULL, 
	[rows_read] bigint NULL, 
	[rows_written] bigint NULL, 
	[inserted_rows] bigint NULL, 
	[updated_rows] bigint NULL, 
	[error_message] varchar(4000) NULL, 
	[run_start] datetime2(6) NULL, 
	[run_end] datetime2(6) NULL, 
	[duration_seconds] bigint NULL
);