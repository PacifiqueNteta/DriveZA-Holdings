CREATE TABLE [metadata].[pipeline_control] (

	[object_name] varchar(100) NULL, 
	[last_run_status] varchar(20) NULL, 
	[last_run_start] datetime2(6) NULL, 
	[last_run_end] datetime2(6) NULL, 
	[rows_read] bigint NULL, 
	[rows_written] bigint NULL, 
	[inserted_rows] bigint NULL, 
	[updated_rows] bigint NULL, 
	[updated_date] datetime2(6) NULL
);