CREATE TABLE [metadata].[load_control] (

	[control_id] int NULL, 
	[object_name] varchar(100) NULL, 
	[object_type] varchar(30) NULL, 
	[business_key] varchar(100) NULL, 
	[load_order] int NULL, 
	[stored_procedure] varchar(200) NULL, 
	[is_active] bit NULL, 
	[created_date] datetime2(6) NULL
);