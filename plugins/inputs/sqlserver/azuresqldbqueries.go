package sqlserver

import (
	_ "github.com/denisenkom/go-mssqldb" // go-mssqldb initialization
)

const sqlAzureDB string = `SET DEADLOCK_PRIORITY -10;
IF OBJECT_ID('sys.dm_db_resource_stats') IS NOT NULL
BEGIN
	SELECT TOP(1)
		'sqlserver_azurestats' AS [measurement],
		REPLACE(@@SERVERNAME,'\',':') AS [sql_instance],
		avg_cpu_percent,
		avg_data_io_percent,
		avg_log_write_percent,
		avg_memory_usage_percent,
		xtp_storage_percent,
		max_worker_percent,
		max_session_percent,
		dtu_limit,
		avg_login_rate_percent,
		end_time
	FROM
		sys.dm_db_resource_stats WITH (NOLOCK)
	ORDER BY
		end_time DESC
	OPTION (RECOMPILE)
END
ELSE
BEGIN
	RAISERROR('This does not seem to be an AzureDB instance. Set "azureDB = false" in your telegraf configuration.',16,1)
END`
