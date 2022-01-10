package sqlserver

import (
	"database/sql"
	"sync"
	"time"

	_ "github.com/denisenkom/go-mssqldb" // go-mssqldb initialization
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

// SQLServer struct
type SQLServer struct {
	Servers      []string `toml:"servers"`
	QueryVersion int      `toml:"query_version"`
	AzureDB      bool     `toml:"azuredb"`
	DatabaseType string   `toml:"database_type"`
	ExcludeQuery []string `toml:"exclude_query"`
}

// Query struct
type Query struct {
	Script         string
	ResultByRow    bool
	OrderedColumns []string
}

// MapQuery type
type MapQuery map[string]Query

var queries MapQuery

// Initialized flag
var isInitialized = false

var defaultServer = "Server=.;app name=telegraf;log=1;"

const typeSQLServer = "SQLServer"

var sampleConfig = `
  ## Specify instances to monitor with a list of connection strings.
  ## All connection parameters are optional.
  ## By default, the host is localhost, listening on default port, TCP 1433.
  ##   for Windows, the user is the currently running AD user (SSO).
  ##   See https://github.com/denisenkom/go-mssqldb for detailed connection
  ##   parameters.
  # servers = [
  #  "Server=192.168.1.10;Port=1433;User Id=<user>;Password=<pw>;app name=telegraf;log=1;",
  # ]

  ## "database_type" enables a specific set of queries depending on the database type. If specified, it replaces azuredb = true/false and query_version = 2
  ## In the config file, the sql server plugin section should be repeated each with a set of servers for a specific database_type.
  ## Possible value for database_type are - "SQLServer"

  database_type = "SQLServer"

  ## Queries enabled by default for database_type = "SQLServer" are - 
  ## SQLServerPerformanceCounters, SQLServerWaitStatsCategorized, SQLServerDatabaseIO, SQLServerProperties, SQLServerMemoryClerks, 
  ## SQLServerSchedulers, SQLServerRequests, SQLServerVolumeSpace, SQLServerCpu, SQLServerAvailabilityReplicaStates, SQLServerDatabaseReplicaStates

  ## Optional parameter, setting this to 2 will use a new version
  ## of the collection queries that break compatibility with the original
  ## dashboards.
  # query_version = 2

  ## If you are using AzureDB, setting this to true will gather resource utilization metrics
  # azuredb = false

  ## If you would like to exclude some of the metrics queries, list them here
  ## Possible choices:
  ## - PerformanceCounters
  ## - WaitStatsCategorized
  ## - DatabaseIO
  ## - DatabaseProperties
  ## - CPUHistory
  ## - DatabaseSize
  ## - DatabaseStats
  ## - MemoryClerk
  ## - VolumeSpace
  ## - PerformanceMetrics
  # exclude_query = [ 'DatabaseIO' ]
`

// SampleConfig return the sample configuration
func (s *SQLServer) SampleConfig() string {
	return sampleConfig
}

// Description return plugin description
func (s *SQLServer) Description() string {
	return "Read metrics from Microsoft SQL Server"
}

type scanner interface {
	Scan(dest ...interface{}) error
}

func initQueries(s *SQLServer) {
	queries = make(MapQuery)

	// New config option database_type
	// Constant definitions for type "SQLServer" start with sqlServer
	if s.DatabaseType == typeSQLServer { //These are still V2 queries and have not been refactored yet.
		queries["SQLServerPerformanceCounters"] = Query{Script: sqlServerPerformanceCounters, ResultByRow: false}
		queries["SQLServerWaitStatsCategorized"] = Query{Script: sqlServerWaitStatsCategorized, ResultByRow: false}
		queries["SQLServerDatabaseIO"] = Query{Script: sqlServerDatabaseIO, ResultByRow: false}
		queries["SQLServerProperties"] = Query{Script: sqlServerProperties, ResultByRow: false}
		queries["SQLServerMemoryClerks"] = Query{Script: sqlServerMemoryClerks, ResultByRow: false}
		queries["SQLServerSchedulers"] = Query{Script: sqlServerSchedulers, ResultByRow: false}
		queries["SQLServerRequests"] = Query{Script: sqlServerRequests, ResultByRow: false}
		queries["SQLServerVolumeSpace"] = Query{Script: sqlServerVolumeSpace, ResultByRow: false}
		queries["SQLServerCpu"] = Query{Script: sqlServerRingBufferCPU, ResultByRow: false}
		queries["SQLServerAvailabilityReplicaStates"] = Query{Script: sqlServerAvailabilityReplicaStates, ResultByRow: false}
		queries["SQLServerDatabaseReplicaStates"] = Query{Script: sqlServerDatabaseReplicaStates, ResultByRow: false}
	} else {
		// If this is an AzureDB instance, grab some extra metrics
		if s.AzureDB {
			queries["AzureDB"] = Query{Script: sqlAzureDB, ResultByRow: false}
		}

		// Decide if we want to run version 1 or version 2 queries
		if s.QueryVersion == 2 {
			queries["PerformanceCounters"] = Query{Script: sqlPerformanceCountersV2, ResultByRow: true}
			queries["WaitStatsCategorized"] = Query{Script: sqlWaitStatsCategorizedV2, ResultByRow: false}
			queries["DatabaseIO"] = Query{Script: sqlDatabaseIOV2, ResultByRow: false}
			queries["ServerProperties"] = Query{Script: sqlServerPropertiesV2, ResultByRow: false}
			queries["MemoryClerk"] = Query{Script: sqlMemoryClerkV2, ResultByRow: false}
		} else {
			queries["PerformanceCounters"] = Query{Script: sqlPerformanceCounters, ResultByRow: true}
			queries["WaitStatsCategorized"] = Query{Script: sqlWaitStatsCategorized, ResultByRow: false}
			queries["CPUHistory"] = Query{Script: sqlCPUHistory, ResultByRow: false}
			queries["DatabaseIO"] = Query{Script: sqlDatabaseIO, ResultByRow: false}
			queries["DatabaseSize"] = Query{Script: sqlDatabaseSize, ResultByRow: false}
			queries["DatabaseStats"] = Query{Script: sqlDatabaseStats, ResultByRow: false}
			queries["DatabaseProperties"] = Query{Script: sqlDatabaseProperties, ResultByRow: false}
			queries["MemoryClerk"] = Query{Script: sqlMemoryClerk, ResultByRow: false}
			queries["VolumeSpace"] = Query{Script: sqlVolumeSpace, ResultByRow: false}
			queries["PerformanceMetrics"] = Query{Script: sqlPerformanceMetrics, ResultByRow: false}
		}
	}

	for _, query := range s.ExcludeQuery {
		delete(queries, query)
	}

	// Set a flag so we know that queries have already been initialized
	isInitialized = true
}

// Gather collect data from SQL Server
func (s *SQLServer) Gather(acc telegraf.Accumulator) error {
	if !isInitialized {
		initQueries(s)
	}

	if len(s.Servers) == 0 {
		s.Servers = append(s.Servers, defaultServer)
	}

	var wg sync.WaitGroup

	for _, serv := range s.Servers {
		for _, query := range queries {
			wg.Add(1)
			go func(serv string, query Query) {
				defer wg.Done()
				acc.AddError(s.gatherServer(serv, query, acc))
			}(serv, query)
		}
	}

	wg.Wait()
	return nil
}

func (s *SQLServer) gatherServer(server string, query Query, acc telegraf.Accumulator) error {
	// deferred opening
	conn, err := sql.Open("mssql", server)
	if err != nil {
		return err
	}
	defer conn.Close()

	// execute query
	rows, err := conn.Query(query.Script)
	if err != nil {
		return err
	}
	defer rows.Close()

	// grab the column information from the result
	query.OrderedColumns, err = rows.Columns()
	if err != nil {
		return err
	}

	for rows.Next() {
		err = s.accRow(query, acc, rows)
		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *SQLServer) accRow(query Query, acc telegraf.Accumulator, row scanner) error {
	var columnVars []interface{}
	var fields = make(map[string]interface{})

	// store the column name with its *interface{}
	columnMap := make(map[string]*interface{})
	for _, column := range query.OrderedColumns {
		columnMap[column] = new(interface{})
	}
	// populate the array of interface{} with the pointers in the right order
	for i := 0; i < len(columnMap); i++ {
		columnVars = append(columnVars, columnMap[query.OrderedColumns[i]])
	}
	// deconstruct array of variables and send to Scan
	err := row.Scan(columnVars...)
	if err != nil {
		return err
	}

	// measurement: identified by the header
	// tags: all other fields of type string
	tags := map[string]string{}
	var measurement string
	for header, val := range columnMap {
		if str, ok := (*val).(string); ok {
			if header == "measurement" {
				measurement = str
			} else {
				tags[header] = str
			}
		}
	}

	if query.ResultByRow {
		// add measurement to Accumulator
		acc.AddFields(measurement,
			map[string]interface{}{"value": *columnMap["value"]},
			tags, time.Now())
	} else {
		// values
		for header, val := range columnMap {
			if _, ok := (*val).(string); !ok {
				fields[header] = (*val)
			}
		}
		// add fields to Accumulator
		acc.AddFields(measurement, fields, tags, time.Now())
	}
	return nil
}

func init() {
	inputs.Add("sqlserver", func() telegraf.Input {
		return &SQLServer{}
	})
}
