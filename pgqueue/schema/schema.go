package schema

import (
	_ "embed"
	"time"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed objects.sql
var Objects string

//go:embed queries.sql
var Queries string

const (
	DefaultSchema                = "pgqueue"
	DefaultNotifyChannel         = "queue_notify"
	QueueListLimit               = 100
	TickerListLimit              = 100
	DefaultPartitionSize         = 100_000 // tasks per partition
	DefaultMaintenanceTickerName = "$maintenance$"
	DefaultCleanupTickerName     = "$cleanup$"
	DefaultTickerPeriod          = 5 * time.Second  // how often to look for matured tickers
	DefaultQueuePeriod           = 10 * time.Second // how often to poll queues for retries and missed notifications
	DefaultCleanupPeriod         = 15 * time.Minute // how often to delete expired tasks
	DefaultMaintenancePeriod     = time.Hour        // create and drop partitions
)
