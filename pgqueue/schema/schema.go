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
	DefaultPartitionThreshold    = 0.5     // create partition(s) when sequence reaches this fraction of the highest partition end
	DefaultPartitionAhead        = 1       // number of partition(s) to create when threshold is reached
	DefaultMaintenanceTickerName = "$maintenance$"
	DefaultCleanupTickerName     = "$cleanup$"
	DefaultTickerPeriod          = 5 * time.Second  // how often to look for matured tickers
	DefaultQueuePeriod           = 10 * time.Second // how often to poll queues for retries and missed notifications
	DefaultCleanupPeriod         = 15 * time.Minute // how often to delete expired tasks
	DefaultMaintenancePeriod     = 10 * time.Minute // create and drop partitions
)
