package ddb

import (
	"expvar"
	"log"
	"sync"
)

const (
	defaultAwsRegion     = "us-west-1"
	defaultTotalSegments = 100
)

// Config is wrapper around the configuration variables
type Config struct {
	// CompletedSegments tracks how many of segments have completed
	CompletedSegments *expvar.Int

	// TotalProcessed counts the number of items pullsed from DB
	TotalProcessed *expvar.Int

	// WaitGroup for tracking completed segments
	WaitGroup *sync.WaitGroup

	// TableName is name of DynamoDB table
	TableName string

	// TotalSegments determines amount of concurrency to scan table with
	TotalSegments int

	// AwsRegion is the region the database is in. Defaults to us-west-1
	AwsRegion string
}

// defaults for configuration.
func (c *Config) setDefaults() {
	if c.TableName == "" {
		log.Fatal("TableName required as config var")
	}

	if c.WaitGroup == nil {
		c.WaitGroup = &sync.WaitGroup{}
	}

	if c.AwsRegion == "" {
		c.AwsRegion = defaultAwsRegion
	}

	if c.CompletedSegments == nil {
		c.CompletedSegments = expvar.NewInt("completed_segments")
	}

	if c.TotalProcessed == nil {
		c.TotalProcessed = expvar.NewInt("total_processed")
	}
}
