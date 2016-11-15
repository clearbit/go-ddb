package ddb

import (
	"expvar"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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

	// DDB is the initialized DynamoDB connection
	DDB *dynamodb.DynamoDB
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

	if c.DDB == nil {
		c.DDB = dynamodb.New(
			session.New(),
			aws.NewConfig().WithRegion(c.AwsRegion),
		)
	}

	if c.CompletedSegments == nil {
		c.CompletedSegments = expvar.NewInt("completed_segments")
	}

	if c.TotalProcessed == nil {
		c.TotalProcessed = expvar.NewInt("total_processed")
	}
}
