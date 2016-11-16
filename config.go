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

	// TableName is name of table to scan
	TableName string

	// CheckpointTableName is the name of checkpont table
	CheckpointTableName string

	// CheckpointNamespace is the unique namespace for checkpoints. This must be unique so
	// checkpoints so differnt scripts can maintain their own checkpoints.
	CheckpointNamespace string

	// TotalSegments determines amount of concurrency to scan table with
	TotalSegments int

	// AwsRegion is the region the database is in. Defaults to us-west-1
	AwsRegion string

	// Checkpoint
	Checkpoint *Checkpoint

	// svc the dynamodb connection
	Svc *dynamodb.DynamoDB
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

	if c.Svc == nil {
		c.Svc = dynamodb.New(
			session.New(),
			aws.NewConfig().WithRegion(c.AwsRegion),
		)
	}

	if c.CheckpointTableName != "" && c.CheckpointNamespace != "" {
		c.Checkpoint = &Checkpoint{
			TableName: c.CheckpointTableName,
			Namespace: c.CheckpointNamespace,
			Svc:       c.Svc,
		}
	}
}
