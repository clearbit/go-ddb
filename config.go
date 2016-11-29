package ddb

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	defaultAwsRegion     = "us-west-1"
	defaultTotalSegments = 50
)

// Config is wrapper around the configuration variables
type Config struct {
	// Svc the dynamodb connection
	Svc *dynamodb.DynamoDB

	// AwsRegion is the region the database is in. Defaults to us-west-1
	AwsRegion string

	// TableName is name of table to scan
	TableName string

	// TotalSegments determines amount of concurrency to scan table with
	TotalSegments int

	// Checkpoint
	Checkpoint *Checkpoint

	// CheckpointTableName is the name of checkpont table
	CheckpointTableName string

	// CheckpointNamespace is the unique namespace for checkpoints. This must be unique so
	// checkpoints so differnt scripts can maintain their own checkpoints.
	CheckpointNamespace string
}

// defaults for configuration.
func (c *Config) setDefaults() {
	if c.AwsRegion == "" {
		c.AwsRegion = defaultAwsRegion
	}

	if c.TableName == "" {
		log.Fatal("TableName required as config var")
	}

	if c.TotalSegments == 0 {
		c.TotalSegments = defaultTotalSegments
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
