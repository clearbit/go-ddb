package ddb

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/jpillora/backoff"
)

// NewScanner creates a new scanner with default config vars
func NewScanner(config Config) *Scanner {
	config.setDefaults()

	return &Scanner{
		Config: config,
	}
}

// Scanner wraps the scan functionality across multiple segments
type Scanner struct {
	Config
}

// Start uses the handler function to process items for each of the total shard
func (s *Scanner) Start(handler Handler) {
	for i := 0; i < s.TotalSegments; i++ {
		s.WaitGroup.Add(1)
		go s.handlerLoop(handler, i)
	}
}

// Wait pauses program until waitgroup is fulfilled
func (s *Scanner) Wait() {
	s.WaitGroup.Wait()
}

// handlerLoop started from Start takes a handler func and segment number. It scans
// the segment until it reaches the end of the segment
func (s *Scanner) handlerLoop(handler Handler, segment int) {
	var lastEvaluatedKey map[string]*dynamodb.AttributeValue

	bk := &backoff.Backoff{
		Max:    5 * time.Minute,
		Jitter: true,
	}

	for {
		params := &dynamodb.ScanInput{
			TableName:      aws.String(s.TableName),
			Segment:        aws.Int64(int64(segment)),
			TotalSegments:  aws.Int64(int64(s.TotalSegments)),
			ConsistentRead: aws.Bool(true),
		}

		// last evaluated key
		if lastEvaluatedKey != nil {
			params.ExclusiveStartKey = lastEvaluatedKey
		}

		// scan, sleep if throughput
		resp, err := s.DDB.Scan(params)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() == "ProvisionedThroughputExceededException" {
					time.Sleep(bk.Duration())
				}
			}
			continue
		}
		bk.Reset()

		// call the handler function with items
		handler.HandleItems(resp.Items)

		// exit if last evaluated key empty
		if resp.LastEvaluatedKey == nil {
			s.CompletedSegments.Add(1)
			s.WaitGroup.Done()
			return
		}

		// set last evaluated key
		lastEvaluatedKey = resp.LastEvaluatedKey
	}
}
