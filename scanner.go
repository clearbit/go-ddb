package ddb

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/jpillora/backoff"
)

// NewScanner creates a new scanner with ddb connection
func NewScanner(config Config) *Scanner {
	config.setDefaults()

	svc := dynamodb.New(
		session.New(),
		aws.NewConfig().WithRegion(config.AwsRegion),
	)

	return &Scanner{
		svc:    svc,
		Config: config,
	}
}

// Scanner is
type Scanner struct {
	svc *dynamodb.DynamoDB
	Config
}

// Wait pauses program until waitgroup is fulfilled
func (s *Scanner) Wait() {
	s.WaitGroup.Wait()
}

// Start uses the handler function to process items for each of the total shard
func (s *Scanner) Start(handler Handler) {
	for i := 0; i < s.TotalSegments; i++ {
		s.WaitGroup.Add(1)
		go s.handlerLoop(handler, i)
	}
}

func (s *Scanner) handlerLoop(handler Handler, segment int) {
	var lastEvaluatedKey map[string]*dynamodb.AttributeValue

	bk := &backoff.Backoff{
		Max:    5 * time.Minute,
		Jitter: true,
	}

	for {
		// scan params
		params := &dynamodb.ScanInput{
			TableName:     aws.String(s.TableName),
			Segment:       aws.Int64(int64(segment)),
			TotalSegments: aws.Int64(int64(s.TotalSegments)),
		}

		// last evaluated key
		if lastEvaluatedKey != nil {
			params.ExclusiveStartKey = lastEvaluatedKey
		}

		// scan, sleep if rate limited
		resp, err := s.svc.Scan(params)
		if err != nil {
			log.Printf("scan error: %v", err)
			time.Sleep(bk.Duration())
			continue
		}
		bk.Reset()

		// call the handler function with items
		handler.HandleItems(resp.Items)
		s.TotalProcessed.Add(int64(len(resp.Items)))

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
