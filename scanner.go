package ddb

import (
	"expvar"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/jpillora/backoff"
)

// NewScanner creates a new scanner with ddb connection
func NewScanner(config Config) *Scanner {
	config.setDefaults()

	return &Scanner{
		waitGroup:         &sync.WaitGroup{},
		Config:            config,
		CompletedSegments: expvar.NewInt("scanner.CompletedSegments"),
	}
}

// Scanner is
type Scanner struct {
	waitGroup *sync.WaitGroup
	Config
	CompletedSegments *expvar.Int
}

// Start uses the handler function to process items for each of the total shard
func (s *Scanner) Start(handler Handler) {
	for i := 0; i < s.TotalSegments; i++ {
		s.waitGroup.Add(1)
		go s.handlerLoop(handler, i)
	}
}

// Wait pauses program until waitgroup is fulfilled
func (s *Scanner) Wait() {
	s.waitGroup.Wait()
}

func (s *Scanner) handlerLoop(handler Handler, segment int) {
	defer s.waitGroup.Done()

	var lastEvaluatedKey map[string]*dynamodb.AttributeValue
	if s.Checkpoint != nil {
		lastEvaluatedKey = s.Checkpoint.Get(segment)
	}

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
		resp, err := s.Svc.Scan(params)
		if err != nil {
			fmt.Println(err)
			time.Sleep(bk.Duration())
			continue
		}
		bk.Reset()

		// call the handler function with items
		handler.HandleItems(resp.Items)

		// exit if last evaluated key empty
		if resp.LastEvaluatedKey == nil {
			s.CompletedSegments.Add(1)
			break
		}

		// set last evaluated key
		lastEvaluatedKey = resp.LastEvaluatedKey
		if s.Checkpoint != nil {
			s.Checkpoint.Set(segment, lastEvaluatedKey)
		}
	}
}
