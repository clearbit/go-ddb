package ddb

import (
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Checkpoint wraps the interactions with dynamo for setting/getting checkpoints
type Checkpoint struct {
	Svc       *dynamodb.DynamoDB
	Namespace string
	TableName string
}

// row reprsents a record in dynamodb table
type row struct {
	Namespace        string           `json:"namespace"`
	Segment          int              `json:"segment"`
	LastEvaluatedKey LastEvaluatedKey `json:"last_evaluated_key"`
}

// LastEvaluatedKey is the attribute value of the last evaluated key in a scan
type LastEvaluatedKey map[string]*dynamodb.AttributeValue

// Get returns the exclusive start key for current segment
func (c *Checkpoint) Get(segment int) LastEvaluatedKey {
	resp, err := c.Svc.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(c.TableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"namespace": &dynamodb.AttributeValue{
				S: aws.String(c.Namespace),
			},
			"segment": &dynamodb.AttributeValue{
				N: aws.String(strconv.Itoa(segment)),
			},
		},
	})
	if err != nil {
		if retriableError(err) {
			c.Get(segment)
		} else {
			fmt.Printf("Checkpoint > Get > GetItem: %v", err)
			return nil
		}
	}
	item := row{}
	dynamodbattribute.UnmarshalMap(resp.Item, &item)
	return item.LastEvaluatedKey
}

// Set the lastEvaluatedKey as most recent checkpoint
func (c *Checkpoint) Set(segment int, lastEvaluatedKey LastEvaluatedKey) {
	item, err := dynamodbattribute.MarshalMap(row{
		Namespace:        c.Namespace,
		Segment:          segment,
		LastEvaluatedKey: lastEvaluatedKey,
	})
	if err != nil {
		fmt.Printf("Checkpoint > Set > MarshalMap: %v", err)
		return
	}
	_, err = c.Svc.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(c.TableName),
		Item:      item,
	})
	if err != nil {
		if retriableError(err) {
			c.Set(segment, lastEvaluatedKey)
		} else {
			fmt.Printf("Checkpoint > Set > PutItem: %v", err)
		}
	}
	return
}

func retriableError(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		if awsErr.Code() == "ProvisionedThroughputExceededException" {
			return true
		}
	}
	return false
}
