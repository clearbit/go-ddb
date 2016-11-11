# go-ddb

Golang DynamoDB helpers

## Scan a segment

```go
// follows structure of DDB table
type message struct {
    name string `json:"name"`
}

// set up scanner with table name and total segments
scanner := ddb.NewScanner(ddb.Config{
    TableName:     "ddb-table-name",
    TotalSegments: 150,   // calculate value: (table size GB / 2GB)
})

// provider a handler loop which processes items
scanner.Start(ddb.HandlerFunc(func(items ddb.Items) {
    for _, item := range items {
        var msg message
        dynamodbattribute.UnmarshalMap(item, &msg)
        fmt.Println(msg.Name)
    }
}))

// wait for all segments to complete
scanner.Wait()
```