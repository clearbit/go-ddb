# ddb

A collection of DynamoDB helpers written in Golang to assit with reading and writing data. 

## Installation

```
go get github.com/clearbit/go-ddb
```

## Parallel Scan

To get maximum read throughput from a DynamodDB table we can leverage the [Parallel Scan](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#QueryAndScanParallelScan) functionality.

```go
// structure of DDB item
type message struct {
    name string `json:"name"`
}

// new scanner with table name and total segments
scanner := ddb.NewScanner(ddb.Config{
    TableName:     "ddb-table-name",
    TotalSegments: 150,   // calculate value: (table size GB / 2GB)
})

// start parallel scan w/ handler func
scanner.Start(ddb.HandlerFunc(func(items ddb.Items) {
    for _, item := range items {
        var msg message
        dynamodbattribute.UnmarshalMap(item, &msg)
        fmt.Println(msg.Name)
    }
}))

// wait for all scans to complete
scanner.Wait()
```