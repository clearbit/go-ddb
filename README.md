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
    SegmentOffset: 0, // optional param for controlling offset
    SegmentCount:  150, // optional param for controlling how many routines get created
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

Leverage a checkpoint table to store the last evaluated key of a scan:

```go
scanner := ddb.NewScanner(ddb.Config{
    TableName:           "ddb-table-name",
    CheckpointTableName: "checkpoint-production",  // name of table to store last evaluated keys
    CheckpointNamespace: "my-sample-app",          // namespace to avoid collisions with other scripts
    TotalSegments:       150,
})
```

## License

go-ddb is copyright © 2016 Clearbit. It is free software, and may
be redistributed under the terms specified in the [`LICENSE`] file.

[`LICENSE`]: /MIT-LICENSE
