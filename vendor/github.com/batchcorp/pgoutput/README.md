# pgoutput

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx"
	"github.com/kyleconroy/pgoutput"
)

func main() {
	ctx := context.Background()
	config := pgx.ConnConfig{Database: "opsdash", User: "replicant"}
	conn, err := pgx.ReplicationConnect(config)
	if err != nil {
		log.Fatal(err)
	}

  // Create a slot if it doesn't already exist
	// if err := conn.CreateReplicationSlot("sub2", "pgoutput"); err != nil {
	// 	log.Fatalf("Failed to create replication slot: %v", err)
	// }

	set := pgoutput.NewRelationSet()

	dump := func(relation uint32, row []pgoutput.Tuple) error {
		values, err := set.Values(relation, row)
		if err != nil {
			return fmt.Errorf("error parsing values: %s", err)
		}
		for name, value := range values {
			val := value.Get()
			log.Printf("%s (%T): %#v", name, val, val)
		}
		return nil
	}

	handler := func(m pgoutput.Message) error {
		switch v := m.(type) {
		case pgoutput.Relation:
			log.Printf("RELATION")
			set.Add(v)
		case pgoutput.Insert:
			log.Printf("INSERT")
			return dump(v.RelationID, v.Row)
		case pgoutput.Update:
			log.Printf("UPDATE")
			return dump(v.RelationID, v.Row)
		case pgoutput.Delete:
			log.Printf("DELETE")
			return dump(v.RelationID, v.Row)
		}
		return nil
	}

	replication := pgoutput.LogicalReplication{
		Subscription:  "sub2",
		Publication:   "pub2",
		WaitTimeout:   time.Second * 10,
		StatusTimeout: time.Second * 10,
		Handler:       handler,
	}

	if err := replication.Start(ctx, conn); err != nil {
		log.Fatal(err)
	}
}
```
