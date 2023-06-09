package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/dathan/go-safely-change-data/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Define and parse command line flags
	dbName := flag.String("db", "yourdb", "Name of the DB")
	tableName := flag.String("table", "", "Name of the table to delete rows from")
	whereClause := flag.String("where", "1=1", "WHERE clause to filter rows to delete")
	concurrency := flag.Int("concurrency", 5, "How many concurrent changes at once")
	dryRun := flag.Bool("dry_run", true, "Print the changes out")

	flag.Parse()

	if *tableName == "" {
		log.Fatal("Please provide a table name using --table flag")
	}

	// Open a connection to the database
	dsn := fmt.Sprintf("root:@tcp(localhost:3306)/%s", *dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Determine the columns of the composite primary key
	rows, err := db.Query(`SELECT COLUMN_NAME
                           FROM information_schema.KEY_COLUMN_USAGE
                           WHERE TABLE_NAME = ?
                             AND CONSTRAINT_NAME = 'PRIMARY'`, *tableName)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			log.Fatal(err)
		}
		primaryKeys = append(primaryKeys, key)
	}

	// Query all rows needed to delete, use the primary key only because we want to change the data based on the where clause
	// sql := fmt.Sprintf("SELECT "+strings.Join(primaryKeys, ",")+" FROM %s WHERE %s", *tableName, *whereClause)
	sql := fmt.Sprintf("SELECT * FROM %s WHERE %s", *tableName, *whereClause)

	// log.Println(sql)

	// the previous rows are closed
	rows, err = db.Query(sql)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Prepare the delete statement
	whereConditions := make([]string, len(primaryKeys))
	for i, key := range primaryKeys {
		whereConditions[i] = fmt.Sprintf("%s = ?", key)
	}

	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s", *tableName, strings.Join(whereConditions, " AND "))
	stmt, err := db.Prepare(deleteSQL)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	// Concurrency control
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, *concurrency) // Limit to 5 concurrent deletions

	// Iterate over the rows
	cols, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}

	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			log.Fatal(err)
		}

		// Extract primary key values
		var primaryKeyValues []interface{}
		for _, key := range primaryKeys {
			for i, col := range cols {
				if col == key {
					primaryKeyValues = append(primaryKeyValues, values[i])
					break
				}
			}
		}

		if *dryRun {
			fmt.Printf("%s;\n", utils.InterpolateQuery(deleteSQL, primaryKeyValues))
			continue
		}

		// Delete row by composite primary key concurrently
		wg.Add(1)
		semaphore <- struct{}{}
		go func(values []interface{}) {
			defer wg.Done()
			_, err := stmt.Exec(values...)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("Deleted row with %s\n", strings.Join(whereConditions, " AND "))
			<-semaphore
		}(primaryKeyValues)
	}

	wg.Wait()

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
