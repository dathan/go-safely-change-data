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
	rowsTableStructure, err := db.Query(`SELECT COLUMN_NAME
                           FROM information_schema.KEY_COLUMN_USAGE
                           WHERE TABLE_NAME = ?
                             AND CONSTRAINT_NAME = 'PRIMARY'`, *tableName)
	if err != nil {
		log.Fatal(err)
	}
	defer rowsTableStructure.Close()

	var primaryKeys []string
	for rowsTableStructure.Next() {
		var key string
		if err := rowsTableStructure.Scan(&key); err != nil {
			log.Fatal(err)
		}
		primaryKeys = append(primaryKeys, key)
	}

	// Query all rows needed to delete, use the primary key only because we want to change the data based on the where clause
	// sql := fmt.Sprintf("SELECT "+strings.Join(primaryKeys, ",")+" FROM %s WHERE %s", *tableName, *whereClause)
	sql := fmt.Sprintf("SELECT * FROM %s WHERE %s", *tableName, *whereClause)
	// log.Println(sql)

	// the previous rows are closed
	rowsStream, err := db.Query(sql)
	if err != nil {
		log.Fatal(err)
	}
	defer rowsStream.Close()

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
	cols, err := rowsStream.Columns()
	if err != nil {
		log.Fatal(err)
	}

	// scan will write to the values array from the valuesPtr
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// rows.Next is using streaming. So, we are not buffering the entire table locally
	for rowsStream.Next() {
		err := rowsStream.Scan(valuePtrs...)
		if err != nil {
			log.Fatal(err)
		}

		// Extract primary key values
		primaryKeyValues := extractPrimaryKeys(cols, primaryKeys, values)

		// printOut what you will do 1st, incase you need to check in the changes into git
		if *dryRun {
			fmt.Printf("%s;\n", utils.InterpolateQuery(deleteSQL, primaryKeyValues))
			continue
		}

		// Delete row by composite primary key concurrently
		wg.Add(1)
		semaphore <- struct{}{}
		go executeDeleteQuery(&wg, semaphore, stmt, primaryKeyValues, whereConditions)
	} // end stream

	wg.Wait()

	// Check for errors from iterating over rows
	if err := rowsStream.Err(); err != nil {
		log.Fatal(err)
	}
}

// extract the primary key value from the table structure
func extractPrimaryKeys(cols, primaryKeys []string, values []interface{}) []interface{} {
	var primaryKeyValues []interface{}
	for _, key := range primaryKeys {
		for i, col := range cols {
			if col == key {
				primaryKeyValues = append(primaryKeyValues, values[i])
				break
			}
		}
	}
	return primaryKeyValues
}

// run the delete
func executeDeleteQuery(wg *sync.WaitGroup, semaphore chan struct{}, stmt *sql.Stmt, values []interface{}, whereConditions []string) {
	defer wg.Done()
	_, err := stmt.Exec(values...)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Deleted row with %s\n", strings.Join(whereConditions, " AND "))
	<-semaphore
}
