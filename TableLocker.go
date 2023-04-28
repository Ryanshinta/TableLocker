package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"github.com/TwiN/go-color"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/TwiN/go-color"
	_ "github.com/lib/pq"
)

/*
Author: Ryan Ouyang
27/04/2023
*/

var errTimeout = errors.New("Error: Timeout")

type SchemaTable struct {
	Schema string
	Table  string
}

var schemaTables []SchemaTable

func main() {

	var (
		schema     string
		maxConnect int
		dbHost     string
		dbPort     int
		dbUser     string
		dbPassword string
		dbName     string
	)
	flag.StringVar(&schema, "schema", "", "The name of the schema to lock tables in")
	flag.IntVar(&maxConnect, "max-connect", 50, "The maximum number of connections to the database")
	flag.StringVar(&dbHost, "host", "", "The database host")
	flag.IntVar(&dbPort, "port", 0, "The database port")
	flag.StringVar(&dbUser, "user", "", "The database user")
	flag.StringVar(&dbPassword, "password", "", "The database password")
	flag.StringVar(&dbName, "db", "", "The database name")
	flag.Parse()

	if schema == "" {
		fmt.Fprintf(os.Stderr, "Error: schema name is required\n")
		os.Exit(1)
	}

	if dbHost == "" || dbPort == 0 || dbUser == "" || dbPassword == "" || dbName == "" {
		fmt.Fprintf(os.Stderr, "Error: database connection parameters are required\n")
		os.Exit(1)
	}
	lockStart := time.Now()
	// Create Connection
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", dbHost, dbPort, dbUser, dbPassword, dbName))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Max Connection
	db.SetMaxOpenConns(maxConnect)
	db.SetMaxIdleConns(maxConnect)
	db.SetConnMaxLifetime(24 * time.Hour)

	// Schema
	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = $1", schema)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error querying tables: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	// Start Lock
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error starting transaction: %v\n", err)
		os.Exit(1)
	}
	defer tx.Rollback()

	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			fmt.Fprintf(os.Stderr, "Error scanning table name: %v\n", err)
			os.Exit(1)
		}
		//		_, err = tx.Exec("LOCK TABLE " + schema + "." + tableName + " IN EXCLUSIVE MODE")
		//		fmt.Printf("Table %s locked.\n", schema+"."+tableName)

		err := lockTableWithTimeout(ctx, tx, schema, tableName)
		if err != nil {
			switch {
			case errors.Is(err, errTimeout):

				fmt.Fprintf(os.Stderr, "Error lock table %s: %v timeout\n", tableName, err)
				schemaTables = append(schemaTables, SchemaTable{schema, tableName})

			default:
				fmt.Fprintf(os.Stderr, "Error locking table %s: %v\n", tableName, err)
				os.Exit(1)
			}
		} else {
			fmt.Printf("Table %s locked.\n", schema+"."+tableName)
		}

	}
	lockDuration := time.Since(lockStart)
	fmt.Printf("All Table locked in %v.\n", lockDuration)
	if len(schemaTables) != 0 {
		fmt.Println(color.Ize(color.Red, "Following table cannot lock, rerun after 5sec"))
		for _, schemaTable := range schemaTables {
			fmt.Println(color.Ize(color.Red, schemaTable.Schema+"."+schemaTable.Table))
		}
		time.Sleep(5 * time.Second)
	}
	// Releasing
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT)
	go func() {
		<-sig
		fmt.Println("Received SIGINT, releasing table locks...")
		tx.Rollback()
		os.Exit(1)
	}()

	// Exit
	fmt.Printf("Locked tables in schema %s, press Ctrl+C to release...\n", schema)
	for {
		time.Sleep(time.Second)
	}
}

func lockTableWithTimeout(ctx context.Context, tx *sql.Tx, schemaName string, tableName string) error {

	query := "LOCK TABLE " + schemaName + "." + tableName + " IN EXCLUSIVE MODE"

	done := make(chan error, 1)

	go func() {
		_, err := tx.Exec(query)

		done <- err
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		return errTimeout // Timeout expired, return without an error
	case <-ctx.Done():
		return ctx.Err() // Context cancelled, return the error
	}

}
