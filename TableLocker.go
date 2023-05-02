package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TwiN/go-color"

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
		log.Fatalln("Error: schema name is required")
	}

	if dbHost == "" || dbPort == 0 || dbUser == "" || dbPassword == "" || dbName == "" {
		log.Fatalln("Error: database connection parameters are required")
	}
	lockStart := time.Now()
	// Create Connection
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", dbHost, dbPort, dbUser, dbPassword, dbName))
	if err != nil {
		log.Fatalf("Error connecting to database: %v\n", err)
	}
	defer db.Close()

	// Max Connection
	db.SetMaxOpenConns(maxConnect)
	db.SetMaxIdleConns(maxConnect)
	db.SetConnMaxLifetime(24 * time.Hour)

	// Schema
	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = $1", schema)
	if err != nil {
		log.Fatalf("Error querying tables: %v\n", err)
	}
	defer rows.Close()

	// Start Lock
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		log.Fatalf("Error starting transaction: %v\n", err)
	}
	defer tx.Rollback()

	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			log.Fatalf("Error scanning table name: %v\n", err)
		}
		isTableLock := checkTableLockStatus(tx, schema, tableName)

		if isTableLock {
			schemaTables = append(schemaTables, SchemaTable{schema, tableName})
		} else {

			err := lockTableWithTimeout(ctx, tx, schema, tableName)
			if err != nil {
				switch {
				case errors.Is(err, errTimeout):
					log.Printf("Error lock table %s: %v timeout\n", tableName, err)

				default:
					log.Fatalf("Error locking table %s: %v\n", tableName, err)
				}
			} else {
				//log.Printf("Table %s locked.\n", schema+"."+tableName)
			}
		}

	}
	lockDuration := time.Since(lockStart)
	log.Printf("All Table locked in %v.\n", lockDuration)

	for {
		if len(schemaTables) != 0 {
			log.Println(color.Ize(color.Red, "Following table lock failed, rerun after 5sec"))
			for _, schemaTable := range schemaTables {
				log.Println(color.Ize(color.Red, schemaTable.Schema+"."+schemaTable.Table))
			}

			time.Sleep(5 * time.Second)
			for _, schemaTable := range schemaTables {
				schemaTables = schemaTables[1:] //remove first one
				lockTableWithTimeout(ctx, tx, schemaTable.Schema, schemaTable.Table)
			}
		} else {
			break
		}
	}

	// Releasing
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT)
	go func() {
		<-sig
		log.Println("Received SIGINT, releasing table locks...")
		tx.Rollback()
		os.Exit(1)
	}()

	// Exit
	log.Printf("Locked tables in schema %s, press Ctrl+C to release...\n", schema)
	for {
		time.Sleep(time.Second)
	}
}

func lockTableWithTimeout(ctx context.Context, tx *sql.Tx, schemaName string, tableName string) error {

	query := "LOCK TABLE " + schemaName + "." + tableName + " IN SHARE MODE"

	done := make(chan error, 1)
	go func() {
		_, err := tx.Exec(query)
		done <- err
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		return errTimeout
	case <-ctx.Done():
		return ctx.Err()
	}

}

func checkTableLockStatus(tx *sql.Tx, schemaName string, tableName string) bool {

	query := "SELECT * FROM pg_locks WHERE relation::regclass::text = '" + schemaName + "." + tableName + "' "

	r, err := tx.Exec(query)

	if err != nil {
		log.Fatalf("Error Query: %v\n", err)
	}

	i, _ := r.RowsAffected()

	if i == 0 {
		return false
	} else {
		return true
	}
}
