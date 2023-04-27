Table Locking Script

This script is used to lock all tables in a specified schema in a PostgreSQL database.

Usage
Before running the script, you will need to provide the following parameters:

-schema: the name of the schema to lock tables in (required)
-max-connect: the maximum number of connections to the database (default: 50)
-host: the database host (required)
-port: the database port (required)
-user: the database user (required)
-password: the database password (required)
-db: the database name (required)
Once you have provided the necessary parameters, you can run the script with the following command:


$ go run table_lock.go -schema=<schema> -max-connect=<maxConnect> -host=<dbHost> -port=<dbPort> -user=<dbUser> -password=<dbPassword> -db=<dbName>

The script will lock all tables in the specified schema, and you can press Ctrl+C to release the locks.
