// package main is an example demonstrating how to connect to an instance of
// of GlareDB in Go.
//
// To try this example, install glaredb locally and run it in 'server' mode:
// ```
// cd ~
// curl https://glaredb.com/install.sh | sh
// ./glaredb server
// ```
//
// See: <https://github.com/GlareDB/glaredb#install>
package main

import (
	"database/sql"
	"log"
	"os"

	_ "github.com/lib/pq"
)

func main() {
	// localHostStr will work for locally running glaredb in 'server' mode. If
	// you want to connect to a remote Cloud-hosted instance, use the connection
	// string generated for you in the web client.
	//
	// See <https://docs.glaredb.com/cloud/access/connection-details/>
	localHostStr := "host=localhost port=6543 user=glaredb dbname=glaredb sslmode=disable"

	db, err := sql.Open("postgres", localHostStr)
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	var s sql.NullString
	row := db.QueryRow("SELECT 'Connected to GlareDB!'")
	if err := row.Scan(&s); err != nil {
		log.Fatal(err)
	}

	log.Println(s.String)
	os.Exit(0)
}
