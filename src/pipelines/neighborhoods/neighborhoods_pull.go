package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"

	"cloud.google.com/go/cloudsqlconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
)

// API endpoint URL
const url = "https://data.cityofchicago.org/resource/y6yq-dbs2.json"

// Define struct for individual records
type Neighborhoods struct {
	TheGeom struct {
		GeoType     string          `json:"type"`
		Coordinates [][][][]float64 `json:"coordinates"`
	} `json:"the_geom"`
	PRI_NEIGH  string `json:"pri_neigh"`
	SEC_NEIGH  string `json:"sec_neigh"`
	SHAPE_AREA string `json:"shape_area"`
	SHAPE_LEN  string `json:"shape_len"`
}

var Neighs []Neighborhoods

func GetAPIrequest(url string) []Neighborhoods {
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Error: API get request failed. %v", err)
	}
	defer resp.Body.Close()

	fmt.Println("API request completed")

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error: Failed to read API response: %v", err)
	}

	// TESTING PRINT
	fmt.Println("Response read successfully")

	if err := json.Unmarshal(body, &Neighs); err != nil {
		log.Printf("Cannot unmarshal JSON: %v ", err)
	}

	return Neighs
}

// function for saving JSON file for testing - eliminate excessive API calls
func SaveNeighsJSON(filename string) {
	content, err := json.Marshal(Neighs)
	if err != nil {
		log.Fatalf("Error while marshaling struct: %v", err)
	}
	err = os.WriteFile(filename, content, 0777)
	if err != nil {
		log.Fatalf("Error while writing to json file: %v", err)
	}
}

// function for loading the saved JSON file for testing - eliminate excessive API calls
func LoadNeighsJSON(filename string) {
	input, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Error while reading json file %v", err)
	}
	err = json.Unmarshal(input, &Neighs)
	if err != nil {
		log.Fatalf("Error while unmarshaling json to struct: %v", err)
	}
}

func DbConnect() (*sql.DB, error) {
	mustGetenv := func(k string) string {
		v := os.Getenv(k)
		if v == "" {
			log.Fatalf("Fatal Error in connect_connector.go: %s environment variable not set.\n", k)
		}
		return v
	}

	var (
		dbUser                 = mustGetenv("USER")     // e.g. 'my-db-user'
		dbPwd                  = mustGetenv("PASSWORD") // e.g. 'my-db-password'
		dbName                 = mustGetenv("DBNAME")   // e.g. 'my-database'
		instanceConnectionName = mustGetenv("INSTANCE") // e.g. 'project:region:instance'
	)

	dsn := fmt.Sprintf("user=%s password=%s database=%s", dbUser, dbPwd, dbName)
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	var opts []cloudsqlconn.Option
	d, err := cloudsqlconn.NewDialer(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	// Use the Cloud SQL connector to handle connecting to the instance.
	// This approach does *NOT* require the Cloud SQL proxy.
	config.DialFunc = func(ctx context.Context, network, instance string) (net.Conn, error) {
		return d.Dial(ctx, instanceConnectionName)
	}
	dbURI := stdlib.RegisterConnConfig(config)
	dbPool, err := sql.Open("pgx", dbURI)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %v", err)
	}
	return dbPool, nil
}

func refresh_db_table() {
	db, err := DbConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	dropTableStatement := "DROP TABLE IF EXISTS neighborhoods;"

	_, err = db.Exec(dropTableStatement)
	if err != nil {
		panic(err)
	}

	createTableStatement := `CREATE TABLE neighborhoods (
								GeoType                 TEXT,
								Latitude				TEXT,
								Longitude				TEXT,
								PRI_NEIGH               TEXT,
								SEC_NEIGH			    TEXT,
								SHAPE_AREA        		TEXT,
								SHAPE_LEN               TEXT
							);`

	_, err = db.Exec(createTableStatement)
	if err != nil {
		panic(err)
	}
}

func load_to_db(Neighs []Neighborhoods) {
	db, err := DbConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	insertStatement := `INSERT INTO neighborhoods (GeoType, Latitude, Longitude, PRI_NEIGH, SEC_NEIGH, SHAPE_AREA, SHAPE_LEN) 
							values ($1, $2, $3, $4, $5, $6, $7);`

	for _, v := range Neighs {
		for _, val := range v.TheGeom.Coordinates[0][0] {
			_, err = db.Exec(insertStatement, v.TheGeom.GeoType, val[0], val[1], v.PRI_NEIGH, v.SEC_NEIGH, v.SHAPE_AREA, v.SHAPE_LEN)
			if err != nil {
				fmt.Printf("Error inserting record, theGeom = %v", v.TheGeom)
			}
		}

	}
}

func test_successful_insert() {
	db, err := DbConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	testStatement1 := "SELECT PRI_NEIGH FROM neighborhoods LIMIT 10"
	rows, err := db.Query(testStatement1)
	if err != nil {
		panic(err)
	}

	defer rows.Close()

	for rows.Next() {
		var TheGeom string
		err = rows.Scan(&TheGeom)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(TheGeom)
	}
}

func main() {
	GetAPIrequest(url)

	// // Putting this here to eliminate making API calls over and over while testing
	// SaveNeighsJSON("neighborhoods.json")

	// // Loading from json file to avoid unnecessary API calls
	// LoadNeighsJSON("neighborhoods.json")

	// reducing file size to manage Google Cloud credit consumption
	//LessNeighs := Neighs[0:1000]
	//fmt.Println(LessNeighs)

	// Drop and re-create table
	refresh_db_table()

	// Ingest records to DB
	load_to_db(Neighs)

	// Query DB to confirm
	test_successful_insert()

}
