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
const url = "https://data.cityofchicago.org/resource/wrvz-psew.json"

// Define struct for individual records
type TaxiTrip struct {
	TripID                  string `json:"trip_id"`
	TaxiID                  string `json:"taxi_id"`
	TripStartTimestamp      string `json:"trip_start_timestamp"`
	TripEndTimestamp        string `json:"trip_end_timestamp"`
	TripSeconds             string `json:"trip_seconds"`
	TripMiles               string `json:"trip_miles"`
	PickupCensusTract       string `json:"pickup_census_tract"`
	DropoffCensusTract      string `json:"dropoff_census_tract"`
	PickupCommunityArea     string `json:"pickup_community_area"`
	DropoffCommunityArea    string `json:"dropoff_community_area"`
	Fare                    string `json:"fare"`
	Tips                    string `json:"tips"`
	Tolls                   string `json:"tolls"`
	Extras                  string `json:"extras"`
	TripTotal               string `json:"trip_total"`
	PaymentType             string `json:"payment_type"`
	Company                 string `json:"company"`
	PickupCentroidLatitude  string `json:"pickup_centroid_latitude"`
	PickupCentroidLongitude string `json:"pickup_centroid_longitude"`
	//PickupCentroidLocation   string `json:"pickup_centroid_location"`		// Excluding the point objects since we already have lat/long pairs - I see no reason to define a separate Point struct for these values
	DropoffCentroidLatitude  string `json:"dropoff_centroid_latitude"`
	DropoffCentroidLongitude string `json:"dropoff_centroid_longitude"`
	//DropoffCentroidLocation  string `json:"dropoff_centroid_location"`
}

var Trips []TaxiTrip

func GetAPIrequest(url string) []TaxiTrip {
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Error: API get request failed. %v", err)
	}
	defer resp.Body.Close()

	// TESTING PRINT
	fmt.Println("API request completed")

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error: Failed to read API response: %v", err)
	}

	// TESTING PRINT
	fmt.Println("Response read successfully")

	if err := json.Unmarshal(body, &Trips); err != nil {
		fmt.Printf("Cannot unmarshal JSON: %v", err)
	}

	return Trips
}

// function for saving JSON file for testing - eliminate excessive API calls
func SaveTripsJSON(filename string) {
	content, err := json.Marshal(Trips)
	if err != nil {
		log.Fatalf("Error while marshaling struct: %v", err)
	}
	err = os.WriteFile(filename, content, 0777)
	if err != nil {
		log.Fatalf("Error while writing to json file: %v", err)
	}
}

// function for loading the saved JSON file for testing - eliminate excessive API calls
func LoadTripsJSON(filename string) {
	input, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Error while reading json file %v", err)
	}
	err = json.Unmarshal(input, &Trips)
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

	dropTableStatement := "DROP TABLE IF EXISTS taxi_trips;"

	_, err = db.Exec(dropTableStatement)
	if err != nil {
		panic(err)
	}

	createTableStatement := `CREATE TABLE taxi_trips (
								TripID                  TEXT PRIMARY KEY,
								TaxiID                  TEXT,
								TripStartTimestamp      TEXT,
								TripEndTimestamp        TEXT,
								TripSeconds             TEXT,
								TripMiles               TEXT,
								PickupCensusTract       TEXT,
								DropoffCensusTract      TEXT,
								PickupCommunityArea     TEXT,
								DropoffCommunityArea    TEXT,
								Fare                    TEXT,
								Tips                    TEXT,
								Tolls                   TEXT,
								Extras                  TEXT,
								TripTotal               TEXT,
								PaymentType             TEXT,
								Company                 TEXT,
								PickupCentroidLatitude  TEXT,
								PickupCentroidLongitude TEXT,
								DropoffCentroidLatitude  TEXT,
								DropoffCentroidLongitude TEXT
							);`

	_, err = db.Exec(createTableStatement)
	if err != nil {
		panic(err)
	}
}

func load_to_db(Trips []TaxiTrip) {
	db, err := DbConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	insertStatement := `INSERT INTO taxi_trips (TripID, TaxiID, TripStartTimestamp, TripEndTimestamp, TripSeconds, TripMiles, PickupCensusTract, DropoffCensusTract, PickupCommunityArea, DropoffCommunityArea, Fare, Tips, Tolls, Extras, TripTotal, PaymentType, Company, PickupCentroidLatitude, PickupCentroidLongitude, DropoffCentroidLatitude, DropoffCentroidLongitude) 
							values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
							ON CONFLICT (TripID) 
							DO NOTHING;`

	for _, v := range Trips {
		_, err = db.Exec(insertStatement, v.TripID, v.TaxiID, v.TripStartTimestamp, v.TripEndTimestamp, v.TripSeconds, v.TripMiles, v.PickupCensusTract, v.DropoffCensusTract, v.PickupCommunityArea, v.DropoffCommunityArea, v.Fare, v.Tips, v.Tolls, v.Extras, v.TripTotal, v.PaymentType, v.Company, v.PickupCentroidLatitude, v.PickupCentroidLongitude, v.DropoffCentroidLatitude, v.DropoffCentroidLongitude)
		if err != nil {
			fmt.Printf("Error inserting record, TripId = %v", v.TripID)
		}
	}
}

func test_successful_insert() {
	db, err := DbConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	testStatement1 := "SELECT TripID FROM taxi_trips LIMIT 10"
	rows, err := db.Query(testStatement1)
	if err != nil {
		panic(err)
	}

	defer rows.Close()

	for rows.Next() {
		var TripId string
		err = rows.Scan(&TripId)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(TripId)
	}
}

func main() {
	GetAPIrequest(url)

	// // Putting this here to eliminate making API calls over and over while testing
	// SaveTripsJSON("taxi_trips.json")

	// // Loading from json file to avoid unnecessary API calls
	// LoadTripsJSON("taxi_trips.json")

	// Drop and re-create table
	refresh_db_table()

	// Ingest records to DB
	load_to_db(Trips)

	// Query DB to confirm
	test_successful_insert()

}
