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
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

// API endpoint URL
const url = "https://data.cityofchicago.org/resource/building-permits.json"

// Define struct for individual records
type BuildingPermit struct {
	ID                   string `json:"id"`
	PermitNum            string `json:"permit_"`
	PermitType           string `json:"permit_type"`
	ReviewType           string `json:"review_type"`
	ApplicationStartDate string `json:"application_start_date"`
	IssueDate            string `json:"issue_date"`
	ProcessingTime       string `json:"processing_time"`
	StreetNum            string `json:"street_number"`
	StreetDirection      string `json:"street_direction"`
	StreetName           string `json:"street_name"`
	StreetSuffix         string `json:"suffix"`
	WorkDescription      string `json:"work_description"`
	TotalFee             string `json:"total_fee"`
	Contact1Type         string `json:"contact_1_type"`
	Contact1Name         string `json:"contact_1_name"`
	Contact1City         string `json:"contact_1_city"`
	Contact1State        string `json:"contact_1_state"`
	Contact1Zip          string `json:"contact_1_zipcode"`
	ReportedCost         string `json:"reported_cost"`
	CommunityArea        string `json:"community_area"`
	CensusTract          string `json:"census_tract"`
	Ward                 string `json:"ware"`
	Xcoord               string `json:"xcoordinate"`
	Ycoord               string `json:"ycoordinate"`
	Latitude             string `json:"latitude"`
	Longitude            string `json:"longitude"`
}

var Permits []BuildingPermit

func GetAPIrequest(url string) []BuildingPermit {
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

	if err := json.Unmarshal(body, &Permits); err != nil {
		fmt.Printf("Cannot unmarshal JSON: %v", err)
	}

	return Permits
}

// function for saving JSON file for testing - eliminate excessive API calls
func SaveTripsJSON(filename string) {
	content, err := json.Marshal(Permits)
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
	err = json.Unmarshal(input, &Permits)
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

	//Retreiving DB connection credential environment variables
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Println("Could not load .env file")
	}

	HOST := mustGetenv("HOST")
	PORT := mustGetenv("DBPORT")
	USER := mustGetenv("USER")
	PASSWORD := mustGetenv("PASSWORD")
	DBNAME := mustGetenv("DBNAME")

	DB_DSN := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", HOST, PORT, USER, PASSWORD, DBNAME)

	db, err := sql.Open("postgres", DB_DSN)

	if err != nil {
		return nil, err
	}

	// err = db.Ping()
	// if err != nil {
	// 	panic(err)
	// }

	log.Printf("DB %v. Type %T", db, db)

	return db, nil
}

func DbConnect2() (*sql.DB, error) {
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
	db, err := DbConnect2()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	dropTableStatement := "DROP TABLE IF EXISTS building_permits;"

	_, err = db.Exec(dropTableStatement)
	if err != nil {
		panic(err)
	}

	createTableStatement := `CREATE TABLE building_permits (
								id      						TEXT PRIMARY KEY,
								permit_num         				TEXT,
								permit_type        				TEXT,
								review_type        				TEXT,
								application_start_date          TEXT,
								issue_date               		TEXT,
								processing_time       			TEXT,
								street_num      				TEXT,
								street_direction     			TEXT,
								street_name    					TEXT,
								street_suffix                   TEXT,
								work_description                TEXT,
								total_fee                   	TEXT,
								contact_type                  	TEXT,
								contact_name               		TEXT,
								contact_city             		TEXT,
								contact_state                 	TEXT,
								contact_zip  					TEXT,
								reported_cost 					TEXT,
								community_area  				TEXT,
								census_tract 					TEXT,
								ward 							TEXT,
								x_coordinate 					TEXT,
								y_coordinate 					TEXT,
								latitude 						TEXT,
								longitude 						TEXT
							);`

	_, err = db.Exec(createTableStatement)
	if err != nil {
		panic(err)
	}
}

func load_to_db(Trips []BuildingPermit) {
	db, err := DbConnect2()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	insertStatement := `INSERT INTO building_permits (id, permit_num, permit_type, review_type, application_start_date, issue_date, processing_time, street_num, street_direction, street_name, street_suffix, work_description, total_fee, contact_type,	contact_name, contact_city,	contact_state, contact_zip,	reported_cost, community_area, census_tract, ward, x_coordinate, y_coordinate, latitude, longitude) 
							values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)
							ON CONFLICT (id) 
							DO NOTHING;`

	for _, v := range Trips {
		_, err = db.Exec(insertStatement, v.ID, v.PermitNum, v.PermitType, v.ReviewType, v.ApplicationStartDate, v.IssueDate, v.ProcessingTime, v.StreetNum, v.StreetDirection, v.StreetName, v.StreetSuffix, v.WorkDescription, v.TotalFee, v.Contact1Type, v.Contact1Name, v.Contact1City, v.Contact1State, v.Contact1Zip, v.ReportedCost, v.CommunityArea, v.CensusTract, v.Ward, v.Xcoord, v.Ycoord, v.Latitude, v.Longitude)
		if err != nil {
			fmt.Printf("Error inserting record, ID = %v", v.ID)
		}
	}
}

func test_successful_insert() {
	db, err := DbConnect2()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	testStatement1 := "SELECT id FROM building_permits LIMIT 10"
	rows, err := db.Query(testStatement1)
	if err != nil {
		panic(err)
	}

	defer rows.Close()

	for rows.Next() {
		var ID string
		err = rows.Scan(&ID)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(ID)
	}
}

func main() {
	GetAPIrequest(url)

	// // Putting this here to eliminate making API calls over and over while testing
	// SaveTripsJSON("taxi_trips.json")

	// // Loading from json file to avoid unnecessary API calls
	// LoadTripsJSON("taxi_trips.json")

	// reducing file size to manage Google Cloud credit consumption
	LessPermits := Permits[0:1000]

	// Drop and re-create table
	refresh_db_table()

	// Ingest records to DB
	load_to_db(LessPermits)

	// Query DB to confirm
	test_successful_insert()

}
