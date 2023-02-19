package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

// API endpoint URL
const url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json"

// Define struct for individual records
type CommunityReport struct {
	CommunityArea     string `json:"community_area"`
	CommunityAreaName string `json:"community_area_name"`
	BelowPoverty      string `json:"below_poverty_level"`
	PerCapitaIncome   string `json:"per_capita_income"`
	Unemployment      string `json:"unemployment"`
}

var Reports []CommunityReport

func GetAPIrequest(url string) []CommunityReport {
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

	if err := json.Unmarshal(body, &Reports); err != nil {
		fmt.Printf("Cannot unmarshal JSON: %v", err)
	}

	return Reports
}

// function for saving JSON file for testing - eliminate excessive API calls
func SaveTripsJSON(filename string) {
	content, err := json.Marshal(Reports)
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
	err = json.Unmarshal(input, &Reports)
	if err != nil {
		log.Fatalf("Error while unmarshaling json to struct: %v", err)
	}
}

func DbConnect() (*sql.DB, error) {
	//Retreiving DB connection credential environment variables
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Println("Could not load .env file")
	}

	HOST := os.Getenv("HOST")
	PORT := os.Getenv("PORT")
	USER := os.Getenv("USER")
	PASSWORD := os.Getenv("PASSWORD")
	DBNAME := os.Getenv("DBNAME")

	DB_DSN := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", HOST, PORT, USER, PASSWORD, DBNAME)

	db, err := sql.Open("postgres", DB_DSN)

	if err != nil {
		return nil, err
	}

	// err = db.Ping()
	// if err != nil {
	// 	panic(err)
	// }

	fmt.Println("Successfully connected to DB")

	return db, nil
}

func refresh_db_table() {
	db, err := DbConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	dropTableStatement := "DROP TABLE IF EXISTS community_health;"

	_, err = db.Exec(dropTableStatement)
	if err != nil {
		panic(err)
	}

	createTableStatement := `CREATE TABLE community_health (
		CommunityArea        TEXT PRIMARY KEY,
		CommunityAreaName    TEXT,
		BelowPoverty         TEXT,
		PerCapitaIncome      TEXT,
		Unemployment TEXT
		);`

	_, err = db.Exec(createTableStatement)
	if err != nil {
		panic(err)
	}
}

func load_to_db(Reports []CommunityReport) {
	db, err := DbConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	insertStatement := `INSERT INTO community_health (CommunityArea, CommunityAreaName, BelowPoverty, PerCapitaIncome, Unemployment) 
							values ($1, $2, $3, $4, $5)
							ON CONFLICT (CommunityArea) 
							DO NOTHING;`

	for _, v := range Reports {
		_, err = db.Exec(insertStatement,
			v.CommunityArea,
			v.CommunityAreaName,
			v.BelowPoverty,
			v.PerCapitaIncome,
			v.Unemployment,
		)
		if err != nil {
			fmt.Printf("Error inserting record, Community = %v", v.CommunityAreaName)
		}
	}
}

func test_successful_insert() {
	db, err := DbConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	testStatement1 := "SELECT CommunityAreaName FROM community_health LIMIT 10"
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
	// fmt.Printf("%v", Reports)

	// Putting this here to eliminate making API calls over and over while testing
	// SaveTripsJSON("reports.json")

	// Loading from json file to avoid unnecessary API calls
	// LoadTripsJSON("reports.json")

	// reducing file size to manage Google Cloud credit consumption
	// LessReports := Reports[0:1000]

	// Drop and re-create table
	refresh_db_table()

	// Ingest records to DB
	load_to_db(Reports)

	// Query DB to confirm
	test_successful_insert()

}
