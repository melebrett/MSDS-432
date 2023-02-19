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
const url = "https://data.cityofchicago.org/resource/yhhz-zm2v.json"

// Define struct for individual records
type ZipInfo struct {
	RowId                 string `json:"row_id"`
	ZipCode               string `json:"zip_code"`
	WeekNum               string `json:"week_number"`
	WeekStart             string `json:"week_start"`
	WeekEnd               string `json:"week_end"`
	TestsWeekly           string `json:"test_weekly"`
	TestsCumulative       string `json:"tests_cumulative"`
	TestRateWeekly        string `json:"test_rate_weekly"`
	TestRateCumulative    string `json:"test_rate_cumulative"`
	PctPositiveWeekly     string `json:"percent_tested_positive_weekly"`
	PctPositiveCumulative string `json:"percent_tested_positive_cumulative"`
	DeathsWeekly          string `json:"deaths_weekly"`
	DeathsCumulative      string `json:"deaths_cumulative"`
	DeathRateWeekly       string `json:"death_rate_weekly"`
	DeathRateCumulative   string `json:"death_rate_cumulative"`
	Population            string `json:"population"`
	ZipCodeLocation       string `json:"zip_code_location"`
}

var ZipsInfo []ZipInfo

func GetAPIrequest(url string) []ZipInfo {
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

	if err := json.Unmarshal(body, &ZipsInfo); err != nil {
		fmt.Printf("Cannot unmarshal JSON: %v", err)
	}

	return ZipsInfo
}

// function for saving JSON file for testing - eliminate excessive API calls
func SaveTripsJSON(filename string) {
	content, err := json.Marshal(ZipsInfo)
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
	err = json.Unmarshal(input, &ZipsInfo)
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

	dropTableStatement := "DROP TABLE IF EXISTS weekly_covid_by_zip;"

	_, err = db.Exec(dropTableStatement)
	if err != nil {
		panic(err)
	}

	createTableStatement := `CREATE TABLE weekly_covid_by_zip (
						RowId        TEXT PRIMARY KEY,
						ZipCode TEXT,
						WeekNum TEXT,
						WeekStart TEXT,
						WeekEnd TEXT,
						TestsWeekly TEXT,
						TestsCumulative TEXT,
						TestRateWeekly TEXT,
						TestRateCumulative TEXT,
						PctPositiveWeekly TEXT,
						PctPositiveCumulative TEXT,
						DeathsWeekly TEXT,
						DeathsCumulative TEXT,
						DeathRateWeekly TEXT,
						DeathRateCumulative TEXT,
						Population          TEXT,
						ZipCodeLocation       TEXT
							);`

	_, err = db.Exec(createTableStatement)
	if err != nil {
		panic(err)
	}
}

func load_to_db(ZipsInfo []ZipInfo) {
	db, err := DbConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	insertStatement := `INSERT INTO daily_covid_cases (RowId,ZipCode,WeekNum,WeekStart,WeekEnd,TestsWeekly,TestsCumulative,TestRateWeekly,TestRateCumulative,PctPositiveWeekly,PctPositiveCumulative,DeathsWeekly,DeathsCumulative,DeathRateWeekly,DeathRateCumulative,Population,ZipCodeLocation	) 
							values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16);`

	for _, v := range ZipsInfo {
		_, err = db.Exec(insertStatement,
			v.RowId,
			v.ZipCode,
			v.WeekNum,
			v.WeekStart,
			v.WeekEnd,
			v.TestsWeekly,
			v.TestsCumulative,
			v.TestRateWeekly,
			v.TestRateCumulative,
			v.PctPositiveWeekly,
			v.PctPositiveCumulative,
			v.DeathsWeekly,
			v.DeathsCumulative,
			v.DeathRateWeekly,
			v.DeathRateCumulative,
			v.Population,
			v.ZipCodeLocation,
		)
		if err != nil {
			fmt.Printf("Error inserting record, RowId = %v", v.RowId)
		}
	}
}

func test_successful_insert() {
	db, err := DbConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	testStatement1 := "SELECT RowId FROM weekly_covid_by_zip LIMIT 10"
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
	// fmt.Printf("%v", ZipsInfo)

	// Putting this here to eliminate making API calls over and over while testing
	// SaveTripsJSON("reports.json")

	// Loading from json file to avoid unnecessary API calls
	// LoadTripsJSON("reports.json")

	// reducing file size to manage Google Cloud credit consumption
	// LessReports := Reports[0:1000]

	// Drop and re-create table
	refresh_db_table()

	// Ingest records to DB
	load_to_db(ZipsInfo)

	// Query DB to confirm
	test_successful_insert()

}
