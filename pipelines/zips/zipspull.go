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
const url = "https://data.cityofchicago.org/resource/unjd-c2ca.json"

// Define struct for individual records
type Zipcodes struct {
	theGeom    string //`json:"thegeom"`
	OBJECTID   string `json:"objectid"`
	ZIP        string `json:"zip"`
	SHAPE_AREA string `json:"shape_area"`
	SHAPE_LEN  string `json:"shape_len"`
}

var Zips []Zipcodes

func GetAPIrequest(url string) []Zipcodes {
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

	if err := json.Unmarshal(body, &Zips); err != nil {
		fmt.Printf("Cannot unmarshal JSON: %v ", err)
	}

	return Zips
}

// function for saving JSON file for testing - eliminate excessive API calls
func SaveZipsJSON(filename string) {
	content, err := json.Marshal(Zips)
	if err != nil {
		log.Fatalf("Error while marshaling struct: %v", err)
	}
	err = os.WriteFile(filename, content, 0777)
	if err != nil {
		log.Fatalf("Error while writing to json file: %v", err)
	}
}

// function for loading the saved JSON file for testing - eliminate excessive API calls
func LoadZipsJSON(filename string) {
	input, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Error while reading json file %v", err)
	}
	err = json.Unmarshal(input, &Zips)
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

	dropTableStatement := "DROP TABLE IF EXISTS Zipcodes;"

	_, err = db.Exec(dropTableStatement)
	if err != nil {
		panic(err)
	}

	createTableStatement := `CREATE TABLE Zipcodes (
								TheGeom                 TEXT PRIMARY KEY,
								OBJECTID                TEXT,
								ZIP 				    TEXT,
								SHAPE_AREA        		TEXT,
								SHAPE_LEN               TEXT
							);`

	_, err = db.Exec(createTableStatement)
	if err != nil {
		panic(err)
	}
}

func load_to_db(Zips []Zipcodes) {
	db, err := DbConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	insertStatement := `INSERT INTO Zipcodes (TheGeom, OBJECTID, ZIP, SHAPE_AREA, SHAPE_LEN) 
							values ($1, $2, $3, $4, $5)
							ON CONFLICT (TheGeom) 
							DO NOTHING;`

	for _, v := range Zips {
		_, err = db.Exec(insertStatement, v.theGeom, v.OBJECTID, v.ZIP, v.SHAPE_AREA, v.SHAPE_LEN)
		if err != nil {
			fmt.Printf("Error inserting record, theGeom = %v", v.theGeom)
		}
	}
}

func test_successful_insert() {
	db, err := DbConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	testStatement1 := "SELECT SHAPE_AREA FROM Zipcodes LIMIT 10"
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
	// SaveZipsJSON("Zipcodes.json")

	// // Loading from json file to avoid unnecessary API calls
	// LoadZipsJSON("Zipcodes.json")

	// reducing file size to manage Google Cloud credit consumption
	//LessZips := Zips[0:1000]
	//fmt.Println(LessZips)

	// Drop and re-create table
	refresh_db_table()

	// Ingest records to DB
	load_to_db(Zips)

	// Query DB to confirm
	test_successful_insert()

}
