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
const url = "https://data.cityofchicago.org/resource/naz8-j4nc.json"

// Define struct for individual records
type CaseReport struct {
	LabReportDate        string `json:"lab_report_date"`
	CasesTotal           string `json:"cases_total"`
	DeathsTotal          string `json:"deaths_total"`
	CasesAge_0_17        string `json:"cases_age_0_17"`
	CasesAge_18_29       string `json:"cases_age_18_29"`
	CasesAge_30_39       string `json:"cases_age_30_39"`
	CasesAge_40_49       string `json:"cases_age_40_49"`
	CasesAge_50_59       string `json:"cases_age_50_59"`
	CasesAge_60_69       string `json:"cases_age_60_69"`
	CasesAge_70_79       string `json:"cases_age_70_79"`
	CasesAge_80_         string `json:"cases_age_80_"`
	CasesAgeUnk          string `json:"cases_age_unknown"`
	CasesMale            string `json:"cases_male"`
	CasesFemale          string `json:"cases_female"`
	CasesGenderUnk       string `json:"cases_unknown_gender"`
	CasesLatinx          string `json:"cases_latinx"`
	CasesAsianNonLatinx  string `json:"cases_asian_non_latinx"`
	CasesBlackNonLatinx  string `json:"cases_black_non_latinx"`
	CasesOtherNonLatinx  string `json:"cases_other_non_latinx"`
	CasesEthUnk          string `json:"cases_unkown_race_eth"`
	DeathsAge_0_17       string `json:"deaths_0_17"`
	DeathsAge_18_29      string `json:"deaths_18_29"`
	DeathsAge_30_39      string `json:"deaths_30_39"`
	DeathsAge_40_49      string `json:"deaths_40_49"`
	DeathsAge_50_59      string `json:"deaths_50_59"`
	DeathsAge_60_69      string `json:"deaths_60_69"`
	DeathsAge_70_79      string `json:"deaths_70_79"`
	DeathsAge_80_        string `json:"deaths_80_yrs"`
	DeathsAgeUnk         string `json:"deaths_unknown_age"`
	DeathsMale           string `json:"deaths_male"`
	DeathsFemale         string `json:"deaths_female"`
	DeathsGenderUnk      string `json:"deaths_unknown_gender"`
	DeathsLatinx         string `json:"deaths_latinx"`
	DeathsAsianNonLatinx string `json:"deaths_asian_non_latinx"`
	DeathsBlackNonLatinx string `json:"deaths_black_non_latinx"`
	DeathsOtherNonLatinx string `json:"deaths_other_non_latinx"`
	DeathsEthUnk         string `json:"deaths_unknown_race_eth"`
}

var Reports []CaseReport

func GetAPIrequest(url string) []CaseReport {
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

	dropTableStatement := "DROP TABLE IF EXISTS daily_covid_cases;"

	_, err = db.Exec(dropTableStatement)
	if err != nil {
		panic(err)
	}

	createTableStatement := `CREATE TABLE daily_covid_cases (
						LabReportDate        TEXT,
						CasesTotal           TEXT,
						DeathsTotal          TEXT,
						CasesAge_0_17        TEXT,
						CasesAge_18_29       TEXT,
						CasesAge_30_39       TEXT,
						CasesAge_40_49       TEXT,
						CasesAge_50_59       TEXT,
						CasesAge_60_69       TEXT,
						CasesAge_70_79       TEXT,
						CasesAge_80_         TEXT,
						CasesAgeUnk          TEXT,
						CasesMale            TEXT,
						CasesFemale          TEXT,
						CasesGenderUnk       TEXT,
						CasesLatinx          TEXT,
						CasesAsianNonLatinx  TEXT,
						CasesBlackNonLatinx  TEXT,
						CasesOtherNonLatinx  TEXT,
						CasesEthUnk          TEXT,
						DeathsAge_0_17       TEXT,
						DeathsAge_18_29      TEXT,
						DeathsAge_30_39      TEXT,
						DeathsAge_40_49      TEXT,
						DeathsAge_50_59      TEXT,
						DeathsAge_60_69      TEXT,
						DeathsAge_70_79      TEXT,
						DeathsAge_80         TEXT,
						DeathsAgeUnk         TEXT,
						DeathsMale           TEXT,
						DeathsFemale         TEXT,
						DeathsGenderUnk      TEXT,
						DeathsLatinx         TEXT,
						DeathsAsianNonLatinx TEXT,
						DeathsBlackNonLatinx TEXT,
						DeathsOtherNonLatinx TEXT,
						DeathsEthUnk         TEXT
							);`

	_, err = db.Exec(createTableStatement)
	if err != nil {
		panic(err)
	}
}

func load_to_db(Reports []CaseReport) {
	db, err := DbConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	insertStatement := `INSERT INTO daily_covid_cases (LabReportDate, CasesTotal, DeathsTotal, CasesAge_0_17, CasesAge_18_29, CasesAge_30_39, CasesAge_40_49, CasesAge_50_59, CasesAge_60_69, CasesAge_70_79, CasesAge_80_, CasesAgeUnk, CasesMale, CasesFemale, CasesGenderUnk, CasesLatinx, CasesAsianNonLatinx, CasesBlackNonLatinx, CasesOtherNonLatinx, CasesEthUnk, DeathsAge_0_17, DeathsAge_18_29, DeathsAge_30_39, DeathsAge_40_49, DeathsAge_50_59, DeathsAge_60_69, DeathsAge_70_79, DeathsAge_80_, DeathsAgeUnk, DeathsMale, DeathsFemale, DeathsGenderUnk, DeathsLatinx, DeathsAsianNonLatinx, DeathsBlackNonLatinx, DeathsOtherNonLatinx, DeathsEthUnk) 
							values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36);`

	for _, v := range Reports {
		_, err = db.Exec(insertStatement,
			v.LabReportDate,
			v.CasesTotal,
			v.DeathsTotal,
			v.CasesAge_0_17,
			v.CasesAge_18_29,
			v.CasesAge_30_39,
			v.CasesAge_40_49,
			v.CasesAge_50_59,
			v.CasesAge_60_69,
			v.CasesAge_70_79,
			v.CasesAge_80_,
			v.CasesAgeUnk,
			v.CasesMale,
			v.CasesFemale,
			v.CasesGenderUnk,
			v.CasesLatinx,
			v.CasesAsianNonLatinx,
			v.CasesBlackNonLatinx,
			v.CasesOtherNonLatinx,
			v.CasesEthUnk,
			v.DeathsAge_0_17,
			v.DeathsAge_18_29,
			v.DeathsAge_30_39,
			v.DeathsAge_40_49,
			v.DeathsAge_50_59,
			v.DeathsAge_60_69,
			v.DeathsAge_70_79,
			v.DeathsAge_80_,
			v.DeathsAgeUnk,
			v.DeathsMale,
			v.DeathsFemale,
			v.DeathsGenderUnk,
			v.DeathsLatinx,
			v.DeathsAsianNonLatinx,
			v.DeathsBlackNonLatinx,
			v.DeathsOtherNonLatinx,
			v.DeathsEthUnk,
		)
		if err != nil {
			fmt.Printf("Error inserting record, ReportDate = %v", v.LabReportDate)
		}
	}
}

func test_successful_insert() {
	db, err := DbConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	testStatement1 := "SELECT LabReportDate FROM daily_covid_cases LIMIT 10"
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
