package main

import (
	// "context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/joho/godotenv/autoload"
	_ "github.com/lib/pq"
)

type Coords struct {
	Latitude  float64
	Longitude float64
}

type TaxiTrips struct {
	TripID             string
	TaxiID             string
	TripStartTimestamp time.Time
	PickupLatitude     float64
	PickupLongitude    float64
	DropoffLatitude    float64
	DropoffLongitude   float64
	PickupLocationName string
	PickupZipCode      string
	DropoffZipCode     string
}

type CovidReport struct {
	Zipcode     string
	WeekStart   time.Time
	WeekEnd     time.Time
	TestsWeekly int
	PctPos      float64
	population  int
	cases       int
}

type Nominatim struct {
	PlaceId     int              `json:"place_id"`
	Category    string           `json:"category"`
	DisplayName string           `json:"display_name"`
	Address     NominatimAddress `json:"address"`
	Boundingbox []string         `json:"boundingbox"`
}

type NominatimAddress struct {
	HomeNumber    int    `json:"house_number"`
	Road          string `json:"road"`
	Neighbourhood string `json:"neighbourhood"`
	Suburb        string `json:"suburb"`
	City          string `json:"city"`
	Municipality  string `json:"municipality"`
	County        string `json:"county"`
	Postcode      string `json:"postcode"`
}

var Trips []TaxiTrips
var CovidReports []CovidReport

// data lake connection
func DLConnect() (*sql.DB, error) {
	//Retreiving DB connection credential environment variables
	err := godotenv.Load(".env")
	var DLHOST = os.Getenv("DLHOST")
	var DLPORT = os.Getenv("DLPORT")
	var DLUSER = os.Getenv("DLUSER")
	var DLPASSWORD = os.Getenv("DLPASSWORD")
	var DLDBNAME = os.Getenv("DLDBNAME")
	if err != nil {
		log.Println("Could not load .env file", err)
	}

	DB_DSN := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", DLHOST, DLPORT, DLUSER, DLPASSWORD, DLDBNAME)

	db, err := sql.Open("postgres", DB_DSN)

	if err != nil {
		return nil, err
	}

	log.Println("Successfully connected to Data Lake")

	return db, nil
}

// data mart connection
func DMConnect() (*sql.DB, error) {
	//Retreiving DB connection credential environment variables
	fmt.Println("connecting to db")
	err := godotenv.Load(".env")
	var DMHOST = os.Getenv("DMHOST")
	var DMPORT = os.Getenv("DMPORT")
	var DMUSER = os.Getenv("DMUSER")
	var DMPASSWORD = os.Getenv("DMPASSWORD")
	var DMDBNAME = os.Getenv("DMDBNAME")
	if err != nil {
		log.Println("Could not load .env file")
	}

	DB_DSN := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", DMHOST, DMPORT, DMUSER, DMPASSWORD, DMDBNAME)

	db, err := sql.Open("postgres", DB_DSN)

	if err != nil {
		return nil, err
	}

	log.Println("Successfully connected to Data Mart")

	return db, nil
}

func String2Float(s string) float64 {
	value, _ := strconv.ParseFloat(s, 64)
	return value
}

func String2Int(s string) int {
	value, _ := strconv.Atoi(s)
	return value
}

func String2Timestamp(s string) time.Time {
	// '2023-01-01T00:00:00.000'
	const format = "2006-01-02T15:04:05.000"
	timestamp, err := time.Parse(format, s)
	if err != nil {
		log.Println("Error converting timestamp: ", timestamp, err)
	}
	return timestamp
}

func query_taxis() []TaxiTrips {
	db, err := DLConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	// Limiting query to 10,000 records to allow for runtime <1 hour (Cloud Run timeout limit)
	statement := `SELECT TripID, TaxiID, TripStartTimestamp, PickupCentroidLatitude, PickupCentroidLongitude, DropoffCentroidLatitude, DropoffCentroidLongitude FROM taxi_trips LIMIT 10000`

	rows, err := db.Query(statement)
	if err != nil {
		log.Fatal("Error querying database: ", err)
	}

	Data := []TaxiTrips{}

	for rows.Next() {
		var tripID string
		var taxiID string
		var startTimestamp string
		var pickupLatitude string
		var pickupLongitude string
		var dropoffLatitude string
		var dropoffLongitude string
		err = rows.Scan(&tripID, &taxiID, &startTimestamp, &pickupLatitude, &pickupLongitude, &dropoffLatitude, &dropoffLongitude)
		if err != nil {
			log.Fatal("Scan error", err)
		}
		temp := TaxiTrips{TripID: tripID, TaxiID: taxiID, TripStartTimestamp: String2Timestamp(startTimestamp), PickupLatitude: String2Float(pickupLatitude), PickupLongitude: String2Float(pickupLongitude), DropoffLatitude: String2Float(dropoffLatitude), DropoffLongitude: String2Float(dropoffLongitude)}

		Data = append(Data, temp)
	}

	defer rows.Close()

	return Data
}

func query_covid() []CovidReport {
	db, err := DLConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	// Limiting query to 10,000 records to allow for runtime <1 hour (Cloud Run timeout limit)
	statement := `SELECT zipcode, weekstart, weekend, testsweekly, pctpositiveweekly, population FROM weekly_covid_by_zip LIMIT 10000`

	rows, err := db.Query(statement)
	if err != nil {
		log.Fatal("Error querying database: ", err)
	}

	Data := []CovidReport{}

	for rows.Next() {
		var zipcode string
		var weekstart string
		var weekend string
		var testsweekly string
		var pctpositiveweekly string
		var population string
		err = rows.Scan(&zipcode, &weekstart, &weekend, &testsweekly, &pctpositiveweekly, &population)
		if err != nil {
			log.Fatal("Scan error", err)
		}
		temp := CovidReport{Zipcode: zipcode, WeekStart: String2Timestamp(weekstart), WeekEnd: String2Timestamp(weekend), TestsWeekly: String2Int(testsweekly), PctPos: String2Float(pctpositiveweekly), population: String2Int(population)}

		Data = append(Data, temp)
	}

	defer rows.Close()

	return Data
}

func GetZipCode(userAgent string, lat, lon float64) string {
	var myresults Nominatim
	url := fmt.Sprintf("https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat=%f&lon=%f", lat, lon)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Println(err)
	}

	req.Header.Set("User-Agent", userAgent)

	client := &http.Client{}

	resp, err := client.Do(req)

	if err != nil {
		fmt.Println(err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {

		fmt.Println(resp.StatusCode)

	}

	resBody, err := ioutil.ReadAll(resp.Body)

	if err != nil {

		fmt.Println(err)

	}

	json.Unmarshal(resBody, &myresults)

	return myresults.Address.Postcode
}

func CreateDataMartTables() {
	fmt.Println("\ncreating data mart tables")
	db, err := DMConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	dropTableStatementTrips := "DROP TABLE IF EXISTS requirement_1_taxi_trips;"

	_, err = db.Exec(dropTableStatementTrips)
	if err != nil {
		panic(err)
	}

	createTableStatementTrips := `CREATE TABLE requirement_1_taxi_trips (
								TripID               TEXT PRIMARY KEY,
								TaxiID               TEXT,
								TripStartTimestamp   TIMESTAMPTZ,
								PickupLatitude       FLOAT,
								PickupLongitude      FLOAT,
								DropoffLatitude      FLOAT,
								DropoffLongitude     FLOAT,
								PickupZipCode       VARCHAR(6),
								DropoffZipCode       VARCHAR(6)
							);`

	_, err = db.Exec(createTableStatementTrips)
	if err != nil {
		panic(err)
	}

	dropTableStatementCovid := "DROP TABLE IF EXISTS requirement_1_covid_reports;"

	_, err = db.Exec(dropTableStatementCovid)
	if err != nil {
		panic(err)
	}

	createTableStatementCovid := `CREATE TABLE requirement_1_covid_reports (
								Zipcode               TEXT,
								WeekStart   TIMESTAMPTZ,
								WeekEnd       TIMESTAMPTZ,
								TestsWeekly      INT,
								PctPos      FLOAT,
								Population     INT,
								Cases INT
							);`

	_, err = db.Exec(createTableStatementCovid)
	if err != nil {
		panic(err)
	}

	fmt.Println("data mart tables created")
}

func LoadToDataMart(tripRecords []TaxiTrips, covidReports []CovidReport) {
	db, err := DMConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	fmt.Println(tripRecords[1])

	insertStatement1 := `INSERT INTO requirement_1_taxi_trips (TripID, TaxiID, TripStartTimestamp, PickupLatitude, PickupLongitude, DropoffLatitude, DropoffLongitude, PickupZipCode, DropoffZipCode)
							values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
							ON CONFLICT (TripID)
							DO NOTHING;`

	for _, v := range tripRecords {
		_, err = db.Exec(insertStatement1, v.TripID, v.TaxiID, v.TripStartTimestamp, v.PickupLatitude, v.PickupLongitude, v.DropoffLatitude, v.DropoffLongitude, v.PickupZipCode, v.DropoffZipCode)
		if err != nil {
			log.Println("Error inserting record, TripID = ", v.TripID, err)
		}
	}

	insertStatement2 := `INSERT INTO requirement_1_covid_reports (ZipCode, WeekStart, WeekEnd, TestsWeekly, PctPos, Population, Cases) 
							values ($1, $2, $3, $4, $5, $6, $7);`

	for _, v := range covidReports {
		_, err = db.Exec(insertStatement2, v.Zipcode, v.WeekStart, v.WeekEnd, v.TestsWeekly, v.PctPos, v.population, v.cases)
		if err != nil {
			log.Printf("failed to insert: %v", err)
		}
	}
}

func main() {
	Trips = query_taxis()
	CovidReports = query_covid()

	for i, val := range CovidReports {
		CovidReports[i].cases = int(math.Round(val.PctPos * float64(val.TestsWeekly)))
	}

	fmt.Println(CovidReports[1])

	UniqueZipCoords := make(map[Coords]string)

	// fmt.Println(CovidReports[1])

	for i, val := range Trips {
		Trips[i].PickupLatitude = math.Round(val.PickupLatitude*1000) / 1000
		Trips[i].PickupLongitude = math.Round(val.PickupLongitude*1000) / 1000
		Trips[i].DropoffLatitude = math.Round(val.DropoffLatitude*1000) / 1000
		Trips[i].DropoffLongitude = math.Round(val.DropoffLongitude*1000) / 1000
	}

	for i, val := range Trips {

		var dropCoords Coords

		dropCoords.Latitude = val.DropoffLatitude
		dropCoords.Longitude = val.DropoffLongitude

		_, keyPresent := UniqueZipCoords[dropCoords]
		if keyPresent {
			Trips[i].DropoffZipCode = UniqueZipCoords[dropCoords]
		} else {
			zip := GetZipCode("msds432-final-group-4", dropCoords.Latitude, dropCoords.Longitude)
			UniqueZipCoords[dropCoords] = zip
			Trips[i].DropoffZipCode = zip
		}
	}

	for i, val := range Trips {

		var pickCoords Coords

		pickCoords.Latitude = val.PickupLatitude
		pickCoords.Longitude = val.PickupLongitude

		_, keyPresent := UniqueZipCoords[pickCoords]
		if keyPresent {
			Trips[i].PickupZipCode = UniqueZipCoords[pickCoords]
		} else {
			zip := GetZipCode("msds432-final-group-4", pickCoords.Latitude, pickCoords.Longitude)
			UniqueZipCoords[pickCoords] = zip
			Trips[i].PickupZipCode = zip
		}
	}

	// fmt.Println(Trips[1])

	// Insert to Data Mart
	CreateDataMartTables()

	fmt.Println("loading to data mart...")
	LoadToDataMart(Trips, CovidReports)

}
