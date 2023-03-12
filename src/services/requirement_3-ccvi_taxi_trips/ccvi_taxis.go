package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/cloudsqlconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
)

type TaxiTrips struct {
	TripID              string
	TaxiID              string
	TripStartTimestamp  time.Time
	PickupLatitude      float64
	PickupLongitude     float64
	DropoffLatitude     float64
	DropoffLongitude    float64
	PickupZipCode       int
	DropoffZipCode      int
	PickupNeighborhood  string
	DropoffNeighborhood string
	PickupCCVIScore     float64
	PickupCCVICategory  string
	DropoffCCVIScore    float64
	DropoffCCVICategory string
}

type Nominatim struct {
	PlaceId     int              `json:"place_id"`
	Category    string           `json:"category"`
	DisplayName string           `json:"display_name"`
	Address     NominatimAddress `json:"address"`
	Boundingbox []string         `json:"boundingbox"`
}

type CCVI struct {
	GeoType            string
	CommunityAreaOrZip int
	CommunityAreaName  string
	CCVIScore          float64
	CCVICategory       string
}

type NominatimAddress struct {
	HomeNumber    int    `json:"house_number"`
	Road          string `json:"road"`
	Neighbourhood string `json:"neighbourhood"`
	Suburb        string `json:"suburb"`
	City          string `json:"city"`
	Municipality  string `json:"municipality"`
	County        string `json:"county"`
	Postcode      int    `json:"postcode,string"`
}

type Neighborhood struct {
	Zip              int
	NeighborhoodName string
}

var Trips []TaxiTrips
var CCVIrecords []CCVI
var NeighborhoodMapping []Neighborhood

func DLConnect() (*sql.DB, error) {
	mustGetenv := func(k string) string {
		v := os.Getenv(k)
		if v == "" {
			log.Fatalf("Fatal Error in connect_connector.go: %s environment variable not set.\n", k)
		}
		return v
	}

	var (
		dbUser                 = mustGetenv("DLUSER")     // e.g. 'my-db-user'
		dbPwd                  = mustGetenv("DLPASSWORD") // e.g. 'my-db-password'
		dbName                 = mustGetenv("DLDBNAME")   // e.g. 'my-database'
		instanceConnectionName = mustGetenv("DLINSTANCE") // e.g. 'project:region:instance'
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

func DMConnect() (*sql.DB, error) {
	mustGetenv := func(k string) string {
		v := os.Getenv(k)
		if v == "" {
			log.Fatalf("Fatal Error in connect_connector.go: %s environment variable not set.\n", k)
		}
		return v
	}

	var (
		dbUser                 = mustGetenv("DMUSER")     // e.g. 'my-db-user'
		dbPwd                  = mustGetenv("DMPASSWORD") // e.g. 'my-db-password'
		dbName                 = mustGetenv("DMDBNAME")   // e.g. 'my-database'
		instanceConnectionName = mustGetenv("DMINSTANCE") // e.g. 'project:region:instance'
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

	statement := `SELECT TripID, TaxiID, TripStartTimestamp, PickupCentroidLatitude, PickupCentroidLongitude, DropoffCentroidLatitude, DropoffCentroidLongitude FROM taxi_trips LIMIT 2500`

	rows, err := db.Query(statement)
	if err != nil {
		log.Fatal("Error querying database for taxis: ", err)
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

func query_ccvi() []CCVI {
	db, err := DLConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	statement := `SELECT geo_type, community_area_or_zip, community_area_name, ccvi_score, ccvi_category FROM covid_vulnerability`

	rows, err := db.Query(statement)
	if err != nil {
		log.Fatal("Error querying database for ccvi: ", err)
	}

	Data := []CCVI{}

	for rows.Next() {
		var geotype string
		var communityareazip string
		var communityareaname string
		var ccviscore string
		var ccvicategory string
		err = rows.Scan(&geotype, &communityareazip, &communityareaname, &ccviscore, &ccvicategory)
		if err != nil {
			log.Fatal("Scan error", err)
		}
		temp := CCVI{GeoType: geotype, CommunityAreaOrZip: String2Int(communityareazip), CommunityAreaName: communityareaname, CCVIScore: String2Float(ccviscore), CCVICategory: ccvicategory}

		Data = append(Data, temp)
	}

	defer rows.Close()

	return Data
}

func query_neighborhoods() []Neighborhood {
	db, err := DMConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	statement := `SELECT zipcode, pri_neigh FROM neighborhood_zips`

	rows, err := db.Query(statement)
	if err != nil {
		log.Fatal("Error querying database for neighborhood_zips: ", err)
	}

	Data := []Neighborhood{}

	for rows.Next() {
		var zip string
		var neigh string
		err = rows.Scan(&zip, &neigh)
		if err != nil {
			log.Fatal("Scan error", err)
		}
		temp := Neighborhood{Zip: String2Int(zip), NeighborhoodName: neigh}

		Data = append(Data, temp)
	}

	defer rows.Close()

	return Data

}

func GetZipCode(userAgent string, lat, lon float64) int {
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

func CreateDataMartTable() {
	db, err := DMConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	dropTableStatement := "DROP TABLE IF EXISTS requirement_3_ccvi_alerts;"

	_, err = db.Exec(dropTableStatement)
	if err != nil {
		panic(err)
	}

	createTableStatement := `CREATE TABLE requirement_3_ccvi_alerts (
								TripID               TEXT PRIMARY KEY,
								TaxiID               TEXT,
								TripStartTimestamp   TIMESTAMPTZ,
								PickupLatitude       FLOAT,
								PickupLongitude      FLOAT,
								DropoffLatitude      FLOAT,
								DropoffLongitude     FLOAT,
								PickupZipCode		 INTEGER,
								DropoffZipCode       INTEGER,
								PickupNeighborhood	 TEXT,
								DropoffNeighborhood  TEXT,
								PickupCCVIscore		 FLOAT,
								PickupCCVIcategory	 TEXT,
								DropoffCCVIscore	 FLOAT,
								DropoffCCVIcategory  TEXT
							);`

	_, err = db.Exec(createTableStatement)
	if err != nil {
		panic(err)
	}
}

func LoadToDataMart(TaxisCCVI []TaxiTrips) {
	db, err := DMConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	insertStatement := `INSERT INTO requirement_3_ccvi_alerts (TripID, TaxiID, TripStartTimestamp, PickupLatitude, PickupLongitude, DropoffLatitude, DropoffLongitude, PickupZipCode, DropoffZipCode, PickupNeighborhood, DropoffNeighborhood, PickupCCVIscore, PickupCCVIcategory, DropoffCCVIscore, DropoffCCVIcategory) 
							values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
							ON CONFLICT (TripID) 
							DO NOTHING;`

	for _, v := range TaxisCCVI {
		_, err = db.Exec(insertStatement, v.TripID, v.TaxiID, v.TripStartTimestamp, v.PickupLatitude, v.PickupLongitude, v.DropoffLatitude, v.DropoffLongitude, v.PickupZipCode, v.DropoffZipCode, v.PickupNeighborhood, v.DropoffNeighborhood, v.PickupCCVIScore, v.PickupCCVICategory, v.DropoffCCVIScore, v.DropoffCCVICategory)
		if err != nil {
			log.Println("Error inserting record, TripID = ", v.TripID, err)
		}
	}
}

func TestInsertion() {
	db, err := DMConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	testStatement1 := "SELECT PickupNeighborhood FROM requirement_3_ccvi_alerts LIMIT 50"
	rows, err := db.Query(testStatement1)
	if err != nil {
		panic(err)
	}

	defer rows.Close()

	for rows.Next() {
		var testccvi string
		err = rows.Scan(&testccvi)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(testccvi)
	}
}

func main() {
	// Query CCVI records, parse for zip code records to match to taxi trips
	CCVIrecords = query_ccvi()
	parsedCCVIrecords := []CCVI{}

	for _, v := range CCVIrecords {
		if v.GeoType == "ZIP" {
			parsedCCVIrecords = append(parsedCCVIrecords, v)
		}
	}

	// Query taxi dataset
	Trips = query_taxis()

	// Query neighborhood-zip code mapping
	NeighborhoodMapping = query_neighborhoods()

	// For taxi trips, reverse geocode to get pickup and dropoff zip codes, link zip codes to relevant CCVI score, and update struct fields
	for i := 0; i < len(Trips); i++ {
		record := &Trips[i]
		// pickup zip code
		pickupzip := GetZipCode("msds432-final-group-4", record.PickupLatitude, record.PickupLongitude)

		record.PickupZipCode = pickupzip

		// dropoff zip code
		dropoffzip := GetZipCode("msds432-final-group-4", record.DropoffLatitude, record.DropoffLongitude)

		record.DropoffZipCode = dropoffzip

		for _, v := range parsedCCVIrecords {
			if v.CommunityAreaOrZip == record.PickupZipCode {
				record.PickupCCVIScore = v.CCVIScore
				record.PickupCCVICategory = v.CCVICategory
			}

			if v.CommunityAreaOrZip == record.DropoffZipCode {
				record.DropoffCCVIScore = v.CCVIScore
				record.DropoffCCVICategory = v.CCVICategory
			}
		}

		for _, v := range NeighborhoodMapping {
			if v.Zip == record.PickupZipCode {
				record.PickupNeighborhood = v.NeighborhoodName
			}

			if v.Zip == record.DropoffZipCode {
				record.DropoffNeighborhood = v.NeighborhoodName
			}
		}
	}

	CreateDataMartTable()

	LoadToDataMart(Trips)

	TestInsertion()
}
