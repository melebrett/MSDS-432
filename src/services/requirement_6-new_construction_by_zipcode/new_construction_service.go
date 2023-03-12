package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

type BuildingPermit struct {
	ID            string
	PermitNum     int
	PermitType    string
	Latitude      float64
	Longitude     float64
	PermitZipCode int
}

type Community struct {
	CommunityArea     int
	CommunityAreaName string
	BelowPoverty      float64
	PerCapitaIncome   int
	Unemployment      float64
	CommunityZipCode  int
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
	Postcode      int    `json:"postcode,string"`
}

type CommZip struct {
	ZipCode      int
	CommunityNum int
}

var Permits []BuildingPermit
var CommunityData []Community
var CommunityMapping []CommZip

func DLConnect() (*sql.DB, error) {
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

	HOST := mustGetenv("DLHOST")
	PORT := mustGetenv("DLDBPORT")
	USER := mustGetenv("DLUSER")
	PASSWORD := mustGetenv("DLPASSWORD")
	DBNAME := mustGetenv("DLDBNAME")

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

func DMConnect() (*sql.DB, error) {
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

	HOST := mustGetenv("DMHOST")
	PORT := mustGetenv("DMDBPORT")
	USER := mustGetenv("DMUSER")
	PASSWORD := mustGetenv("DMPASSWORD")
	DBNAME := mustGetenv("DMDBNAME")

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

func String2Float(s string) float64 {
	value, _ := strconv.ParseFloat(s, 64)
	return value
}

func String2Int(s string) int {
	value, _ := strconv.Atoi(s)
	return value
}

func queryBuildingPermits() []BuildingPermit {
	db, err := DLConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	statement := `SELECT id, permit_num, permit_type, latitude, longitude FROM building_permits LIMIT 10000`

	rows, err := db.Query(statement)
	if err != nil {
		log.Fatal("Error querying database for building permits: ", err)
	}

	Data := []BuildingPermit{}

	for rows.Next() {
		var id string
		var permitnum string
		var permittype string
		var latitude string
		var longitude string
		err = rows.Scan(&id, &permitnum, &permittype, &latitude, &longitude)
		if err != nil {
			log.Fatal("Scan error", err)
		}
		temp := BuildingPermit{ID: id, PermitNum: String2Int(permitnum), PermitType: permittype, Latitude: String2Float(latitude), Longitude: String2Float(longitude)}

		Data = append(Data, temp)
	}

	defer rows.Close()

	return Data
}

func queryCommunityHealthUnder30kPCI() []Community {
	db, err := DLConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	statement := `SELECT communityarea, communityareaname, belowpoverty, percapitaincome, unemployment FROM community_health WHERE percapitaincome::INT < 30000`

	rows, err := db.Query(statement)
	if err != nil {
		log.Fatal("Error querying database for community health: ", err)
	}

	Data := []Community{}

	for rows.Next() {
		var communityarea string
		var communityareaname string
		var belowpoverty string
		var percapitaincome string
		var unemployment string
		err = rows.Scan(&communityarea, &communityareaname, &belowpoverty, &percapitaincome, &unemployment)
		if err != nil {
			log.Fatal("Scan error", err)
		}
		temp := Community{CommunityArea: String2Int(communityarea), CommunityAreaName: communityareaname, BelowPoverty: String2Float(belowpoverty), PerCapitaIncome: String2Int(percapitaincome), Unemployment: String2Float(unemployment)}

		Data = append(Data, temp)
	}

	defer rows.Close()

	return Data
}

func queryCommunityMapping() []CommZip {
	db, err := DMConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	statement := `SELECT areanum AS zipcode, community AS community_num FROM comm_zips`

	rows, err := db.Query(statement)
	if err != nil {
		log.Fatal("Error querying database for community-zip code mapping: ", err)
	}

	Data := []CommZip{}

	for rows.Next() {
		var zipcode int
		var communitynum string
		err = rows.Scan(&zipcode, &communitynum)
		if err != nil {
			log.Fatal("Scan error", err)
		}
		temp := CommZip{ZipCode: zipcode, CommunityNum: String2Int(communitynum)}

		Data = append(Data, temp)
	}

	defer rows.Close()

	return Data
}

func NewBuildPermits(AllPermits []BuildingPermit) []BuildingPermit {
	var NewBuilds []BuildingPermit
	for _, v := range AllPermits {
		if v.PermitType == "PERMIT - NEW CONSTRUCTION" {
			NewBuilds = append(NewBuilds, v)
		}
	}
	return NewBuilds
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

	dropTableStatement := "DROP TABLE IF EXISTS requirement_6_new_construction;"

	_, err = db.Exec(dropTableStatement)
	if err != nil {
		panic(err)
	}

	createTableStatement := `CREATE TABLE requirement_6_new_construction (
								ID            TEXT PRIMARY KEY,
								PermitNum     INT,
								PermitType    TEXT,
								Latitude      FLOAT,
								Longitude     FLOAT,
								PermitZipCode INT
							);`

	_, err = db.Exec(createTableStatement)
	if err != nil {
		panic(err)
	}
}

func LoadToDataMart(Permits []BuildingPermit) {
	db, err := DMConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	insertStatement := `INSERT INTO requirement_6_new_construction (ID, PermitNum, PermitType, Latitude, Longitude, PermitZipCode) 
							values ($1, $2, $3, $4, $5, $6)
							ON CONFLICT (ID) 
							DO NOTHING;`

	for _, v := range Permits {
		_, err = db.Exec(insertStatement, v.ID, v.PermitNum, v.PermitType, v.Latitude, v.Longitude, v.PermitZipCode)
		if err != nil {
			log.Println("Error inserting record, ID = ", v.ID, err)
		}
	}
}

func TestInsertion() {
	db, err := DMConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	testStatement1 := "SELECT ID FROM requirement_6_new_construction LIMIT 10"
	rows, err := db.Query(testStatement1)
	if err != nil {
		panic(err)
	}

	defer rows.Close()

	for rows.Next() {
		var testpermit string
		err = rows.Scan(&testpermit)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(testpermit)
	}
}

func main() {
	Permits := queryBuildingPermits()

	NewBuildPermits := NewBuildPermits(Permits)

	CommunitiesLessThan30kPCI := queryCommunityHealthUnder30kPCI()

	CommunityZipCodes := queryCommunityMapping()

	// Get zip codes for new construction building permits
	for i := 0; i < len(NewBuildPermits); i++ {
		record := &NewBuildPermits[i]

		record.PermitZipCode = GetZipCode("msds432-final-group-4", record.Latitude, record.Longitude)
	}

	// Map Communities to ZipCodes for community health / unemployment data
	for i := 0; i < len(CommunitiesLessThan30kPCI); i++ {
		record := &CommunitiesLessThan30kPCI[i]

		for _, v := range CommunityZipCodes {
			if v.CommunityNum == record.CommunityArea {
				record.CommunityZipCode = v.ZipCode
			}
		}
	}

	// Filter for lowest new build permits among qualified zip codes
	var QualifiedBuildingPermits []BuildingPermit

	for _, v := range NewBuildPermits {
		for _, val := range CommunitiesLessThan30kPCI {
			if v.PermitZipCode == val.CommunityZipCode {
				QualifiedBuildingPermits = append(QualifiedBuildingPermits, v)
			}
		}
	}

	// Create table in DataMart
	CreateDataMartTable()

	// Ingest processed records to Data Mart
	LoadToDataMart(QualifiedBuildingPermits)

	// // Test successful ingestion
	// TestInsertion()
}
