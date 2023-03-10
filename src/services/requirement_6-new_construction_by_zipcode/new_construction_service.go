package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

type BuildingPermit struct {
	ID         string
	PermitNum  int
	PermitType string
	Latitude   float64
	Longitude  float64
}

type Community struct {
	CommunityArea     int
	CommunityAreaName string
	BelowPoverty      float64
	PerCapitaIncome   int
	Unemployment      float64
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

var Permits []BuildingPermit
var CommunityData []Community

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

	statement := `SELECT id, permit_num, permit_type, latitude, longitude FROM building_permits`

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

func queryCommunityHealth() []Community {
	db, err := DLConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	statement := `SELECT communityarea, communityareaname, belowpoverty, percapitaincome, unemployment FROM community_health`

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

func NewBuildPermits(AllPermits []BuildingPermit) []BuildingPermit {
	var NewBuilds []BuildingPermit
	for _, v := range AllPermits {
		if v.PermitType == "PERMIT - NEW CONSTRUCTION" {
			NewBuilds = append(NewBuilds, v)
		}
	}
	return NewBuilds
}

func main() {
	Permits := queryBuildingPermits()

	NewBuildPermits := NewBuildPermits(Permits)

	CommunityHealth := queryCommunityHealth()

}
