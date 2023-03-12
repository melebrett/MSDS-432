package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/joho/godotenv/autoload"
	_ "github.com/lib/pq"
)

type Req5 struct {
	ID               int
	PERMIT_TYPE      string
	REVIEW_TYPE      string
	PROCESSING_TIME  string
	WORK_DESCRIPTION string
	TOTAL_FEE        string
	REPORTED_COST    string
	COMMUNITY_AREA   string
	CONTACT_ZIP      string
}

type Req9 struct {
	TRIPID               string
	TRIPMILES            string
	TRIPTOTAL            string
	PICKUPCOMMUNITYAREA  string
	DROPOFFCOMMUNITYAREA string
	TRIPSTARTTIMESTAMP   time.Time
}

var Require5 []Req5
var Require9 []Req9

// data lake connection
func DLConnect() (*sql.DB, error) {
	//Retreiving DB connection credential environment variables
	err := godotenv.Load("dbconn.env")
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
	err := godotenv.Load("dbconn.env")
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

func query_req5() []Req5 {
	db, err := DLConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	statement := `select id, permit_type, review_type, processing_time, work_description, total_fee, reported_cost, community_area, contact_zip 
					from building_permits where coalesce(contact_zip,'0') != '0' or community_area is not null limit 5000;`

	rows, err := db.Query(statement)
	if err != nil {
		log.Fatal("Error querying database: ", err)
	}

	Data := []Req5{}

	for rows.Next() {
		var id int
		var permit_type string
		var review_type string
		var processing_time string
		var work_description string
		var total_fee string
		var reported_cost string
		var community_area string
		var contact_zip string
		err = rows.Scan(&id, &permit_type, &review_type, &processing_time, &work_description, &total_fee, &reported_cost, &community_area, &contact_zip)
		if err != nil {
			log.Fatal("Scan error", err)
		}
		temp := Req5{ID: id, PERMIT_TYPE: permit_type, REVIEW_TYPE: review_type, PROCESSING_TIME: processing_time, WORK_DESCRIPTION: work_description, TOTAL_FEE: total_fee, REPORTED_COST: reported_cost, COMMUNITY_AREA: community_area, CONTACT_ZIP: contact_zip}

		Data = append(Data, temp)
	}

	defer rows.Close()

	return Data
}

func DMProdTable5(rq5fromquery []Req5) {
	//function drops prod table if exists
	//then recreates prod table with necessary fields for mapping
	//inserts the aggCommList which is a grouped query from the temp table
	//and finally drops the temp table
	db, err := DMConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	dropProdTable := "DROP TABLE IF EXISTS requirement_5_buildingpermitfeewaiver;"

	_, err = db.Exec(dropProdTable)
	if err != nil {
		panic(err)
	}

	createProdTable := `CREATE TABLE requirement_5_buildingpermitfeewaiver (
															ID TEXT PRIMARY KEY,
															PERMIT_TYPE text,
															REVIEW_TYPE text,
															PROCESSING_TIME text,
															WORK_DESCRIPTION text,
															TOTAL_FEE TEXT,
															REPORTED_COST TEXT,
															COMMUNITY_AREA text,
															CONTACT_ZIP text);`

	_, err = db.Exec(createProdTable)
	if err != nil {
		panic(err)
	}

	insertProdTable := `insert into requirement_5_buildingpermitfeewaiver (id, permit_type, review_type, processing_time, work_description, total_fee, reported_cost, community_area, contact_zip )
								values ($1, $2, $3, $4, $5, $6, $7, $8, $9);`

	for _, v := range rq5fromquery {
		_, err = db.Exec(insertProdTable, v.ID, v.PERMIT_TYPE, v.REVIEW_TYPE, v.PROCESSING_TIME, v.WORK_DESCRIPTION, v.TOTAL_FEE, v.REPORTED_COST, v.COMMUNITY_AREA, v.CONTACT_ZIP)
		if err != nil {
			log.Println("Error inserting record, ID = ", v.ID, err)
		}
	}
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

func query_req9() []Req9 {
	db, err := DLConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	statement := `select tripid, tripmiles, triptotal, pickupcommunityarea, dropoffcommunityarea, tripstarttimestamp
					from taxi_trips where pickupcommunityarea is not null or dropoffcommunityarea is not null limit 5000;`

	rows, err := db.Query(statement)
	if err != nil {
		log.Fatal("Error querying database: ", err)
	}

	Data := []Req9{}

	for rows.Next() {
		var tripid string
		var tripmiles string
		var triptotal string
		var pickupcommunityarea string
		var dropoffcommunityarea string
		var tripstarttimestamp string
		err = rows.Scan(&tripid, &tripmiles, &triptotal, &pickupcommunityarea, &dropoffcommunityarea, &tripstarttimestamp)
		if err != nil {
			log.Fatal("Scan error", err)
		}
		temp := Req9{TRIPID: tripid, TRIPMILES: tripmiles, TRIPTOTAL: triptotal, PICKUPCOMMUNITYAREA: pickupcommunityarea, DROPOFFCOMMUNITYAREA: dropoffcommunityarea, TRIPSTARTTIMESTAMP: String2Timestamp(tripstarttimestamp)}

		Data = append(Data, temp)
	}

	defer rows.Close()

	return Data
}

func DMProdTable9(rq9fromquery []Req9) {
	//function drops prod table if exists
	//then recreates prod table with necessary fields for mapping
	//inserts the aggCommList which is a grouped query from the temp table
	//and finally drops the temp table
	db, err := DMConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	dropProdTable := "DROP TABLE IF EXISTS requirement_9_txneighzip;"

	_, err = db.Exec(dropProdTable)
	if err != nil {
		panic(err)
	}

	createProdTable := `CREATE TABLE requirement_9_txneighzip (
														TRIPID TEXT PRIMARY KEY,
														TRIPMILES TEXT,
														TRIPTOTAL TEXT,
														PICKUPCOMMUNITYAREA TEXT,
														DROPOFFCOMMUNITYAREA TEXT,
														TRIPSTARTTIMESTAMP TIMESTAMPZ);`

	_, err = db.Exec(createProdTable)
	if err != nil {
		panic(err)
	}

	insertProdTable := `insert into requirement_9_txneighzip (tripid, tripmiles, triptotal, pickupcommunityarea, dropoffcommunityarea, tripstarttimestamp )
								values ($1, $2, $3, $4, $5, $6);`

	for _, v := range rq9fromquery {
		_, err = db.Exec(insertProdTable, v.TRIPID, v.TRIPMILES, v.TRIPTOTAL, v.PICKUPCOMMUNITYAREA, v.DROPOFFCOMMUNITYAREA, v.TRIPSTARTTIMESTAMP)
		if err != nil {
			log.Println("Error inserting record, ID = ", v.TRIPID, err)
		}
	}

}

func main() {
	Require5 = query_req5()
	Require9 = query_req9()

	DMProdTable5(Require5)
	DMProdTable9(Require9)
}
