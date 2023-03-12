package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/joho/godotenv/autoload"
	_ "github.com/lib/pq"
)

type Req5 struct {
	COMMUNITYAREA     int
	COMMUNITYAREANAME string
	BELOWPOVERTY      float64
	PERCAPITAINCOME   int
	UNEMPLOYMENT      float64
}

var Require5 []Req5

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

	statement := `select * from community_health;`

	rows, err := db.Query(statement)
	if err != nil {
		log.Fatal("Error querying database: ", err)
	}

	Data := []Req5{}

	for rows.Next() {
		var communityarea int
		var communityareaname string
		var belowpoverty float64
		var percapitaincome int
		var unemployment float64
		err = rows.Scan(&communityarea, &communityareaname, &belowpoverty, &percapitaincome, &unemployment)
		if err != nil {
			log.Fatal("Scan error", err)
		}
		temp := Req5{COMMUNITYAREA: communityarea, COMMUNITYAREANAME: communityareaname, BELOWPOVERTY: belowpoverty, PERCAPITAINCOME: percapitaincome, UNEMPLOYMENT: unemployment}

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

	dropProdTable := "DROP TABLE IF EXISTS requirement_5_commhealthstats;"

	_, err = db.Exec(dropProdTable)
	if err != nil {
		panic(err)
	}

	createProdTable := `CREATE TABLE requirement_5_commhealthstats (
															COMMUNITYAREA INT PRIMARY KEY,
															COMMUNITYAREANAME text,
															BELOWPOVERTY FLOAT,
															PERCAPITAINCOME INT,
															UNEMPLOYMENT FLOAT);`

	_, err = db.Exec(createProdTable)
	if err != nil {
		panic(err)
	}

	insertProdTable := `insert into requirement_5_commhealthstats (communityarea, communityareaname, belowpoverty, percapitaincome, unemployment)
								values ($1, $2, $3, $4, $5);`

	for _, v := range rq5fromquery {
		_, err = db.Exec(insertProdTable, v.COMMUNITYAREA, v.COMMUNITYAREANAME, v.BELOWPOVERTY, v.PERCAPITAINCOME, v.UNEMPLOYMENT)
		if err != nil {
			log.Println("Error inserting record, ID = ", v.COMMUNITYAREA, err)
		}
	}
}

func main() {
	Require5 = query_req5()

	DMProdTable5(Require5)
}
