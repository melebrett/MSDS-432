package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/joho/godotenv/autoload"
)

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

func DMTempTable(commList []Communities) {
	//function drops temp table if exists
	//then creates temp table
	//and inserts data into temp table for use in prod table
	db, err := DMConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	dropTableStatementLake := "DROP TABLE IF EXISTS comm_zips_temp;"

	_, err = db.Exec(dropTableStatementLake)
	if err != nil {
		panic(err)
	}

	createTableStatementLake := `CREATE TABLE comm_zips_temp (
								ZIPCODE         TEXT,
								LATITUDE        FLOAT,
								LONGITUDE   	FLOAT,
								AREANUM       	INT,
								COMMUNITY      	TEXT
							);`

	_, err = db.Exec(createTableStatementLake)
	if err != nil {
		panic(err)
	}

	insertTempTable := `INSERT INTO comm_zips_temp (Zipcode, Latitude, Longitude, AREANUM, COMMUNITY)
										values ($1, $2, $3, $4, $5)`

	for _, v := range commList {
		_, err = db.Exec(insertTempTable, v.ASSIGNED_ZIP, v.LATITUDE, v.LONGITUDE, v.AREANUM, v.COMMUNITY)
		if err != nil {
			log.Println("Error inserting record, AREANUM = ", v.AREANUM, err)
		}
	}
}

func DMProdTable(aggCommList []AggCommunities) {
	//function drops prod table if exists
	//then recreates prod table with necessary fields for mapping
	//inserts the aggCommList which is a grouped query from the temp table
	//and finally drops the temp table
	db, err := DMConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	dropProdTable := "DROP TABLE IF EXISTS comm_zips;"

	_, err = db.Exec(dropProdTable)
	if err != nil {
		panic(err)
	}

	createProdTable := `CREATE TABLE comm_zips (
												ZIPCODE        TEXT,
												AREANUM        INT,
												COMMUNITY      TEXT);`

	_, err = db.Exec(createProdTable)
	if err != nil {
		panic(err)
	}

	insertProdTable := `insert into comm_zips (zipcode, areanum, community)
								values ($2, $3, $1);`

	for _, v := range aggCommList {
		_, err = db.Exec(insertProdTable, v.ZIPCODE, v.AREANUM, v.COMMUNITY)
		if err != nil {
			log.Println("Error inserting record, AREANUM = ", v.AREANUM, err)
		}
	}

	dropTempTable := "DROP TABLE IF EXISTS comm_zips_temp;"

	_, err = db.Exec(dropTempTable)
	if err != nil {
		panic(err)
	}
}
