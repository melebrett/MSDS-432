package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	_ "github.com/joho/godotenv/autoload"

	//"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"
)

type Communities struct {
	AREANUM      int     `json:"areanum"`
	COMMUNITY    string  `json:"community"`
	LATITUDE     float64 `json:"latitude"`
	LONGITUDE    float64 `json:"longitude"`
	SHAPEAREA    string  `json:"shapearea"`
	SHAPELEN     string  `json:"shapelen"`
	ASSIGNED_ZIP string  `json:"assigned_zip"`
}

type AggCommunities struct {
	AREANUM   int    `json:"areanum"`
	COMMUNITY string `json:"community"`
	ZIPCODE   string `json:"assigned_zip"`
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

var Comms []Communities
var CommZips []Communities
var AggCommZips []AggCommunities

func query_comms() []Communities {
	db, err := DLConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	statement := `select areanum, community, longitude, latitude, shapearea, shapelen from(
					SELECT areanum, community, LATITUDE as LONGITUDE, LONGITUDE as LATITUDE, SHAPELEN, SHAPEAREA, row_number() over (partition by community order by random()) id 
					FROM community_boundaries n)x
					where id <= 20`

	rows, err := db.Query(statement)
	if err != nil {
		log.Fatal("Error querying database: ", err)
	}

	Data := []Communities{}

	for rows.Next() {
		var areanum int
		var community string
		var latitude float64
		var longitude float64
		var shapearea string
		var shapelen string
		err = rows.Scan(&areanum, &community, &latitude, &longitude, &shapearea, &shapelen)
		if err != nil {
			log.Fatal("Scan error", err)
		}
		temp := Communities{AREANUM: areanum, COMMUNITY: community, LATITUDE: latitude, LONGITUDE: longitude, SHAPEAREA: shapearea, SHAPELEN: shapelen}

		Data = append(Data, temp)
	}

	defer rows.Close()

	return Data
}

func query_aggneighs() []AggCommunities {
	db, err := DMConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	statement := `select zipcode, areanum, community from (
					select zipcode, areanum, community
					from (select zipcode, areanum, community,
							row_number() over (partition by areanum, community order by numzips desc) rankno
					from (
						select zipcode, areanum, community, count(*)numzips
						from comm_zips_temp
						group by zipcode, areanum, community)z)y
					where rankno = 1)x;`

	rows, err := db.Query(statement)
	if err != nil {
		log.Fatal("Error querying database: ", err)
	}

	Data := []AggCommunities{}

	for rows.Next() {
		var areanum int
		var community string
		var zipcode string
		err = rows.Scan(&areanum, &community, &zipcode)
		if err != nil {
			log.Fatal("Scan error", err)
		}
		temp := AggCommunities{AREANUM: areanum, COMMUNITY: community, ZIPCODE: zipcode}

		Data = append(Data, temp)
	}

	defer rows.Close()

	return Data
}

func GetZipCode(userAgent string, lat, lon float64) string {
	var myresults Nominatim
	url := fmt.Sprintf("https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat=%f&lon=%f", lon, lat)
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

func main() {
	Comms = query_comms()
	CommZips = append(CommZips, Comms...)

	for i := 0; i < len(CommZips); i++ {
		record := &CommZips[i]
		zip := GetZipCode("msds432-final-group-4", record.LATITUDE, record.LONGITUDE)

		record.ASSIGNED_ZIP = zip
	}

	DMTempTable(CommZips)

	AggCommZips = query_aggneighs()
	DMProdTable(AggCommZips)

}
