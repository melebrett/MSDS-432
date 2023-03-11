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

type Neighborhoods struct {
	LATITUDE     float64 `json:"latitude"`
	LONGITUDE    float64 `json:"longitude"`
	PRI_NEIGH    string  `json:"pri_neigh"`
	SEC_NEIGH    string  `json:"sec_neigh"`
	SHAPE_AREA   string  `json:"shape_area"`
	SHAPE_LEN    string  `json:"shape_len"`
	ASSIGNED_ZIP string  `json:"assigned_zip"`
}

type AggNeighborhoods struct {
	ZIPCODE   string `json:"zipcode"`
	PRI_NEIGH string `json:"pri_neigh"`
	SEC_NEIGH string `json:"sec_neigh"`
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

var Hoods []Neighborhoods
var HoodZips []Neighborhoods
var AggHoodZips []AggNeighborhoods

func query_neighs() []Neighborhoods {
	db, err := DLConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	statement := `select longitude, latitude, pri_neigh, sec_neigh, shape_area, shape_len from(
					SELECT LATITUDE as LONGITUDE, LONGITUDE as LATITUDE, PRI_NEIGH, SEC_NEIGH, SHAPE_AREA, SHAPE_LEN, row_number() over (partition by sec_neigh order by random()) id 
					FROM neighborhoods n)x
					where id <= 20`

	rows, err := db.Query(statement)
	if err != nil {
		log.Fatal("Error querying database: ", err)
	}

	Data := []Neighborhoods{}

	for rows.Next() {
		var latitude float64
		var longitude float64
		var pri_neigh string
		var sec_neigh string
		var shape_area string
		var shape_len string
		err = rows.Scan(&latitude, &longitude, &pri_neigh, &sec_neigh, &shape_area, &shape_len)
		if err != nil {
			log.Fatal("Scan error", err)
		}
		temp := Neighborhoods{LATITUDE: latitude, LONGITUDE: longitude, PRI_NEIGH: pri_neigh, SEC_NEIGH: sec_neigh, SHAPE_AREA: shape_area, SHAPE_LEN: shape_len}

		Data = append(Data, temp)
	}

	defer rows.Close()

	return Data
}

func query_aggneighs() []AggNeighborhoods {
	db, err := DMConnect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	statement := `select zipcode, pri_neigh, sec_neigh from (
					select zipcode, pri_neigh, sec_neigh
					from (select zipcode, pri_neigh, sec_neigh,
							rank() over (partition by pri_neigh, sec_neigh order by numzips desc) rankno
					from (
						select zipcode, pri_neigh, sec_neigh, count(*)numzips
						from neighborhood_zips_temp
						group by zipcode, pri_neigh, sec_neigh)z)y
					where rankno = 1)x
					fetch first row only;`

	rows, err := db.Query(statement)
	if err != nil {
		log.Fatal("Error querying database: ", err)
	}

	Data := []AggNeighborhoods{}

	for rows.Next() {
		var zipcode string
		var pri_neigh string
		var sec_neigh string
		err = rows.Scan(&zipcode, &pri_neigh, &sec_neigh)
		if err != nil {
			log.Fatal("Scan error", err)
		}
		temp := AggNeighborhoods{ZIPCODE: zipcode, PRI_NEIGH: pri_neigh, SEC_NEIGH: sec_neigh}

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
	Hoods = query_neighs()
	HoodZips = append(HoodZips, Hoods...)

	for i := 0; i < len(HoodZips); i++ {
		record := &HoodZips[i]
		zip := GetZipCode("msds432-final-group-4", record.LATITUDE, record.LONGITUDE)

		record.ASSIGNED_ZIP = zip
	}

	DMTempTable(HoodZips)

	AggHoodZips = query_aggneighs()
	DMProdTable(AggHoodZips)

}
