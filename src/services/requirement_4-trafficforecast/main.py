import pandas as pd
import geopandas as gpd
import geopy
from urllib.parse import quote
from sqlalchemy import create_engine
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import os
from dotenv import load_dotenv

def main():

    load_dotenv()

    USER = os.getenv('USER')
    PWD = os.getenv('PASSWORD')
    LAKE = os.getenv('LAKE')
    MART = os.getenv('MART')
    PORT = os.getenv('PORT')
    DB = os.getenv('DBNAME')

    connection_string = f"postgresql+psycopg2://{USER}:%s@{LAKE}:{PORT}/{DB}"
    pglake = create_engine(connection_string % quote(PWD))

    df_raw = pd.read_sql("select * from taxi_trips", pglake)
    # pglake.close()

    df_raw = df_raw.loc[~((df_raw['pickupcentroidlatitude'] == "") | (df_raw["pickupcentroidlongitude"] == "") | (df_raw["dropoffcentroidlatitude"] == "") | (df_raw["dropoffcentroidlongitude"] == "" ))]
    df_raw = df_raw.loc[~((df_raw['tripseconds'] == "") | (df_raw["tripmiles"] == ""))]

    df_raw = df_raw.astype(
        {
        "pickupcentroidlatitude":"float64",
        "pickupcentroidlongitude":"float64",
        "dropoffcentroidlatitude":"float64",
        "dropoffcentroidlongitude":"float64"
        }
        )

    df_raw[["pickupcentroidlatitude", "pickupcentroidlongitude", "dropoffcentroidlatitude", "dropoffcentroidlongitude"]] = round(df_raw[["pickupcentroidlatitude", "pickupcentroidlongitude", "dropoffcentroidlatitude", "dropoffcentroidlongitude"]],3)

    # get zip codes from coordinates
    coords1 = df_raw.drop_duplicates(['pickupcentroidlatitude','pickupcentroidlongitude'])[["pickupcentroidlongitude", "pickupcentroidlatitude"]]
    coords2 = df_raw.drop_duplicates(['dropoffcentroidlatitude','dropoffcentroidlongitude'])[["dropoffcentroidlongitude", "dropoffcentroidlatitude"]]

    coords = pd.concat(
        [
        coords1.rename(columns={"pickupcentroidlatitude":"latitude","pickupcentroidlongitude":"longitude"}).reset_index(drop=True),
        coords2.rename(columns={ "dropoffcentroidlatitude":"latitude", "dropoffcentroidlongitude":"longitude"}).reset_index(drop=True)
        ],
        axis=0
    ).drop_duplicates()

    coords["coords"] = coords["latitude"].astype('str') + "," + coords["longitude"].astype('str')

    locator = Nominatim(user_agent='myGeocoder', timeout=10)
    rgeocode = RateLimiter(locator.reverse, min_delay_seconds=0.001)
    get_zipcode = lambda row: rgeocode((row['latitude'], row['longitude'])).raw['address']['postcode']

    coords['zip'] = coords.apply(get_zipcode, axis=1)

    df_raw['pickupcoords'] = df_raw["pickupcentroidlatitude"].astype('str') + "," + df_raw["pickupcentroidlongitude"].astype('str')
    df_raw['dropoffcoords'] = df_raw["dropoffcentroidlatitude"].astype('str') + "," + df_raw["dropoffcentroidlongitude"].astype('str')
    
    df_raw = df_raw.merge(
        coords.drop_duplicates(),
        right_on= "coords",
        left_on="pickupcoords",
        how = 'left'
    ).rename(columns={"zip":"pickupzip"}).merge(
        coords.drop_duplicates(),
        right_on= "coords",
        left_on="dropoffcoords",
        how = 'left'

    ).rename(columns={"zip":"dropoffzip"})

    df_raw['tripstarttimestamp'] = pd.to_datetime(df_raw['tripstarttimestamp'])
    df_raw['tripendtimestamp'] = pd.to_datetime(df_raw['tripendtimestamp'])
    df_raw = df_raw.astype(
        {'tripseconds':'float64'},
        {'tripmiles':'float64'}
    )

    # create new dataframe with all the columns we care about and real types
    df = df_raw[["tripid","taxiid","tripstarttimestamp","tripendtimestamp","tripseconds","tripmiles","dropoffzip","pickupzip","pickupcoords","dropoffcoords"]]
    df["tripdate"] = df["tripstarttimestamp"].dt.date
    df["tripweekday"] = df["tripstarttimestamp"].dt.weekday

    # aggregate trips to/from by zip code
    df_agg1 = df.groupby(["tripdate","pickupzip"]).size().reset_index(name="count")
    df_agg2 = df.groupby(["tripdate","dropoffzip"]).size().reset_index(name="count")
    df_agg = pd.concat([df_agg1.rename(columns={"pickupzip":"zip"}), df_agg2.rename(columns={"dropoffzip":"zip"})]).groupby(['tripdate','zip']).sum('count').reset_index()
    df_agg = df_agg.rename(columns={'count':'trips'})

    # forecast
    df_agg['pred_next_day'] = df_agg.groupby('zip')['trips'].transform(lambda x: x.rolling(window = 2, min_periods=1).median())
    df_agg['pred_next_week'] = df_agg.groupby('zip')['trips'].transform(lambda x: x.rolling(window = 7, min_periods=5).sum())
    df_agg['pred_next_week'] = df_agg['pred_next_week'].fillna(df_agg['pred_next_day']*0.90*7)

    # write to mart
    try:
        connection_string = f"postgresql+psycopg2://{USER}:%s@{MART}:{PORT}/{DB}"
        pgmart = create_engine(connection_string % quote(PWD))
        conn = pgmart.connect()

        df.to_sql("requirement_4_taxi_trips",schema="public", con = conn, if_exists="replace")
        df_agg.to_sql("requirement_4_taxi_trips_forecast",schema="public",con=conn, if_exists="replace")

        conn.close()
        print("success")

    except Exception as e:
        print(f"failed write to data mart: {e}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"failed with error: {e}")