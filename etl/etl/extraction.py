import os
import psycopg2
import zipfile


def execute():
    print("Extract!")

    stations_path = os.path.join("tmp", "extraction", "stations.csv")
    air_attributes_path = os.path.join("tmp", "extraction", "air_attributes.csv")
    measurements_path = os.path.join("tmp", "extraction", "measurements.csv")

    # connect to the DB
    conn = psycopg2.connect(
        dbname="air_quality",
        user="oltp",
        password="oltp",
        host="localhost",
        port=5432,
    )


    with conn.cursor() as cursor:
        cursor.execute("select version()")
        print(cursor.fetchone())
        print("connected")

        # extract the list of stations
        with open(stations_path, "w+") as stations_file:
            cursor.execute("SELECT ID, NAME FROM STATIONS")
            stations = dict(cursor.fetchall())
                        
            for s_k, s_v in stations.items():
                stations_file.write(f"{s_k},{s_v}\n")
                

        # extract the list of air attributes
        with open(air_attributes_path, "w+") as air_attributes_file:
            cursor.execute("SELECT ID, NAME FROM AIR_ATTRIBUTES")
            air_attributes = dict(cursor.fetchall())
                        
            for a_k, a_v in air_attributes.items():
                air_attributes_file.write(f"{a_k},{a_v}\n")

        # extract all the measurements
        with open(measurements_path, "w+") as measurements_file:
            cursor.execute("SELECT MEASURED_ON, STATION, AIR_ATTRIBUTE, VALUE FROM MEASUREMENTS ORDER BY MEASURED_ON ASC")
            measurements = list(cursor.fetchall())

            for m in measurements:
                measurements_file.write(f"{m[0]},{m[1]},{m[2]},{m[3]}\n")

        # zip all the results for later processing
        with zipfile.ZipFile(os.path.join("tmp","extracted_air_quality.zip"), mode="w") as extraction_file:
            extraction_file.write(stations_path, arcname="stations.csv")
            extraction_file.write(air_attributes_path, arcname="air_attributes.csv")
            extraction_file.write(measurements_path, arcname="measurements.csv")