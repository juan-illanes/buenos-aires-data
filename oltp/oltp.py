#!/usr/bin/env python3

import psycopg2
import csv
from datetime import datetime
import psycopg2.extras as extras

def parse_header(header):
    return header.split("_", maxsplit=1)

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

    # create the schema
    cursor.execute(open("air_quality.sql", "r").read())

    with open("calidad-aire.csv", "r", encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter = ',')

        # get all the different stations and air attributes being monitored
        stations = set()
        air_attributes = set()
        for header in reader.fieldnames:
            if header in ("FECHA", "HORA"):
                continue
            air_attribute, station = parse_header(header)
            stations.add(station)
            air_attributes.add(air_attribute)

        # populate the stations and air_attributes tables
        air_attributes_ids = {}
        for air_attribute in air_attributes:
            cursor.execute("INSERT INTO AIR_ATTRIBUTES (NAME) VALUES (%s) RETURNING ID", (air_attribute, ))
            air_attributes_ids[air_attribute] = cursor.fetchone()

        stations_ids = {}
        for station in stations:
            cursor.execute("INSERT INTO STATIONS (NAME) VALUES (%s) RETURNING ID", (station, ))
            stations_ids[station] = cursor.fetchone()

        x=0
        bulk_size = 10000
        bulk_inserts = []
        for line in reader:
            
            x += 1
            if x % 10_000 == 0:
                print(str(x) + " lines processed")

            try:
                raw_hour = int(line["HORA"])
                adjusted_hour = 0 if raw_hour == 24 else raw_hour # in some entries HORA is 24, which probably means 0
                timestamp = datetime.strptime(line["FECHA"] ,"%d%b%Y:%H:%M:%S").replace(hour=adjusted_hour)
                
                line.pop("FECHA")
                line.pop("HORA")
               
                for key, val in line.items():
                    try:
                        air_attribute, station = parse_header(key)
                        #print(val)
                        measured_value = float(val)
                        
                        # cursor.execute("""
                        #         INSERT INTO MEASUREMENTS (MEASURED_ON, STATION, AIR_ATTRIBUTE, VALUE) VALUES 
                        #         (%s, %s, %s, %s)
                        #         """, 
                        #     (timestamp, stations_ids[station], air_attributes_ids[air_attribute], measured_value)
                        #)

                        if len(bulk_inserts) == bulk_size:
                            extras.execute_batch(cursor, """
                                        INSERT INTO MEASUREMENTS (MEASURED_ON, STATION, AIR_ATTRIBUTE, VALUE) VALUES 
                                        (%s, %s, %s, %s)
                                        """, 
                                    bulk_inserts
                            )
                            bulk_inserts = []
                        else:
                            bulk_inserts.append((timestamp, stations_ids[station], air_attributes_ids[air_attribute], measured_value))

                    except ValueError:
                        #print(f"Invalid or null measurement detected: {val}")
                        continue
                
            except ValueError:
                #print("Invalid date detected")
                continue
        
        # insert remaining measurements
        if len(bulk_inserts) > 0:
            print(str(x + len(bulk_inserts)) + " lines processed")
            extras.execute_batch(cursor, """
                        INSERT INTO MEASUREMENTS (MEASURED_ON, STATION, AIR_ATTRIBUTE, VALUE) VALUES 
                        (%s, %s, %s, %s)
                        """, 
                    bulk_inserts
            )
