import os
import zipfile
from datetime import date, datetime

def execute():
    print("Transform!")

    # unpackage the extracted contents
    extraction_file_path = os.path.join("tmp","extracted_air_quality.zip")
    with zipfile.ZipFile(extraction_file_path, mode="r") as zip:
        zip.extractall(os.path.join("tmp", "transformation", "inputs"))

    # load stations
    stations = {}
    with open(os.path.join("tmp","transformation", "inputs", "stations.csv"), "r") as stations_file:
        for station_line in stations_file:
            s = station_line.strip().split(",")
            stations[s[0]] = s[1]
    print(stations)

    # load air_attributes
    air_attributes = {}
    with open(os.path.join("tmp","transformation", "inputs", "air_attributes.csv"), "r") as air_attributes_file:
        for air_attribute_line in air_attributes_file:
            s = air_attribute_line.strip().split(",")
            air_attributes[s[0]] = s[1]
    print(air_attributes)

    # process measurements and save them into a single csv file
    # here we apply the transformation logic (aggregations, calculations, scale and units corrections)
    # in this case we will just output a csv file that closely resembles the original input from Buenos Aires Data
    # since its non-normalized format makes it a good fit for OLAP storage

    with open(os.path.join("tmp","processed_air_quality.csv"), "w") as output_file:
        with open(os.path.join("tmp","transformation","inputs","measurements.csv"), "r") as input_file:

            for measurement_line in input_file:
                m = measurement_line.strip().split(",")
                output_file.write(f"{m[0]},{stations[m[1]]},{air_attributes[m[2]]},{m[3]}\n")
