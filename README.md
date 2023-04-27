# buenos-aires-data

## Requirements

Packages
 -> python3 and python3-pip
 -> docker

## Instructions

### OLTP
docker build of the custom postgres instance

docker run --name postgres -e POSTGRES_PASSWORD=secret -p 5432:5432 -d postgres

(virtual environment) 
...

### ETL

(virtual environment)
...

### OLAP