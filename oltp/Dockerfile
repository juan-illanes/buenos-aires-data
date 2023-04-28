FROM postgres

LABEL author="Juan Illanes Gauto"
LABEL description="OLTP postgres instance"
LABEL version="1.0"

#RUN apt-get update && apt-get install -y python3 python3-pip

COPY db_init.sql /docker-entrypoint-initdb.d/

#COPY requirements.txt requirements.txt

#RUN pip install -r requirements.txt

#COPY air_quality.sql air_quality.sql
#COPY calidad-aire.csv calidad-aire.csv
#COPY oltp.py oltp.py

#ENTRYPOINT ["python3", "oltp.py"]