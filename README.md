## command to run producer
`docker exec -it spark-master bash
pip install kafka-python requests
python /opt/spark-apps/producer.py
`

## Copy consumer to docker container

`docker cp consumer/consumer.py spark-master:/opt/spark-apps/consumer.py`

## To run consumer python file

`docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/opt/spark/ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /opt/spark-apps/consumer.py`

## spark submit command

`docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/opt/spark/ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /opt/spark-apps/consumer.py
`



cd $SPARK_HOME/jars

wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/2.4.0/delta-spark_2.12-2.4.0.jar
wget https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar

'ride_id', 'rideable_type', 'started_at', 'ended_at',
'start_station_name', 'start_station_id', 'end_station_name',
'end_station_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng',
'member_casual']
