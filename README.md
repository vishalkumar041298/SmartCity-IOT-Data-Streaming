# Data Streaming from IOT devices Using Kafka and Spark

Gathers all data from IOT devices that sent in kafka and Spark reads the stream and stream it to S3.


## Tech Stack

* Kafka
* SPARK STREAMING
* AWS GLUE
* AWS REDSHIFT
* Docker

Architecture is shown in sysarch.png


## important commands

### To stream the data to Kafka
``` python jobs/main.py ```


##### Update AWS credentials in config.py

## To read data from kafka stream and push to S3

``` 
docker exec -it smartcity-spark-master-1 spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469 jobs/spark_city.py 
```

### Post Uploading 
Create crawler un `AWS Glue` to crawl s3 that creates data catalog in AWS glue

You can use `redshift` to run complex queries in the data.








# Notes

--num-executors - number of executores (which is just JVM container inside node) to be created across all nodes
--executor-cores - number tasks thats can run in parallel (sometimes even we give give 6 it may run 3 at parallel other 3 as idle to wait to complete)
--executor-memory - (JVM memory)
--driver-memory - (driver program memory when we have collect or take command all data ccomes to node where driver program exist so it needs memory to handle that data. leads to failure if not correct value)



DRA -> dynamic resource allocate (But sometimes over utilise not recommended)

SPARK LENS - to monitor this


Example - 

6 Node cluster(1 master and 5 worker)

each node 15 cores, 64 gbRAM
cores - no of concurrent task in one node


--executor-cores - 5

--num-executors = cores/executor cores = 15/--executor-cores = 3 executor per node

---  each executor 5 tasks


--executor-memory -  node RAM/--num-executors minus 1 or 2 gb for yarn memory = 18(maximum) (recommnened 5- 18 gb memory)


--driver-memory - Spark context gets created here entry point
    -- recommended  executor memory=driver memory
