from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import spark_schema
from config import aws_config


def main():
    spark = SparkSession.builder.appName('SmartCityStreaming')\
        .config('spark.jars.packages',
                'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,'
                'org.apache.hadoop:hadoop-aws:3.3.1,'
                'com.amazonaws:aws-java-sdk:1.11.469')\
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')\
        .config('spark.hadoop.fs.s3a.access.key', aws_config.get('access_key'))\
        .config('spark.hadoop.fs.s3a.secret.key', aws_config.get('secret_key'))\
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
        .getOrCreate()
     # Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel('WARN')

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'kafka:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value as STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes')
            )

    def stream_writer_to_s3(input, checkpoint_folder, output):
        return(input.writeStream
               .format('parquet')
               .option('checkpointLocation', checkpoint_folder)
               .option('path', output)
               .outputMode('append')
               .start()
            )

    vehicleDF = read_kafka_topic('vehicle_data', spark_schema.vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', spark_schema.gpsSchema).alias('gps')
    trafficCameraDF = read_kafka_topic('traffic_data', spark_schema.trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', spark_schema.weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', spark_schema.emergencySchema).alias('emergency')

    # join all the dfs with id and timestamp
    # print(vehicleDF.count())
    query1 = stream_writer_to_s3(
        vehicleDF, 
        's3a://spark-streaming-data-smartcity/checkpoints/vehicle_data',
        's3a://spark-streaming-data-smartcity/data/vehicle_data'
    )
    query2 = stream_writer_to_s3(
        gpsDF,
        's3a://spark-streaming-data-smartcity/checkpoints/gps_data',
        's3a://spark-streaming-data-smartcity/data/gps_data'
    )
    query3 = stream_writer_to_s3(
        trafficCameraDF,
        's3a//spark-streaming-data-smartcity/checkpoints/traffic_data',
        's3a//spark-streaming-data-smartcity/data/traffic_data'
    )
    query4 = stream_writer_to_s3(
        weatherDF,
        's3a//spark-streaming-data-smartcity/checkpoints/weather_data',
        's3a//spark-streaming-data-smartcity/data/weather_data'
    )
    query5 = stream_writer_to_s3(
        emergencyDF,
        's3a//spark-streaming-data-smartcity/checkpoints/emergency_data',
        's3a//spark-streaming-data-smartcity/data/emergency_data'
    )

    query5.awaitTermination()

if __name__ == '__main__':
    main()