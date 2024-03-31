from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import spark_schema
import config


def main():
    spark = SparkSession.builder.appName('SmartCityStreaming')\
        .config('spark.jars.packages',
                'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,'
                'org.apache.hadoop:hadoop-aws:3.3.1,'
                'com.amazonaws:aws-java-sdk:1.11.469')\
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')\
        .config('spark.hadoop.fs.s3a.access.key', config.aws_config.get('access_key'))\
        .config('spark.hadoop.fs.s3a.secret.key', config.aws_config.get('secret_key'))\
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

    vehicleDF = read_kafka_topic(config.VEHICLE_TOPIC, spark_schema.vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic(config.GPS_TOPIC, spark_schema.gpsSchema).alias('gps')
    trafficCameraDF = read_kafka_topic(config.TRAFFIC_CAMERA_TOPIC, spark_schema.trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic(config.WEATHER_TOPIC, spark_schema.weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic(config.EMERGENCY_TOPIC, spark_schema.emergencySchema).alias('emergency')

    # join all the dfs with id and timestamp
    # print(vehicleDF.count())
    query1 = stream_writer_to_s3(
        vehicleDF, 
        f'{config.aws_config.get("s3_path")}/checkpoints/{config.VEHICLE_TOPIC}',
        f'{config.aws_config.get("s3_path")}/data/{config.VEHICLE_TOPIC}'
    )
    query2 = stream_writer_to_s3(
        gpsDF,
        f'{config.aws_config.get("s3_path")}/checkpoints/{config.GPS_TOPIC}',
        f'{config.aws_config.get("s3_path")}/data/{config.GPS_TOPIC}'
    )
    query3 = stream_writer_to_s3(
        trafficCameraDF,
        f'{config.aws_config.get("s3_path")}/checkpoints/{config.TRAFFIC_CAMERA_TOPIC}',
        f'{config.aws_config.get("s3_path")}/data/{config.TRAFFIC_CAMERA_TOPIC}'
    )
    query4 = stream_writer_to_s3(
        weatherDF,
        f'{config.aws_config.get("s3_path")}/checkpoints/{config.WEATHER_TOPIC}',
        f'{config.aws_config.get("s3_path")}/data/{config.WEATHER_TOPIC}'
    )
    query5 = stream_writer_to_s3(
        emergencyDF,
        f'{config.aws_config.get("s3_path")}/checkpoints/{config.EMERGENCY_TOPIC}',
        f'{config.aws_config.get("s3_path")}/data/{config.EMERGENCY_TOPIC}'
    )

    query5.awaitTermination()

if __name__ == '__main__':
    main()