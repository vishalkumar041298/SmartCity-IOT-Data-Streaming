from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, DoubleType
)


vehicleSchema = StructType([
        StructField('id', StringType(), False),
        StructField('vehicle_id', StringType(), False),
        StructField('timestamp', TimestampType(), False),
        StructField('location', StringType(), False),
        StructField('speed', DoubleType(), False),
        StructField('direction', StringType(), False),
        StructField('make', StringType(), False),
        StructField('model', StringType(), False),
        StructField('year', IntegerType(), False),
        StructField('fuel_type', StringType(), False)

    ])


gpsSchema = StructType([
    StructField('id', StringType(), False),
    StructField('vehicle_id', StringType(), False),
    StructField('timestamp', TimestampType(), False),
    StructField('speed', DoubleType(), False),
    StructField('direction', StringType(), False),
    StructField('vehicle_type', StringType(), False)
])


trafficSchema = StructType([
    StructField('id', StringType(), False),
    StructField('vehicle_id', StringType(), False),
    StructField('camera_id', StringType(), False),
    StructField('location', StringType(), False),
    StructField('timestamp', TimestampType(), False),
    StructField('snapshot', StringType(), False)
])


weatherSchema = StructType([
    StructField('id', StringType(), False),
    StructField('vehicle_id', StringType(), False),
    StructField('location', StringType(), False),
    StructField('timestamp', TimestampType(), False),
    StructField('temperature', DoubleType(), False),
    StructField('weather_condition', StringType(), False),
    StructField('precipitation', DoubleType(), False),
    StructField('wind_speed', DoubleType(), False),
    StructField('humidity', IntegerType(), False),
    StructField('air_quality_index', DoubleType(), False)
])


emergencySchema = StructType([
    StructField('id', StringType(), False),
    StructField('vehicle_id', StringType(), False),
    StructField('location', StringType(), False),
    StructField('timestamp', TimestampType(), False),
    StructField('incident_id', StringType(), False),
    StructField('type', StringType(), False),
    StructField('status', StringType(), False),
    StructField('description', StringType(), False)
])
