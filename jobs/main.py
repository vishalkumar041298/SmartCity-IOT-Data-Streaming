import datetime
import os
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
import constants

# loadinf env variables from .env
load_dotenv()

LATITUDE_INCREMENT = (
    constants.BIRMINGHAM_COORDINATES.get('latitude') - constants.LONDON_COORDINATES.get('latitude')
) / 100

LONGITUDE_INCREMENT = (
    constants.BIRMINGHAM_COORDINATES.get('longitude') - constants.LONDON_COORDINATES.get('longitude')
) / 100

# Getting os environments
vehicle_topic = os.getenv('VEHICLE_TOPIC')
kafka_boostrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
gps_topic = os.getenv('GPS_TOPIC')
traffic_topic = os.getenv('TRAFFIC_TOPIC')
emergency_topic = os.getenv('EMERGENCY_TOPIC')

# start time and location
start_time = datetime.datetime.now()
start_location = constants.LONDON_COORDINATES.copy()
vehicle = 'fazer-250'


def simulate_journey(producer: KafkaProducer, vehicle: str) -> None:
    pass


if __name__ == "__main__":
    # if kafka in cloud we need to provide config for username, password and if needed schema registry as well

    producer = KafkaProducer(bootstrap_servers=kafka_boostrap_servers)


    try:
        simulate_journey(producer, vehicle)

    except KafkaError as e:
        print("Failed to send messages:", e)
