import datetime
import os
import random
from typing import Any
import uuid
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
import constants

# loading env variables from .env
load_dotenv()
random.seed(42)

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
traffic_camera_topic = os.getenv('TRAFFIC_CAMERA_TOPIC')
emergency_topic = os.getenv('EMERGENCY_TOPIC')

# start time and location
start_time = datetime.datetime.now()
start_location = constants.LONDON_COORDINATES.copy()
vehicle_id = 'fazer-250'


def get_next_time():
    global start_time
    start_time += datetime.timedelta(seconds=random.randint(30, 60)) # update frequent
    return start_time


def simulate_vehicle_movement() -> dict[str, float]:
    global start_location

    # move towards birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location
 

def generate_vehicle_data(vehicle_id: str) -> dict[str, Any]:
    location = simulate_vehicle_movement()

    return dict(
        id=uuid.uuid4(),
        vehicle_id=vehicle_id,
        timestamp= get_next_time().isoformat(),
        location=(location['latitude'], location['longitude']),
        speed=random.uniform(10, 40),
        direction='North-East',
        make='BMW',
        model='C500',
        year=2024,
        fuel_type='Hybrid'
    )


def generate_gps_data(vehicle_id: str, timestamp: str, vehicle_type='private') -> dict[str, Any]:
    return dict(
        id=uuid.uuid4(),
        vehicle_id=vehicle_id,
        timestamp=timestamp,
        speed=random.uniform(0, 40), # km/h
        direction='North-East',
        vehicle_type=vehicle_type
    )

def generate_traffic_camera_data(vehicle_id: str, timestamp: str, location: tuple[float], camera_id: str) -> dict[str, Any]:
    return dict(
        id=uuid.uuid4(),
        vehicle_id=vehicle_id,
        camera_id=camera_id,
        location=location,
        timestamp=timestamp,
        snapshot='Base64EncodedString'
    )


def generate_eid(vehicle_id: str, timestamp: str, location: tuple[float]) -> dict[str, Any]:
    type_choices = ['Accident', 'Fire', 'Medical', 'Police', 'None']
    status_choices = ['Active', 'Resolved']
    return dict(
        id=uuid.uuid4(),
        vehicle_id=vehicle_id,
        location=location,
        timestamp=timestamp,
        incident_id=uuid.uuid4(),
        type=random.choice(type_choices),
        status=random.choice(status_choices),
        description='Description of the incident'
    )


def generate_weather_data(vehicle_id: str, timestamp: str, location: tuple[float]) -> dict[str, Any]:
    choices = ['Sunny', 'Cloudy', 'Rainy', 'Snow']
    return dict(
        id=uuid.uuid4(),
        vehicle_id=vehicle_id,
        location=location,
        timestamp=timestamp,
        temperature=random.uniform(-5, 26),
        weather_condition=random.choice(choices),
        precipitation=random.uniform(0, 25),
        wind_speed=random.uniform(0, 100),
        humidity=random.randint(0, 100), # percentage
        air_quality_index=random.uniform(0, 500)
    )


def simulate_journey(producer: KafkaProducer, vehicle_id: str) -> None:
    while True:
        vehicle_data = generate_vehicle_data(vehicle_id)
        gps_data = generate_gps_data(vehicle_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(
            vehicle_id, vehicle_data['timestamp'],
            vehicle_data['location'], 'Canon-x1y3'
        )
        weather_data = generate_weather_data(vehicle_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_eid(vehicle_id, vehicle_data['timestamp'], vehicle_data['location'])
        print(vehicle_data, gps_data, traffic_camera_data, weather_data, emergency_incident_data, end='\n')
        break


if __name__ == "__main__":
    # if kafka in cloud we need to provide config for username, password and if needed schema registry as well
    producer = None
    # producer = KafkaProducer(bootstrap_servers=kafka_boostrap_servers)


    try:
        simulate_journey(producer, vehicle_id)

    except KafkaError as error:
        print("Failed to send messages:", error)
    except KeyboardInterrupt:
        print('Simulation is ended by user')
    except Exception as error:
        print('Unexpected error occurred: ', error)
