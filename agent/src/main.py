from paho.mqtt import client as mqtt_client
import json
import time
from schema.aggregated_data_schema import AggregatedDataSchema
from schema.accelerometer_schema import AccelerometerSchema
from schema.gps_schema import GpsSchema
from schema.parking_schema import ParkingSchema
from file_datasource import FileDatasource
import config


def connect_mqtt(broker, port):
    """Create MQTT client"""
    print(f"CONNECT TO {broker}:{port}")

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to MQTT Broker ({broker}:{port})!")
        else:
            print("Failed to connect {broker}:{port}, return code %d\n", rc)
            exit(rc)  # Stop execution

    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.connect(broker, port)
    client.loop_start()
    return client


def publish(client, topics, datasource, delay):
    datasource.startReading()
    while True:
        time.sleep(delay)
        data_list = datasource.read()
        if not data_list:
            print("No more data to read.")
            break

        accelerometer_data, gps_data, parking_data = data_list

        # Accelerometer
        if accelerometer_data is not None:
            msg = AccelerometerSchema().dumps(accelerometer_data)
            client.publish(topics['accelerometer'], msg)
            print(f"Sent accelerometer data to topic `{topics['accelerometer']}`")
        
        # GPS
        if gps_data is not None:
            msg = GpsSchema().dumps(gps_data)
            client.publish(topics['gps'], msg)
            print(f"Sent GPS data to topic `{topics['gps']}`")

        # Parking
        if parking_data is not None:
            msg = ParkingSchema().dumps(parking_data)
            client.publish(topics['parking'], msg)
            print(f"Sent parking data to topic `{topics['parking']}`")



def run():
    # Prepare mqtt client
    client = connect_mqtt(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT)
    # Topics
    topics = {
        'accelerometer': 'measurement/accelerometer',
        'gps': 'measurement/gps',
        'parking': 'measurement/parking'
    }
    # Prepare datasource
    datasource = FileDatasource("data/accelerometer.csv", "data/gps.csv", "data/parking.csv")
    # Infinity publish data
    publish(client, topics, datasource, config.DELAY)


if __name__ == "__main__":
    run()
