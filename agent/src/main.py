from paho.mqtt import client as mqtt_client
import time
from schema.aggregated_data_schema import AggregatedDataSchema
from file_datasource import FileDatasource
import config
from datetime import datetime
from domain.aggregated_data import AggregatedData
import logging


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


def publish(client, topic, datasource, delay):
    datasource.startReading()
    while True:
        time.sleep(delay)
        data_list = datasource.read()
        if not data_list:
            print("No more data to read.")
            break

        accelerometer_data, gps_data, parking_data = data_list

        msg = AggregatedDataSchema().dumps(AggregatedData(
            accelerometer_data,
            gps_data,
            parking_data,
            datetime.now(),
            1
        ))

        logging.info(f"Sent data to topic `{topic}`")

        client.publish(topic, msg)


def run():
    # Prepare mqtt client
    client = connect_mqtt(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT)
    # Prepare datasource
    datasource = FileDatasource("data/accelerometer.csv", "data/gps.csv", "data/parking.csv")
    # Infinity publish data
    publish(client, config.MQTT_TOPIC, datasource, config.DELAY)


if __name__ == "__main__":
    run()
