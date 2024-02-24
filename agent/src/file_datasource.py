from csv import reader
from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.parking import Parking
from domain.aggregated_data import AggregatedData
import config

class FileDatasource:
    def __init__(self, accelerometer_filename: str, gps_filename: str, parking_filename: str) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.accelerometer_file = None
        self.gps_file = None
        self.parking_file = None

    def startReading(self):
        self.accelerometer_file = open(self.accelerometer_filename, 'r')
        self.gps_file = open(self.gps_filename, 'r')
        self.parking_file = open(self.parking_filename, 'r')
        self.accelerometer_reader = reader(self.accelerometer_file)
        self.gps_reader = reader(self.gps_file)
        self.parking_reader = reader(self.parking_file)

        next(self.accelerometer_reader, None)
        next(self.gps_reader, None)
        next(self.parking_reader, None)

    def read(self):
        data = []

        def read_data_or_restart(file, reader):
            try:
                return next(reader)
            except StopIteration:
                file.seek(0)
                next(reader, None)
                return next(reader)

        # Accelerometer
        accelerometer_data = read_data_or_restart(self.accelerometer_file, self.accelerometer_reader)
        accelerometer = Accelerometer(
            float(accelerometer_data[0]),
            float(accelerometer_data[1]),
            float(accelerometer_data[2]),
        )
        data.append(accelerometer)

        # GPS
        gps_data = read_data_or_restart(self.gps_file, self.gps_reader)
        gps = Gps(
            float(gps_data[0]),
            float(gps_data[1]),
        )
        data.append(gps)

        # Parking
        parking_data = read_data_or_restart(self.parking_file, self.parking_reader)
        parking = Parking(
            int(parking_data[0]),
            Gps(
                float(parking_data[1]),
                float(parking_data[2]),
            )
        )
        data.append(parking)

        return data



    def stopReading(self):
        if self.accelerometer_file:
            self.accelerometer_file.close()
        if self.gps_file:
            self.gps_file.close()
        if self.parking_file:
            self.parking_file.close()

