"""MQTT Daemon which collected Hydroquebec Data.

And send it to MQTT using Home-Assistant format.
"""
import asyncio
from datetime import datetime, timedelta, timezone
import logging
import json
import os
import signal
import uuid

from influxdb import InfluxDBClient
from yaml import load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from pyhydroquebec.client import HydroQuebecClient
from pyhydroquebec.consts import DAILY_MAP, CURRENT_MAP, HQ_TIMEZONE, COST_MEASUREMENT, KWH_MEASUREMENT


def _get_logger(logging_level):
    """Build logger."""
    logger = logging.getLogger(name='influxdb_pyhydroquebec')
    logger.setLevel(logging_level)
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger

def _create_influxdb_client(influxdb_config):
    """Creates an InfluxDB client with the given |influxdb_config|."""
    host = influxdb_config.get('host', None)
    port = influxdb_config.get('port', None)
    username = influxdb_config.get('username', None)
    password = influxdb_config.get('password', None)
    database = influxdb_config.get('database', None)
    return InfluxDBClient(host, port, username, password, database)

def create_influxdb_datapoint(measurement, contract_id, entity_id,
                                yyyy_mm_dd, hour, value):
    """Returns an influxDB datapoint that can be written."""
    influxdb_date = datetime.strptime(yyyy_mm_dd + "--0400", "%Y-%m-%d-%z").astimezone(timezone.utc)
    influxdb_date = influxdb_date + timedelta(hours=hour)
    return { "measurement": measurement,
            "tags": {
                "entity_id": entity_id, # Name of the thing being measured.
                    "contract": contract_id
                },
            # Timestamp is brought back to string format.
            "time": datetime.strftime(influxdb_date, '%Y-%m-%dT%H:%M:%S.%fZ'),
            "fields": {
                "value": value  # Ideally a float.
            }
        }

class InfluxDBHydroQuebec:
    """Periodically writes to InfluxDB"""

    def __init__(self):
        """Create new InfluxDBHydroQuebec Object."""
        with open(os.environ['CONFIG']) as fhc:
            self.config = load(fhc, Loader=Loader)
        self.timeout = self.config.get('timeout', 30)
        # 6 hours
        self.frequency = self.config.get('frequency', None)
        if 'influxdb' not in self.config:
            raise AttributeError('influxdb not in config')
        self.logging_level = os.environ.get('LOGGING', 'WARNING')
        self.logger = _get_logger(self.logging_level)
        self.logger.info('Initializing...')
        self.client = _create_influxdb_client(self.config.get('influxdb')[0])
        self.must_run = False

    async def _init_main_loop(self):
        """Init before starting main loop."""

    async def _main_loop(self):
        """Run main loop."""
        self.logger.debug("Running the main loop.")
        for account in self.config['accounts']:
            client = HydroQuebecClient(account['username'],
                                       account['password'],
                                       self.timeout,
                                       log_level=self.logging_level)
            await client.login()
            datapoints = []
            for contract_data in account['contracts']:
                # Get contract
                customer = None
                contract_id = str(contract_data['id'])
                # Find the right customer data according to the contract ID.
                for client_customer in client.customers:
                    if str(client_customer.contract_id) == contract_id:
                        customer = client_customer

                if customer is None:
                    self.logger.warning('Contract %s not found', contract_id)
                    continue

                await customer.fetch_current_period()

                # Look for yesterday's data.
                yesterday = datetime.now(HQ_TIMEZONE) - timedelta(days=1)
                yesterday_str = yesterday.strftime("%Y-%m-%d")
                await customer.fetch_daily_data(yesterday_str, yesterday_str)
                await customer.fetch_hourly_data(yesterday_str)

                # If it's not there, look for the day before.
                if not customer.current_daily_data:
                    yesterday = yesterday - timedelta(days=1)
                    yesterday_str = yesterday.strftime("%Y-%m-%d")
                    await customer.fetch_daily_data(yesterday_str, yesterday_str)

                # Look at the date of the data we received.
                date_of_data = list(customer.current_daily_data.keys())[0]

                # Write balance
                datapoints.append(create_influxdb_datapoint(
                        COST_MEASUREMENT, contract_id, 'daily_balance',
                        date_of_data, 0, float(customer.balance)))

                # # Current period
                # for data_name, data in CURRENT_MAP.items():
                #     # Publish sensor
                #     sensor_topic = self._publish_sensor(data_name,
                #                                         customer.contract_id,
                #                                         unit=data['unit'],
                #                                         icon=data['icon'],
                #                                         device_class=data['device_class'])
                #     # Send sensor data
                #     self.mqtt_client.publish(
                #             topic=sensor_topic,
                #             payload=customer.current_period[data_name])

                # # Yesterday data
                # for data_name, data in DAILY_MAP.items():
                #     # Publish sensor
                #     sensor_topic = self._publish_sensor('yesterday_' + data_name,
                #                                         customer.contract_id,
                #                                         unit=data['unit'],
                #                                         icon=data['icon'],
                #                                         device_class=data['device_class'])
                #     # Send sensor data
                #     self.mqtt_client.publish(
                #             topic=sensor_topic,
                #             payload=customer.current_daily_data[yesterday_str][data_name])
                for hour, data in customer.hourly_data[date_of_data]["hours"].items():
                    datapoints.append(create_influxdb_datapoint(
                        KWH_MEASUREMENT, contract_id, 'total_consumption',
                        date_of_data, hour, float(data['total_consumption'])))

            self.client.write_points(datapoints)
            await client.close_session()

        if self.frequency is None:
            self.logger.info("Frequency is None, so it's a one shot run")
            self.must_run = False
            return

        self.logger.info("Waiting for %d seconds before the next check", self.frequency)
        i = 0
        while i < self.frequency and self.must_run:
            await asyncio.sleep(1)
            i += 1

    async def async_run(self):
        """Run main base loop."""
        self.logger.info("Starting main process...")
        self.must_run = True
        # Add signal handler
        signal.signal(signal.SIGINT, self._signal_handler)
        await self._init_main_loop()
        while self.must_run:
            self.logger.debug("We are in the main loop")
            await self._main_loop()
        self.logger.info("Main loop stopped")
        await self._loop_stopped()

    def _signal_handler(self, signal_, frame):
        """Signal handler."""
        self.logger.info("SIGINT received")
        self.logger.debug("Signal %s in frame %s received", signal_, frame)
        self.must_run = False
        self.logger.info("Exiting...")

    async def _loop_stopped(self):
        """Run after the end of the main loop."""
