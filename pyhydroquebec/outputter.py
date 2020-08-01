"""PyHydroQuebec Output Module.

This module defines the different output functions:
* text
* influxdb
* json
"""
from datetime import datetime, timedelta, timezone
from influxdb import InfluxDBClient

from pyhydroquebec.consts import (OVERVIEW_TPL,
                                  CONSUMPTION_PROFILE_TPL,
                                  YESTERDAY_TPL, ANNUAL_TPL, HOURLY_HEADER, HOURLY_TPL)


def output_text(customer, show_hourly=False):
    """Format data to get a readable output."""
    print(OVERVIEW_TPL.format(customer))
    if customer.current_period['period_total_bill']:
        print(CONSUMPTION_PROFILE_TPL.format(d=customer.current_period))
    if customer.current_annual_data:
        print(ANNUAL_TPL.format(d=customer.current_annual_data))
    yesterday_date = list(customer.current_daily_data.keys())[0]
    data = {'date': yesterday_date}
    data.update(customer.current_daily_data[yesterday_date])
    print(YESTERDAY_TPL.format(d=data))
    if show_hourly:
        print(HOURLY_HEADER)
        for hour, data in customer.hourly_data[yesterday_date]["hours"].items():
            print(HOURLY_TPL.format(d=data, hour=hour))


def output_influx(customer, influxdb_host, influxdb_port, influxdb_database,
                  influxdb_username, influxdb_password):
    """Print data using influxDB format."""
    yesterday_date = list(customer.current_daily_data.keys())[0]
    yesterday = datetime.strptime(yesterday_date + "--0400", "%Y-%m-%d-%z").astimezone(timezone.utc) 
    datapoints = []
    for hour, data in customer.hourly_data[yesterday_date]["hours"].items():
        time_of_data = yesterday + timedelta(hours=hour)
        datapoints.append({
            "measurement": "KWh",
            "tags": {
                "entity_id": "total_consumption",
            },
            "time": datetime.strftime(time_of_data, '%Y-%m-%dT%H:%M:%S.%fZ'),
            "fields": {
                "value": float(data['total_consumption'])
            }
        })

    client = InfluxDBClient(influxdb_host, influxdb_port, influxdb_username,
                             influxdb_password, influxdb_database)
    client.write_points(datapoints)


def output_json(data):
    """Print data as Json."""
    raise Exception("FIXME")
#    print(json.dumps(data))
