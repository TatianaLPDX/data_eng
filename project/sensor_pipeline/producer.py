#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================
from datetime import date
import ccloud_lib
from confluent_kafka import Producer, KafkaError
import json
from urllib.request import urlopen


#config file location
config_file = "/home/tatianal/config.conf"

def log_data(err, msg):
	date_time = today.strftime("%b_%d_%Y")
	file_to_record = '/home/tatianal/logs.json'
	return send_messages += 1


def main():
	
    # Read arguments and configurations and initialize
    topic = "sensor_data"
    conf = ccloud_lib.read_config(config_file)
    today = date.today()

    # producer settings
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
    })

    # Create topic 
    ccloud_lib.create_topic(conf, topic)

    send_messages = 0
    #get data from web server
    url = "http://rbi.ddns.net/getBreadCrumbData"

    html = urlopen(url)

    string = html.read().decode('utf-8')

    data = json.loads(string)

    date_time = today.strftime("%b_%d_%Y")
    file_to_record = '/home/tatianal/logs.json'

    count = 0
    with open(file_to_record, 'w') as file:
        json.dump(len(data), file)
        
    #iterate though data
    for record in data:
        event_no_trip = record['EVENT_NO_TRIP']
        event_no_stop = record['EVENT_NO_STOP']
        opd_date = record['OPD_DATE']
        vehicle_id = record['VEHICLE_ID']
        meters = record['METERS']
        act_time = record['ACT_TIME']
        velocity = record['VELOCITY']
        direction = record['DIRECTION']
        radio_quality = record['RADIO_QUALITY']
        gps_longitude = record['GPS_LONGITUDE']
        gps_latitude = record['GPS_LATITUDE']
        gps_satellites = record['GPS_SATELLITES']
        gps_hdop = record['GPS_HDOP']
        schedule_deviation = record['SCHEDULE_DEVIATION']

        producer.produce(topic, value=json.dumps(record), on_delivery=log_data)

		#constrains on size
        producer.poll(0)
        if count % 10000 == 0:
            producer.flush()
        count+=1

    producer.flush()

    message = "{date_time}:{} messages sent to topic {} \t".format(send_messages, topic)
    with open(file_to_record, 'a') as file:
        file.append(message)
     
#start main script execution  
if __name__== "__main__":
   main()

