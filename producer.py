#!/usr/bin/env python

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import schedule
import time

batch_size = 10


def schedule_kafka_producer(i, batch):
    for i in range(i, batch):
        record_value = json.dumps(data[i])
        print("Producing record: {}\t{}".format("timeseries data", record_value))
        producer.produce(topic, key="timeseries data", value=record_value, on_delivery=acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)

    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))


if __name__ == '__main__':

    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0


    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """
        Message Delivered Successfully @!!!!!!
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))


    json_file = open('input01.json', "r")
    data = json.load(json_file)
    import pycron
    import time
    i = 0
    j = len(data)
    while True:
        if pycron.is_now('*/1 * * * *'):  # True Every Sunday at 02:00
            if i <= len(data):
                schedule_kafka_producer(i, i+batch_size)
                i = i + batch_size
            time.sleep(60)  # The process should take at least 60 sec
            # to avoid running twice in one minute
        else:
            time.sleep(15)
