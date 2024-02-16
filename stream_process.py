import argparse

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.io import ReadFromPubSub
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions

TABLE_NAME = "Your_Table_Name"
DATASET = "Your_Dataset"
PROJECT = "Your_Project_ID"
TOPIC_NAME = "Your_topic_name"
PUBSUB_TOPIC = f"projects/{PROJECT}/topics/{TOPIC_NAME}"
BQ_TABLE_COMPLETE_NAME = f"{PROJECT}:{DATASET}.{TABLE_NAME}"

TABLE_SCHEMA = {"fields": [
    {"name": "productid", "type": "STRING", "mode": "NULLABLE"},
    {"name": "userid", "type": "STRING", "mode": "NULLABLE"},
    {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "timestamp", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "orderid", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "categoryid", "type": "STRING", "mode": "NULLABLE"},
]}

import base64
import json
from datetime import datetime, timedelta
import random
import csv
import logging


def parse_ascii_message(data):
    return json.loads(data.decode("ascii"))


class LoadCategoryParse(beam.DoFn):
    def __init__(self):
        # super().__init__()
        beam.DoFn.__init__(self)

    def process(self, elem):
        row = list(csv.reader([elem]))[0]
        yield {
            "productid": row[0],
            "categoryid": row[1]
        }


class OrdersDataParse(beam.DoFn):
    def __init__(self):
        # super().__init__()
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def random_timestamp(self):
        start_date = datetime(2024,1,2,0,0,0)
        end_date = start_date + timedelta(days=1)
        random_date = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
        return int(random_date.timestamp())

    def process(self, element):
        try:
            data = json.loads(element)

            # Extract common fields
            event = data["event"]
            messageid = data["messageid"]
            userid = data["userid"]

            # Extract fields specific to OrderEvent
            if event == "OrderEvent":
                orderid = data["orderid"]
                lineitems = data.get("lineitems", [])

                for item in lineitems:
                    productid = item.get("productid", '')
                    quantity = item.get("quantity", 0)
                    #        time.sleep(5)

                    yield {
                        "event": event,
                        "messageid": messageid,
                        "userid": userid,
                        "productid": productid,
                        "quantity": quantity,
                        "orderid": orderid,
                        "timestamp": self.random_timestamp()
                    }
            else:
                # For other event types, default values or additional handling can be added
                yield {
                    "event": event,
                    "messageid": messageid,
                    "userid": userid,
                    "productid": '',
                    "quantity": 0,
                    "orderid": '',
                    "timestamp": self.random_timestamp()
                }
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")


class DictToTuple(beam.DoFn):
    def __init__(self):
        # super().__init__()
        beam.DoFn.__init__(self)

    def process(self, elem):
        product = elem["productid"]
        userid = elem["userid"]
        quantity = elem["quantity"]
        orderid = elem["orderid"]
        timestamp = elem["timestamp"]
        result = (product, userid, quantity, orderid, timestamp)
        yield result


def run(argv=None):
    parser = argparse.ArgumentParser()

    # Topic
    parser.add_argument(
        '--input_topic',
        type=str,
        default='projects/{PROJECT_ID}/topics/{TOPIC_NAME}'
    )

    # Csv from GCS
    parser.add_argument(
        '--category_input',
        type=str,
        default='gs://retail_test_data/product-category-map.csv'
    )
    args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True, streaming=True)

    with beam.Pipeline(options=pipeline_options) as p:
        orders_data = (
                p
                | 'ReadFromJson' >> ReadFromPubSub(topic=args.input_topic)
                | 'Decode base64 message' >> beam.Map(lambda msg: base64.b64decode(msg))
                | 'Parse ascii and load message into a object' >> beam.Map(parse_ascii_message)
                | 'ParseOrderData' >> beam.ParDo(OrdersDataParse())
                | 'Tuple' >> beam.ParDo(DictToTuple())
        )

        category_data = (
                p
                | 'ReadCategoryFromCsv' >> beam.io.ReadFromText(args.category_input)
                | 'ParseCategoryData' >> beam.ParDo(LoadCategoryParse())
                | 'ToIterable' >> beam.Map(lambda elem: (elem['productid'], elem['categoryid']))
                | 'GroupByKey' >> beam.GroupByKey()
        )

        def brodcast_inner_join(element, side_input):
            product = element[0]
            userid = element[1]
            quantity = element[2]
            orderid = element[3]
            timestamp = element[4]
            categoryid = side_input.get(product, [])

            joined = [{
                "productid": product,
                "userid": userid,
                "quantity": quantity,
                "timestamp": timestamp,
                "orderid": orderid,
                "categoryid": category
            } for category in categoryid]
            return joined

        brodcast_join = (
                orders_data | beam.FlatMap(brodcast_inner_join, side_input=beam.pvalue.AsDict(category_data))
                | 'Write in bigquery' >> WriteToBigQuery(
            table=BQ_TABLE_COMPLETE_NAME,
            schema=TABLE_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()