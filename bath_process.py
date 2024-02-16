import argparse
import logging
import time
from datetime import datetime
import json 

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import datetime, timedelta
import random



def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
    """Converts a unix timestamp into a formatted string."""
    return datetime.fromtimestamp(t).strftime(fmt)

def str2timestamp(s, fmt='%Y-%m-%d-%H-%M'):
    """Converts a string into a unix timestamp."""
    dt = datetime.strptime(s, fmt)
    epoch = datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds()

class UserQuantityDict(beam.DoFn):

    def process(self, user_quantity, window=beam.DoFn.WindowParam):
        user, quantity = user_quantity
        start = timestamp2str(int(window.start))
        yield {
            'userid': user,
            'total_quantity': quantity,
            'window_start': start,
            'processing_time': timestamp2str(int(time.time()))
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
            event = data['event']
            messageid = data['messageid']
            userid = data['userid']

            # Extract fields specific to OrderEvent
            if event == 'OrderEvent':
                orderid = data['orderid']
                lineitems = data.get('lineitems', [])

                for item in lineitems:
                    productid = item.get('productid', '')
                    quantity = item.get('quantity', 0)
                    #        time.sleep(5)

                    yield {
                        'event': event,
                        'messageid': messageid,
                        'userid': userid,
                        'productid': productid,
                        'quantity': quantity,
                        'orderid': orderid,
                        'timestamp': self.random_timestamp()
                    }
            else:
                # For other event types, default values or additional handling can be added
                yield {
                    'event': event,
                    'messageid': messageid,
                    'userid': userid,
                    'productid': '',
                    'quantity': 0,
                    'orderid': '',
                    'timestamp': self.random_timestamp()
                }
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")



class ExtractAndSumQuantity(beam.PTransform):

    def __init__(self, field):
        # super().__init__()
        beam.PTransform.__init__(self)
        self.field = field

    def expand(self, pcoll):
        return (
                pcoll
                | beam.Map(lambda elem: (elem[self.field], elem['quantity']))
                | beam.CombinePerKey(sum))


    
class HourlyUserProduct(beam.PTransform):
    def __init__(self,start_min, stop_min, window_duration):
        beam.PTransform.__init__(self)
        self.start_timestamp = str2timestamp(start_min)
        self.stop_timestamp = str2timestamp(stop_min)
        self.window_duration_in_seconds = window_duration * 60

    def expand(self, pcoll):
        return (
            pcoll
            | 'ParseGameEventFn' >> beam.ParDo(OrdersDataParse())
            | 'FilterStartTime' >> beam.Filter(lambda elem: elem['timestamp'] > self.start_timestamp)
            | 'FilterEndTime' >> beam.Filter(lambda elem: elem['timestamp'] < self.stop_timestamp)
            | 'AddEventTimestamps' >> beam.Map(lambda elem: beam.window.TimestampedValue(elem, elem['timestamp']))
            | 'FixedWindowsTeam' >> beam.WindowInto(beam.window.FixedWindows(self.window_duration_in_seconds))
            | 'ExtractAndSumScore' >> ExtractAndSumQuantity('userid')
        )



def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument(
    '--window_duration',
    type=int,
    default=60,
    help='Numeric value of fixed window duration, in minutes')

    parser.add_argument(
        '--start_min',
        type=str,
        default='2024-01-02-00-00',
    
    parser.add_argument(
        '--stop_min',
        type=str,
        default='2024-01-03-00-00',
    
    ## GOOGLE-CLOUD
    parser.add_argument(
        '--input',
        type=str,
        default='gs://retail_test_data/orders.json'
    )


    args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadFromText' >> beam.io.ReadFromText(args.input)
            | 'OrdersDataParseFn' >> HourlyUserProduct(args.start_min, args.stop_min, args.window_duration)
            | 'UserQuantityDict' >> beam.ParDo(UserQuantityDict())
            | 'Write results GCS' >> beam.io.WriteToText('gs://retail_test_data/output')
        )



if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()