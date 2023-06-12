import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from dotenv import load_dotenv

# Get configuration variables from .env implementation
load_dotenv()

# Specify input and output file paths
input_file = os.getenv("INPUT_FILE")
output_table = os.getenv("OUTPUT_TABLE")

# Pipeline Configuration
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = os.getenv("PROJECT_ID")
google_cloud_options.job_name = 'my-first-dataflow-project-python'
google_cloud_options.staging_location = os.getenv("STAGING_LOCATION")
google_cloud_options.temp_location = os.getenv("TEMP_LOCATION")
google_cloud_options.region = 'us-central1'
options.view_as(StandardOptions).runner = 'DataflowRunner'

# Define ParDo class to transform data
class ConvertTimestamp(beam.DoFn):
    def process(self, element):
        import pandas as pd

        element['open_time'] = pd.to_datetime(int(element['open_time']), unit='ms', utc=True)
        element['close_time'] = pd.to_datetime(int(element['close_time']), unit='ms', utc=True)

        return [element]

try:
    # Build and run Apache Beam Pipeline
    with beam.Pipeline(options=options) as p:
        (
            p | 'ReadInputFile' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | 'ParseCSV' >> beam.Map(lambda line: line.split(','))
            | 'CreateDict' >> beam.Map(lambda elements: {
                'open_time': elements[0],
                'open': float(elements[1]),
                'high': float(elements[2]),
                'low': float(elements[3]),
                'close': float(elements[4]),
                'volume': float(elements[5]),
                'close_time': elements[6],
                'quote_asset_volume': float(elements[7]),
                'num_trades': int(elements[8]),
                'taker_base_vol': float(elements[9]),
                'taker_quote_vol': float(elements[10]),
                'ignore': int(elements[11])
            })
            | 'ConvertTimestamp' >> beam.ParDo(ConvertTimestamp())
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table=output_table,
                schema='open_time:TIMESTAMP, open:FLOAT, high:FLOAT, low:FLOAT, close:FLOAT, '
                       'volume:FLOAT, close_time:TIMESTAMP, quote_asset_volume:FLOAT, '
                       'num_trades:INTEGER, taker_base_vol:FLOAT, taker_quote_vol:FLOAT, ignore:INTEGER',
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
        )

        # Start Pipeline running and wait for it to complete
        result = p.run()
        result.wait_until_finish()

except Exception as e:
    print("An error occurred: {}".format(str(e)))
