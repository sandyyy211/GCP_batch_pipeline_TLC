

PROJECT_ID = 'gcp-cep-g23-330206'
SCHEMA = 'VendorID:INTEGER,lpep_pickup_datetime:DATETIME,lpep_dropoff_datetime:DATETIME,PULocationID:INTEGER,DOLocationID:INTEGER,passenger_count:INTEGER,trip_distance:FLOAT,total_amount:FLOAT,payment_type:INTEGER,trip_type:INTEGER,congestion_surcharge:FLOAT'

def discard_incomplete(data):
    """Filters out records that don't have an information."""
    return len(data['VendorID']) > 0 and len(data['lpep_pickup_datetime']) > 0 and len(data['lpep_dropoff_datetime']) > 0 and len(data['PULocationID']) > 0 and len(data['DOLocationID']) > 0


def convert_types(data):
	from datetime import datetime
	"""Converts string values to their appropriate type."""
	data['VendorID'] = int(data['VendorID']) if 'VendorID' in data else None
	data['lpep_pickup_datetime'] = datetime.strptime(data['lpep_pickup_datetime'], '%m/%d/%Y %H:%M') if 'lpep_pickup_datetime' in data else None
	data['lpep_dropoff_datetime'] = datetime.strptime(data['lpep_dropoff_datetime'], '%m/%d/%Y %H:%M') if 'lpep_dropoff_datetime' in data else None
	data['PULocationID'] = int(data['PULocationID']) if 'PULocationID' in data else None
	data['DOLocationID'] = int(data['DOLocationID']) if 'DOLocationID' in data else None
	data['passenger_count'] = int(data['passenger_count']) if 'passenger_count' in data else None
	data['trip_distance'] = float(data['trip_distance']) if 'trip_distance' in data else None
	data['total_amount'] = float(data['total_amount']) if 'total_amount' in data else None
	data['payment_type'] = int(data['payment_type']) if 'payment_type' in data else None
	data['trip_type'] = int(data['trip_type']) if 'trip_type' in data else None
	data['congestion_surcharge'] = float(data['congestion_surcharge']) if 'congestion_surcharge' in data else None
	return data	

def del_unwanted_cols(data):
	"""Delete the unwanted columns"""
	del data['store_and_fwd_flag']
	del data['RatecodeID']
	del data['fare_amount']
	del data['extra']
	del data['mta_tax']
	del data['tip_amount']
	del data['tolls_amount']
	del data['ehail_fee']
	del data['improvement_surcharge']
	return data

if __name__ == '__main__':

	import apache_beam as beam
	import argparse
	import datetime
	from datetime import datetime
	from apache_beam.options.pipeline_options import PipelineOptions
	from sys import argv
	import google.cloud
	from google.cloud import storage
	import os

	parser = argparse.ArgumentParser()
	known_args = parser.parse_known_args(argv)

	p = beam.Pipeline(options=PipelineOptions())

	(p | 'ReadData' >> beam.io.ReadFromText('gs://tlc_staging/*.csv', skip_header_lines =1)
       | 'SplitData' >> beam.Map(lambda x: x.split(','))
       | 'FormatToDict' >> beam.Map(lambda x: {"VendorID": x[0], "lpep_pickup_datetime": x[1], "lpep_dropoff_datetime": x[2], "store_and_fwd_flag": x[3],"RatecodeID": x[4], "PULocationID": x[5], "DOLocationID": x[6], "passenger_count": x[7], "trip_distance": x[8],"fare_amount": x[9],"extra": x[10],"mta_tax": x[11],"tip_amount": x[12],"tolls_amount": x[13],"ehail_fee": x[14],"improvement_surcharge": x[15],"total_amount": x[16],"payment_type": x[17],"trip_type": x[18],"congestion_surcharge": x[19]}) 
       | 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)
       | 'ChangeDataType' >> beam.Map(convert_types)
       | 'DeleteUnwantedData' >> beam.Map(del_unwanted_cols)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:tlc.batch_data'.format(PROJECT_ID),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
	result = p.run()
	result.wait_until_finish()
	
	os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/karthik_ramachandran/GCP_batch_pipeline_TLC/619683616285-51fd93ea97a6.json'
	bucket_name = 'tlc_staging'
	storage_client = storage.Client()
	source_bucket = storage_client.get_bucket(bucket_name)
	# source_blob = source_bucket.blob('batch')
	# print(source_bucket)
	
	# bucketFolder = 'batch'
	files = source_bucket.list_blobs()
	fileList = [file.name for file in files if '.' in file.name and '.csv' in file.name]
	destination_bucket = storage_client.get_bucket('tlc_raw')

	for file in fileList:
		source_blob = source_bucket.blob(file)
		source_bucket.copy_blob(source_blob, destination_bucket, file)
		source_blob.delete()
	