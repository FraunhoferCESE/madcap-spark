# Imports the Google Cloud client library
from google.cloud import storage, bigquery
from google.cloud.bigquery import SchemaField
import re, time, logging, logging.handlers

def import_to_bigquery(bucket, client, dataset):
	for blob in bucket.list_blobs():
		m = re.match("\S+\.(\S*)\.backup_info",blob.name)
		if(m):
			name = m.group(1)
			logging.info("------")
			logging.info("Importing {} to BigQuery...".format(name))
			job = client.load_table_from_storage("load-{}-from-storage-job_{}".format(name,time.strftime('%Y-%m-%d_%H-%M-%S-%z')), 
				dataset.table(name), 
				"gs://{}/{}".format(bucket.name, blob.name))
			job.source_format = 'DATASTORE_BACKUP'
			job.begin()
			logging.info("{} started.".format(job.name))
			retry_count = 100
			while(retry_count > 0 and job.state != 'DONE'):
				retry_count -= 1
				time.sleep(10)
				job.reload()
			logging.info("{}, state: {}, duration: {}".format(job.name, job.state, job.ended - job.started))
			if(job.errors):
				logging.error("Errors: {}".format(job.errors))
			else:
				logging.info("Rows imported: {}".format(job.output_rows))
		
		
def export_from_bigquery_to_storage(client, dataset, destination_bucket):
	for table in dataset.list_tables():
		logging.info("------")
		job = client.extract_table_to_storage("export-{}-from-storage-job_{}".format(table.name,time.strftime('%Y-%m-%d_%H-%M-%S-%z')), 
			table, 
			"gs://{}/{}-*.json.gz".format(destination_bucket.name,table.name))
		job.destination_format = 'NEWLINE_DELIMITED_JSON'
		job.compression = 'GZIP'
		job.begin()
		logging.info("{} started.".format(job.name))
		retry_count = 100
		while(retry_count > 0 and job.state != 'DONE'):
			retry_count -= 1
			time.sleep(10)
			job.reload()		
		logging.info("{}, state: {}, duration: {}".format(job.name, job.state, job.ended - job.started))
		if job.errors:
			logging.error("Errors: {}".format(job.errors))
		else:
			logging.info("No errors detected. Deleting {}.".format(table.name))
			table.delete()
	if len(list(dataset.list_tables())) == 0:
		logging.info("Dataset contains no tables. Deleting.")
		dataset.delete()
	else:
		logging.warn("Dataset still contains tables. Check for errors.")
	
	
	
def download_backup_from_storage(client, bucket):
	for blob in bucket.list_blobs():
		logging.info("Downloading {}...".format(blob.name))
		blob.download_to_filename(blob.name)
		logging.info("Download finished. Deleting {}.".format(blob.name))
		blob.delete()
	

if __name__ == "__main__":
	logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
	rootLogger = logging.getLogger()
	rootLogger.setLevel(logging.INFO)

	logPath = 'log'
	fileName = 'import'
	fileHandler = logging.handlers.RotatingFileHandler("{0}/{1}.log".format(logPath, fileName), maxBytes=(1048576*5), backupCount=7)
	fileHandler.setFormatter(logFormatter)
	rootLogger.addHandler(fileHandler)

	consoleHandler = logging.StreamHandler()
	consoleHandler.setFormatter(logFormatter)
	rootLogger.addHandler(consoleHandler)	
	
	# Instantiate clients
	# If downloading from MADCAP production, change madcap-dev1 to madcap-142815
	storage_client = storage.Client(project='madcap-dev1')
	bq_client = bigquery.Client(project='madcap-dev1')

	## If necessary, change the backup_bucket_name to the appropriate name. 
	backup_bucket_name = 'madcap-backup'
	backup_bucket = storage_client.get_bucket(backup_bucket_name)
	if not backup_bucket.exists():
		logging.error("Can't find {} bucket. Quitting.".format(backup_bucket_name))
		quit()

	## If necessary, change the export_bucket_name to the appropriate name. 
	export_bucket_name = 'madcap-export'
	export_bucket = storage_client.get_bucket(export_bucket_name)
	if not backup_bucket.exists():
		logging.error("Can't find {} bucket. Quitting.".format(export_bucket_name))
		quit()

	dataset = bq_client.dataset('madcap_backup_import')
	if not dataset.exists():
		logging.error("Can't find dataset '{}'. Creating dataset...".format(dataset.name))
		dataset.create()
			
	import_to_bigquery(backup_bucket, bq_client, dataset)
	export_from_bigquery_to_storage(bq_client, dataset, export_bucket)
	download_backup_from_storage(storage_client, export_bucket)
