# madcap-spark

Simple python scripts for backing up MADCAP data to a local disk. Requires the [Google Cloud SDK](https://cloud.google.com/sdk/) be installed on your computer.

The script performs the following:
1. Import Cloud datastore backups from the 'madcap-backup' Cloud Storage bucket into BigQuery
1. Export from BigQuery to 'madcap-export' Cloud Storage bucket in GZipped JSON
1. Download all files from 'madcap-export'
1. Clean up temporary BigQuery and 'madcap-export' contents

## Prerequisites
- Python2/3 installed
- [Google Cloud SDK](https://cloud.google.com/sdk/) be installed on your computer
- Must have appropriate Cloud Console permissions to read/write data to/from Cloud Storage & BigQuery for your project
- MADCAP data store backup has been created in the `madcap-backup` Cloud Storage bucket. The backup is a manual step that is initiated from the Google Cloud Console. See [Datastore Backup and Restoring Instructions](https://cloud.google.com/appengine/docs/standard/python/console/datastore-backing-up-restoring)
- `madcap-backup` and `madcap-export` buckets must exist in Cloud Storage for your project. Bucket names can be changed in the Python script if desired.

## Running
- Change bucket name (Cloud Storage) and temporary table name (BigQuery) variables within the script if desired.
- Run 'python download-backups.py'. Execution will take some time. Files will be downloaded to the execution directory.