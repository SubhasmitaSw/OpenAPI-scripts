#!/bin/bash


# Variables
INSTANCE_NAME="civo-production"
DATABASE_NAME_API="prod_civo_api"
DATABASE_NAME_DOTCOM="prod_civo_dotcom"
BACKUP_BUCKET="gs://sql-thg-backups"
PROCESSING_DIR="../processing"
NEW_BACKUP_BUCKET=""

# Step 1: Export MySQL dump backups to the instance
echo "Exporting MySQL dump backups from GCP storage bucket to the instance..."
gcloud sql export sql $INSTANCE_NAME $BACKUP_BUCKET/$DATABASE_NAME_API.sql --database $DATABASE_NAME_API
gcloud sql export sql $INSTANCE_NAME $BACKUP_BUCKET/$DATABASE_NAME_DOTCOM.sql --database $DATABASE_NAME_DOTCOM

# Step 2: Compress the database dump backups
echo "Compressing MySQL dump backups..."
gzip -f $PROCESSING_DIR/$DATABASE_NAME_API.sql -c > $PROCESSING_DIR/$DATABASE_NAME_API.sql.gz
gzip -f $PROCESSING_DIR/$DATABASE_NAME_DOTCOM.sql -c > $PROCESSING_DIR/$DATABASE_NAME_DOTCOM.sql.gz

# Step 3: Export the compressed backups to a new GCP storage bucket
echo "Exporting compressed backups to a new GCP storage bucket..."
gsutil cp $PROCESSING_DIR/$DATABASE_NAME_API.sql.gz $NEW_BACKUP_BUCKET/$DATABASE_NAME_API.sql.gz
gsutil cp $PROCESSING_DIR/$DATABASE_NAME_DOTCOM.sql.gz $NEW_BACKUP_BUCKET/$DATABASE_NAME_DOTCOM.sql.gz

# Cleanup: Remove temporary files
echo "Cleaning up temporary files..."
rm $PROCESSING_DIR/$DATABASE_NAME_API.sql
rm $PROCESSING_DIR/$DATABASE_NAME_DOTCOM.sql
rm $PROCESSING_DIR/$DATABASE_NAME_API.sql.gz
rm $PROCESSING_DIR/$DATABASE_NAME_DOTCOM.sql.gz

echo "Backup process completed successfully."
