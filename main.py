import os
import logging
import sys
from typing import List
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError, AzureError

source_url = os.environ['SOURCE_URL']
source_access_key = os.environ['SOURCE_ACCESS_KEY']
source_container_name = os.environ['SOURCE_CONTAINER_NAME']

destination_url = os.environ['DESTINATION_URL']
destination_access_key = os.environ['DESTINATION_ACCESS_KEY']
destination_container_name = os.environ['DESTINATION_CONTAINER_NAME']

# Create a custom logger
logger = logging.getLogger(__name__)  # Use module's name for better traceability

# Configure the logger
handler = logging.StreamHandler(sys.stdout)  # Send logs to STDOUT
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)  # Add the handler to the logger
logger.setLevel(logging.INFO)  # Set the minimum log level


def move_blobs(blobs: List) -> None:
    """Inputs a list of blobs and moves them from their source storage account container to the destination storage account container"""
    try:
        # Create BlobServiceClient for source and destination
        source_blob_service_client = BlobServiceClient(account_url=source_url, credential=source_access_key)
        destination_blob_service_client = BlobServiceClient(account_url=destination_url, credential=destination_access_key)

        # Get the container clients for source and destination
        source_container_client = source_blob_service_client.get_container_client(source_container_name)
        destination_container_client = destination_blob_service_client.get_container_client(destination_container_name)

        # List and move blobs from source to destination
        for blob in blobs:
            try:
                logger.info(f"Moving blob: {blob.name}")
                # Get the source blob client
                source_blob_client = source_container_client.get_blob_client(blob.name)

                # Download the blob content
                blob_data = source_blob_client.download_blob().readall()

                # Get the destination blob client
                destination_blob_client = destination_container_client.get_blob_client(blob.name)

                # Upload the blob to the destination
                destination_blob_client.upload_blob(blob_data) 
                logger.info(f"Successfully uploaded blob to destination: {blob.name}")

                # Delete the source blob
                source_blob_client.delete_blob()
                logger.info(f"Deleted source blob: {blob.name}")

            except ResourceExistsError:
                logger.warning(f"Blob {blob.name} already exists in the destination. Skipping")
            except AzureError as e:
                logger.error(f"Azure error occurred while moving blob {blob.name}: {e}")
            except Exception as e:
                logger.error(f"An unexpected error occurred while processing blob {blob.name}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while processing blobs : {e}")

    logger.info("Blob transfer completed.")

def get_blobs() -> List:
    """Get blobs from source storage account and return as a list"""
    logger.info(f"Checking {source_url}/{source_container_name} for blobs")

    try:
        # Create BlobServiceClient for source
        source_blob_service_client = BlobServiceClient(account_url=source_url, credential=source_access_key)

        # Get the container client for source
        source_container_client = source_blob_service_client.get_container_client(source_container_name)

        blobs = []
        # List blobs in the container
        for blob in source_container_client.list_blobs():
            logger.info(f"Found blob {blob.name}")
            blobs.append(blob)
        return blobs
    except AzureError as e:
        logger.error(f"Error occurred while listing blobs: {e}")
        return []


def main():
    if not all([source_url, source_access_key, source_container_name, destination_url, destination_access_key, destination_container_name]):
        logger.error("One or more required environment variables are not set.")
        return

    blobs = get_blobs()
    if blobs:
        move_blobs(blobs)
    else:
        logger.info("No blobs found to process")


if __name__ == "__main__":
    main()
