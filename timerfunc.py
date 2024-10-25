import sys, subprocess;
subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
import azure.functions as func
import os
import logging
import json
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential

app = func.FunctionApp()

source_url = os.environ['SOURCE_STORAGE_ACCOUNT_URL']
destination_url = os.environ['DESTINATION_STORAGE_ACCOUNT_URL']
source_container = "files"
destination_container = "dest"

def move_blob(credential, blobs):
    """Move blobs from the source to the destination."""
    try:
        # Initialize source and destination clients
        source_blob_service_client = BlobServiceClient(
            account_url=source_url, credential=credential
        )
        destination_blob_service_client = BlobServiceClient(
            account_url=destination_url, credential=credential
        )
        container_client = source_blob_service_client.get_container_client(container=source_container)

        for blob in blobs:
            logging.info(f"Processing {len(blobs)} blobs")

            # Download blob from the source
            source_blob_client = source_blob_service_client.get_blob_client(container=source_container, blob=blob.name)
            blob_data = source_blob_client.download_blob().readall()

            # Upload the blob to the destination
            destination_blob_client = destination_blob_service_client.get_blob_client(destination_container, blob.name)
            destination_blob_client.upload_blob(blob.name)

            logging.info(f"Successfully uploaded blob: {blob.name}")

            # Delete the source blob only if the upload succeeded
            source_blob_client.delete_blob()
            logging.info(f"Deleted source blob: {blob.name}")
    except Exception as e:
        logging.error(f"Error processing blobs: {e}")
    

def grab_blobs(credential):
    """Retrieve a list of blobs from the source container."""
    try:
        # Initialize source client
        source_blob_service_client = BlobServiceClient(
            account_url=source_url, credential=credential
        )
        container_client = source_blob_service_client.get_container_client(container=source_container)
        blob_list = container_client.list_blobs()
        blobs = []
        for blob in blob_list:
            logging.info(f"Found blob {blob.name}")
            blobs.append(blob)
        
        return blobs
    except Exception as e:
        logging.error(f"Error checking for blobs: {e}")


@app.function_name(name="mytimer1")
@app.schedule(schedule="0 */1 * * * *", arg_name="mytimer", run_on_startup=True,
              use_monitor=False) 
def test_function(mytimer: func.TimerRequest) -> None:
    """Main function executed on schedule."""
    logging.info(f"TIMER EXECUTED")

    try:
        # Use Managed Identity Credential
        credential = ManagedIdentityCredential()
        
        blobs = grab_blobs(credential)
        if not blobs:
            logging.info("No blobs found")
        else:
            logging.info(f"{len(blobs)} blobs found")
            move_blob(credential, blobs)
    except Exception as e:
        logging.error(f"Error running function: {e}")
