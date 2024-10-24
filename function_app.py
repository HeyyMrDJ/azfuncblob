import azure.functions as func
import os
import logging
import json
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

app = func.FunctionApp()


def process_blob(source_url, container_name, blob_name):
    logging.info(f"Processing blob: {blob_name} from {container_name}")

    try:
        # Initial Azure Default Credential (Managed Identity)
        credential = DefaultAzureCredential()

        # Initialize source and destination clients
        source_blob_service_client = BlobServiceClient(
            account_url=source_url, credential=credential
        )
        destination_blob_service_client = BlobServiceClient(
            account_url=os.environ['DESTINATION_STORAGE_ACCOUNT_URL'], credential=credential
        )

        # Download blob from the source
        source_blob_client = source_blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_data = source_blob_client.download_blob().readall()

        # Upload the blob to the destination
        destination_blob_client = destination_blob_service_client.get_blob_client("dest", blob_name)
        destination_blob_client.upload_blob(blob_data)

        logging.info(f"Successfully uploaded blob: {blob_name}")

        # Delete the source blob only if the upload succeeded
        source_blob_client.delete_blob()
        logging.info(f"Deleted source blob: {blob_name}")

    except Exception as e:
        logging.error(f"Error processing blob: {e}")


@app.function_name(name="BlobeventGridTrigger")
@app.event_grid_trigger(arg_name="event")
def eventGridTest(event: func.EventGridEvent):
    logging.info("Processing Event Grid blob event.")

    # Extract the event data directly
    event_data = event.get_json()
    
    # Get the blob URL, container name, and blob name from the event
    url = event_data['url']  # This contains the full blob URL
    new_url = url.split("/", 4)
    source_url = "https://" + new_url[2]
    container_name = new_url[3]
    blob_name = new_url[4]

    logging.info(f"URL: {url}")
    logging.info(f"Source URL: {source_url}, Container Name: {container_name}, Blob Name: {blob_name}")
    process_blob(source_url, container_name, blob_name)
