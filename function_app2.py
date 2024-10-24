import azure.functions as func
import os
import logging
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

app = func.FunctionApp()


@app.blob_trigger(arg_name="myblob", path="files/{name}",
                               connection="sourcetesticle_STORAGE") 
def BlobTrigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")

    # Access the source blob (No need to prepend the 'files' container again)
    blob = myblob.name.replace("files/", "")
    logging.info(f"MyBlob: {myblob.name}, BLOB: {blob}")
    # Initialize DefaultAzureCredential to authenticate Managed Identity
    credential = DefaultAzureCredential()

    # Set up BlobServiceClient for both source and destination storage accounts
    source_blob_service_client = BlobServiceClient(account_url=os.environ['SOURCE_STORAGE_ACCOUNT_URL'], credential=credential)
    destination_blob_service_client = BlobServiceClient(account_url=os.environ['DESTINATION_STORAGE_ACCOUNT_URL'], credential=credential)
    
    source_blob_client = source_blob_service_client.get_blob_client(container="files", blob=blob)
    
    # Read the source blob data
    blob_data = source_blob_client.download_blob().readall()
    
    # Set up the destination container and blob client
    destination_container_client = destination_blob_service_client.get_container_client("dest")
    destination_blob_client = destination_container_client.get_blob_client(blob)
    
    try:
        # Download the blob data from the source
        blob_data = source_blob_client.download_blob().readall()

        # Upload the blob to the destination
        destination_blob_client.upload_blob(blob_data)
        logging.info(f"Successfully uploaded blob to destination: {blob}")

        # Delete the source blob after successful upload
        source_blob_client.delete_blob()
        logging.info(f"Deleted source blob: {blob}")

    except Exception as e:
        logging.error(f"Error during copy or delete operation: {e}")
