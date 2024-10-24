# azfuncblob

# Auto Blob Transfer
- Source Storage Account
- Destination Storage Account
- Managed Identity
- Assigned Managed Identity access to source and destination storage account
- Create Function App
- Create Function
- Create Event Grid subscription and point to function
- Add destination address environment variable
- pip install must be ran from within script?

## Expanding
- Event Grid Subscription per SA
- Assign Managed Identity Blob 

## Thoughts
- Managed Identities doesn't allow for server side file transfers
- Server side file transfers require SAS tokens not managed identity
- For Private Function Endpoint either premum or beta consumption plan is needed
- Not seeing an option for centralized or private endpoint event grid for Storage Accounts
