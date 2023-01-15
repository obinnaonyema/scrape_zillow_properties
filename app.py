# %%
import pandas as pd
import pyodbc
import requests
import json
import time
import io
from datetime import datetime
import creds

# %%
import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

KVUri = f"https://keyvaultproperty.vault.azure.net"

credential = DefaultAzureCredential(additionally_allowed_tenants=['*'])
client = SecretClient(vault_url=KVUri, credential=credential)


# %%
url = "https://zillow-com1.p.rapidapi.com/propertyExtendedSearch"



headers = {
	"X-RapidAPI-Key": client.get_secret("X-RapidAPI-Key").value,
	"X-RapidAPI-Host": client.get_secret("X-RapidAPI-Host").value
}

current_page=1
querystring = {"location":"brampton, on","home_type":"Houses","page":current_page}

data = []
response = requests.request("GET", url, headers=headers, params=querystring)
data.extend(response.json()['props'])

#fetch all pages, if there are multiple pages
while current_page<=response.json()["totalPages"]:
	data.extend(response.json()['props'])
	print(current_page,end='-')
	current_page+=1

print('\n\n Completed extraction. Final data shape: {}'.format(len(data)))



# %%
#connect to data lake storage account
from azure.storage.blob import BlockBlobService
blobService = BlockBlobService(account_name='propertystorage89768', account_key=client.get_secret("storagekey").value)


# %%
#write into data lake
blobService.create_blob_from_text(container_name='extracted',\
    blob_name=f'raw\extracted_{datetime.today().strftime("%d%m%Y")}.json',\
    text=json.dumps(data))

# %%



