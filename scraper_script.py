# Databricks notebook source
#pip install azure-identity

# COMMAND ----------

# %%
import requests
import json
from datetime import datetime

# %%
import os
#from azure.keyvault.secrets import SecretClient
#from azure.identity import DefaultAzureCredential

#KVUri = f"https://keyvaultproperty.vault.azure.net"

credential = DefaultAzureCredential(additionally_allowed_tenants=['*'])
#client = SecretClient(vault_url=KVUri, credential=credential)


# %%
url = "https://zillow-com1.p.rapidapi.com/propertyExtendedSearch"



headers = {
	"X-RapidAPI-Key": dbutils.secrets.get(scope="propertyScope",key="X-RapidAPI-Key"),
	"X-RapidAPI-Host": dbutils.secrets.get(scope="propertyScope",key="X-RapidAPI-Host")
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



# COMMAND ----------



# # %%
# #connect to data lake storage account
# from azure.storage.blob import BlockBlobService
# blobService = BlockBlobService(account_name='propertystorage89768', account_key=client.get_secret("storagekey").value)


# # %%
# #write into data lake
# blobService.create_blob_from_text(container_name='extracted',\
#     blob_name=f'raw\extracted_{datetime.today().strftime("%d%m%Y")}.json',\
#     text=

# # %%





# COMMAND ----------

try:
    dbutils.fs.ls('mnt/Files/ADLS/raw/no')
    with open(f'/dbfs/mnt/Files/ADLS/raw/extracted_again{datetime.today().strftime("%d%m%Y")}.json', 'w') as f:
        f.write(json.dumps(data))
        scraping_status = dbutils.notebook.exit('Success')
except Exception as e:
    scraping_status = dbutils.notebook.exit('Failure')
    print(e)
    

# COMMAND ----------

    
