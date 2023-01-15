# Databricks notebook source
# Databricks notebook source
adlsAccountName="propertystorage89768"
adlsContainerName="extracted"
#adlsFolderName="Data"
mountPoint="/mnt/Files/ADLS"

# Application (Client) ID
applicationId=dbutils.secrets.get(scope="propertyScope", key="clientID")

# Application (Client) secret key
authenticationKey=dbutils.secrets.get(scope="propertyScope",key="clientSecret")

# Directory (tenant) ID
tenantId=dbutils.secrets.get(scope="propertyScope", key="tenantID")

endpoint="https://login.microsoftonline.com/"+tenantId+"/oauth2/token"
source="abfss://"+adlsContainerName+"@"+adlsAccountName+".dfs.core.windows.net"


# COMMAND ----------

#Connecting using service principal secrets and OAuth
configs={"fs.azure.account.auth.type":"OAuth",
      "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id":applicationId,
        "fs.azure.account.oauth2.client.secret":authenticationKey,
        "fs.azure.account.oauth2.client.endpoint":endpoint}

# Mounting ADLS storage to DBFS
# Mount only if directory is not already mounted
if not any(mount.mountPoint==mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
    source=source,
    mount_point=mountPoint,
    extra_configs=configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


