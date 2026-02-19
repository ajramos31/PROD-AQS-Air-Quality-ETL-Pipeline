# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1396535b-1d64-4192-9050-f56cca4d11c6",
# META       "default_lakehouse_name": "team2_LH",
# META       "default_lakehouse_workspace_id": "b58eabfa-e1c0-4347-9f2f-1f54092b9b89",
# META       "known_lakehouses": [
# META         {
# META           "id": "1396535b-1d64-4192-9050-f56cca4d11c6"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "0014c6a3-9bc2-bd6d-4db3-57d700f5a835",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import requests
from pyspark.sql.functions import col

years = range(2015, 2025)
results = []


url = 'https://api.census.gov/data/2024/acs/acs1?get=NAME,B01003_001E&for=state:*'
r = requests.get(url)
data = r.json()
headers = data[0]

df = spark.createDataFrame(data, headers)
df = df.withColumnRenamed('state', 'State_code')
df = df.withColumnRenamed('B01003_001E', 'Population')
df = df.withColumnRenamed('NAME', 'State')
df = df.where(col('State') != 'NAME')

df.write.format('delta').mode('overwrite').saveAsTable('state_population')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
