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
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### API Call

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import requests
import pandas as pd
 
# -----------------------------
# API configuration
# -----------------------------
BASE_URL = "https://aqs.epa.gov/data/api/dailyData/byState"
DAILY_URL = 'https://aqs.epa.gov/data/api/dailyData/byState?email=test@aqs.api&key=test&param=45201&bdate=19950515&edate=19950515&state=37'

# 88101 - PM 2.5, 44201 - Ozone, 42602 - Nitrogen dioxide, 42401 - Sulfur dioxide, 14129 - Lead, 42101 - Carbon monoxide, 81102 - PM10, 85129 - Lead PM10
# 45201 - Benezene
params = {
    "email": "alfred.ramos3519@outlook.com",   # replace with your email
    "key": "bayhawk89",                        # replace with your API key
    "param": "45201",                          # Different Pollutants above
    "bdate": "20240515",                       # Begin date
    "edate": "20240629",                       # End date
    "state": "17"                              # State Code
}
 
# -----------------------------
# Call API
# -----------------------------
response = requests.get(BASE_URL, params=params)
response.raise_for_status()   # fail fast if request breaks
 
data = response.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = pd.DataFrame(data["Data"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# #### Pollutant: Benzene

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Pollutant: PM 2.5

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
