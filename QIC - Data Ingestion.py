#!/usr/bin/env python
# coding: utf-8

# # Underwriting Risk Analysis (QIC)

# ### 1- Data Ingestion to google bigquery

# #### Trafic Accidents

# In[3]:


import requests
from google.cloud import bigquery
from google.oauth2 import service_account

# === CONFIGURATION ===
API_URL = "https://www.data.gov.qa/api/explore/v2.1/catalog/datasets/number-of-deaths-and-injuries-from-traffic-accidents/records?limit=20"
PROJECT_ID = "stoked-mapper-466516-p4"
DATASET_ID = "underwriting_risk"
TABLE_ID = "traffic_accidents"
CREDENTIALS_PATH = r'C:\\Users\\nadae\\Downloads\\stoked-mapper-466516-p4-9acc589eabdb.json'

# === AUTHENTICATION ===
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# === FETCH DATA FROM API ===
def fetch_traffic_data():
    response = requests.get(API_URL)
    if response.status_code == 200:
        raw = response.json()
        print("Raw keys:", raw.keys())        # <-- Add this
        print("Sample result:", raw.get("results", [])[0])  # <-- And this to see a sample record
        return raw.get("results", [])
    else:
        print(f"❌ Failed to fetch: {response.status_code}")
        return []


data = fetch_traffic_data()
print(f"Records fetched: {len(data)}")
print(data[0] if data else "No data fetched")





# === UPLOAD TO BIGQUERY ===
def upload_to_bigquery(records):
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    schema = [
    bigquery.SchemaField("year", "INTEGER"),
    bigquery.SchemaField("result_of_the_accident", "STRING"),
    bigquery.SchemaField("number_of_people", "INTEGER"),
    bigquery.SchemaField("result_of_the_accident_ar", "STRING"),
]
    # Create table if it doesn't exist
   
    try:
        bq_client.get_table(table_ref)
    except Exception:
        print("Table not found. Creating table...")
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)

    # Upload rows
    errors = bq_client.insert_rows_json(table_ref, records)
    if errors:
        print("❌ Upload errors:", errors)
    else:
        print("✅ Upload successful")

# === MAIN ===
if __name__ == "__main__":
    data = fetch_traffic_data()
    if data:
        upload_to_bigquery(data)
        
        


# In[5]:


print(f"Fetched {len(data)} records")
print("Sample record:", data[0])


# #### Rain fall 

# In[6]:


import requests

RAIN_URL = "https://www.data.gov.qa/api/explore/v2.1/catalog/datasets/rainfall-average-mm-at-selected-monitoring-stations-in-qatar/records?limit=20"

response = requests.get(RAIN_URL)
if response.status_code == 200:
    raw = response.json()
    print("🔑 Keys:", raw.keys())
    print("📄 Sample record:", raw["results"][0])
    print("🔢 Total records fetched:", len(raw["results"]))
else:
    print("❌ Failed to fetch:", response.status_code)


# In[7]:


import requests
from google.cloud import bigquery
from google.oauth2 import service_account

# === CONFIGURATION ===
API_URL = "https://www.data.gov.qa/api/explore/v2.1/catalog/datasets/rainfall-average-mm-at-selected-monitoring-stations-in-qatar/records?limit=20"
PROJECT_ID = "stoked-mapper-466516-p4"
DATASET_ID = "underwriting_risk"
TABLE_ID = "rainfall"
CREDENTIALS_PATH = r'C:\\Users\\nadae\\Downloads\\stoked-mapper-466516-p4-9acc589eabdb.json'

# === AUTHENTICATION ===
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# === FETCH DATA ===
def fetch_rainfall_data():
    response = requests.get(API_URL)
    if response.status_code == 200:
        raw = response.json()
        return raw["results"]
    else:
        print(f"❌ Failed to fetch: {response.status_code}")
        return []

# === UPLOAD TO BIGQUERY ===
def upload_to_bigquery(records):
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    schema = [
        bigquery.SchemaField("station", "STRING"),
        bigquery.SchemaField("2016", "FLOAT"),
        bigquery.SchemaField("2017", "FLOAT"),
        bigquery.SchemaField("2018", "FLOAT"),
        bigquery.SchemaField("2019", "FLOAT"),
        bigquery.SchemaField("2020", "FLOAT"),
        bigquery.SchemaField("2021", "FLOAT"),
    ]

    try:
        bq_client.get_table(table_ref)
    except Exception:
        print("Table not found. Creating table...")
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)

    errors = bq_client.insert_rows_json(table_ref, records)
    if errors:
        print("❌ Upload errors:", errors)
    else:
        print("✅ Upload successful")

# === MAIN ===
if __name__ == "__main__":
    data = fetch_rainfall_data()
    if data:
        upload_to_bigquery(data)

