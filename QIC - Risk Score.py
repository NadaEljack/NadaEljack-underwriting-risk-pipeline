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


# ## 2- Risk Score:
from google.cloud import bigquery

# Query Traffic Accident Data
query_traffic = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET_ID}.traffic_accidents`
"""
traffic_df = bq_client.query(query_traffic).to_dataframe()
print("🚗 Traffic data:")
print(traffic_df.head())

# In[1]:


get_ipython().system('pip install --user db-dtypes')


# In[16]:


# === Step 1: Fetch traffic_accidents data from BigQuery ===
query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    LIMIT 1000
"""

traffic_df = bq_client.query(query).to_dataframe()
print(f"Fetched {len(traffic_df)} rows from traffic_accidents table.")
traffic_df.head()


# In[15]:


traffic_df.head()
traffic_df.info()


# In[12]:


print(traffic_df.columns.tolist())


# In[19]:


from google.cloud import bigquery

# Make sure you're authenticated with the correct credentials
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# ✅ Correct query for traffic accidents table
query = """
SELECT year, result_of_the_accident, number_of_people, result_of_the_accident_ar
FROM `stoked-mapper-466516-p4.underwriting_risk.traffic_accidents`
"""

traffic_df = bq_client.query(query).to_dataframe()
print(f"Fetched {len(traffic_df)} rows from traffic_accidents table.")
traffic_df.head()


# In[22]:


query = """
SELECT station, `2016`, `2017`, `2018`, `2019`, `2020`, `2021`
FROM `stoked-mapper-466516-p4.underwriting_risk.rainfall`
"""

rainfall_df = bq_client.query(query).to_dataframe()
print(f"Fetched {len(rainfall_df)} rows from rainfall table.")
rainfall_df.head()


# In[ ]:


normalize rainfall data:


# In[23]:


# Melt rainfall table: years to rows
rainfall_melted = rainfall_df.melt(id_vars=['station'], 
                                    var_name='year', 
                                    value_name='rainfall_mm')

# Convert year to integer (optional for BigQuery joins later)
rainfall_melted['year'] = rainfall_melted['year'].astype(int)

print(rainfall_melted.head())


# Pivot traffic data to show raw per year

# In[24]:


traffic_pivot = traffic_df.pivot_table(
    index='year',
    columns='result_of_the_accident',
    values='number_of_people',
    aggfunc='sum'
).reset_index()
 
traffic_pivot.columns.name = None
traffic_pivot.head()


# In[25]:


rainfall_melted = rainfall_df.melt(id_vars=["station"], var_name="year", value_name="rainfall_mm")
rainfall_melted["year"] = rainfall_melted["year"].astype(int)

# Optionally, average rainfall per year across stations:
rainfall_avg = rainfall_melted.groupby("year")["rainfall_mm"].mean().reset_index()
rainfall_avg.head()


# ### Join the 2 datasets:

# In[27]:


import pandas as pd
combined_df = pd.merge(traffic_pivot, rainfall_avg, on='year', how='inner')
combined_df.head()


# ### Compute a basic composite risk score
# 

# In[41]:


# Normalize (simple scaling)
combined_df["injury_score"] = combined_df["Injury"] / combined_df["Injury"].max()
combined_df["death_score"] = combined_df["Death"] / combined_df["Death"].max()
combined_df["rainfall_score"] = combined_df["rainfall_mm"] / combined_df["rainfall_mm"].max()

# Composite score (you can change weights)
combined_df["risk_score"] = (
    0.5 * combined_df["injury_score"] +
    0.3 * combined_df["death_score"] +
    0.2 * combined_df["rainfall_score"]
)

combined_df = combined_df.round(3)
combined_df.head()


# In[31]:


# Flatten MultiIndex columns if necessary
traffic_pivot.columns = [col if isinstance(col, str) else col[-1] for col in traffic_pivot.columns]
print(traffic_pivot.columns)


# In[34]:


print(combined_df.columns)


# In[35]:


# If not already done:
traffic_pivot = traffic_pivot.fillna(0)

# Normalize columns (min-max scaling)
traffic_pivot["death_score"] = traffic_pivot["Death"] / traffic_pivot["Death"].max()
traffic_pivot["severe_injury_score"] = traffic_pivot["Severe Injury"] / traffic_pivot["Severe Injury"].max()
traffic_pivot["slight_injury_score"] = traffic_pivot["Slight Injury"] / traffic_pivot["Slight Injury"].max()

# Composite risk score with custom weights
traffic_pivot["traffic_risk_score"] = (
    traffic_pivot["death_score"] * 0.5 +
    traffic_pivot["severe_injury_score"] * 0.3 +
    traffic_pivot["slight_injury_score"] * 0.2
)

# View result
traffic_pivot.head()


# In[37]:


# Convert wide format to long format: station, year, rainfall_mm
rainfall_long = rainfall_df.melt(id_vars=["station"], var_name="year", value_name="rainfall_mm")
rainfall_long["year"] = rainfall_long["year"].astype(int)

# Group by year (averaging across stations)
rainfall_grouped = rainfall_long.groupby("year", as_index=False)["rainfall_mm"].mean()


# In[ ]:





# In[39]:


rainfall_grouped


# In[42]:


rainfall_grouped["rainfall_score"] = rainfall_grouped["rainfall_mm"] / rainfall_grouped["rainfall_mm"].max()


# In[43]:


combined_df = pd.merge(traffic_pivot, rainfall_grouped, on="year", how="inner")


# In[44]:


combined_df.head()


# In[ ]:


### composite risk score


# In[45]:


# Normalize traffic injury and death scores
combined_df["death_score"] = combined_df["Death"] / combined_df["Death"].max()
combined_df["severe_injury_score"] = combined_df["Severe Injury"] / combined_df["Severe Injury"].max()
combined_df["slight_injury_score"] = combined_df["Slight Injury"] / combined_df["Slight Injury"].max()

# Normalize rainfall
combined_df["rainfall_score"] = combined_df["rainfall_mm"] / combined_df["rainfall_mm"].max()

# Compute composite traffic risk score (weighted sum example)
combined_df["traffic_risk_score"] = (
    combined_df["death_score"] * 0.5 + 
    combined_df["severe_injury_score"] * 0.3 + 
    combined_df["slight_injury_score"] * 0.2
)

# Compute overall risk score including rainfall (you can adjust weights)
combined_df["overall_risk_score"] = (
    combined_df["traffic_risk_score"] * 0.7 + 
    combined_df["rainfall_score"] * 0.3
)

print(combined_df[["year", "overall_risk_score"]])


# ### Visaisation: 
# 
# ### risk over Time:

# In[51]:


get_ipython().system('pip install matplotlib seaborn')


# In[52]:


import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(10, 6))
sns.lineplot(data=combined_df, x='year', y='traffic_risk_score', marker='o')
plt.title('Traffic Risk Score Over Time')
plt.ylabel('Risk Score (Normalized)')
plt.xlabel('Year')
plt.grid(True)
plt.tight_layout()
plt.show()


# ### Rainfall vs Traffic Risk score

# In[53]:


plt.figure(figsize=(10, 6))
sns.scatterplot(data=combined_df, x='rainfall_mm', y='traffic_risk_score', hue='year', palette='viridis', s=100)
plt.title('Rainfall vs. Traffic Risk Score')
plt.xlabel('Rainfall (mm)')
plt.ylabel('Traffic Risk Score')
plt.grid(True)
plt.tight_layout()
plt.show()


# ### Casulties per year

# In[ ]:


plt.figure(figsize=(12, 7))
combined_df.set_index('year')[['Death', 'Severe Injury', 'Slight Injury']].plot(
    kind='bar', stacked=True, colormap='Set2', figsize=(12, 7)
)
plt.title('Casualties by Type Over the Years')
plt.ylabel('Number of People')
plt.xlabel('Year')
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()


# ### Composite RIsk Score Over Time: 

# In[54]:


import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(10, 5))
sns.lineplot(data=combined_df, x="year", y="traffic_risk_score", marker="o", label="Traffic Risk")
sns.lineplot(data=combined_df, x="year", y="rainfall_score", marker="o", label="Rainfall Score")
sns.lineplot(data=combined_df, x="year", y="composite_risk_score", marker="o", label="Composite Risk")

plt.title("Composite and Component Risk Scores Over Time")
plt.xlabel("Year")
plt.ylabel("Normalized Risk Score")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()


# In[ ]:




