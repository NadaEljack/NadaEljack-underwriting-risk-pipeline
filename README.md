# Underwriting Risk Insight Pipeline
![CI](https://github.com/NadaEljack/underwriting-risk-pipeline/actions/workflows/run-scripts.yml/badge.svg)

## Objective
Build a serverless pipeline that ingests, processes, and visualizes two open datasets to generate underwriting risk insights for **automobiles** line of business.

## Datasets Used
- Traffic Accident Casualties (ingested via API to BigQuery)  
- Rainfall Averages (ingested via API to BigQuery)  

## Architecture Overview
API → Ingestion (Python) → BigQuery → Risk Score Computation → Visualization (Matplotlib)

## Project Structure


## How to Run

1. Install dependencies:  
   ```bash
   pip install -r requirements.txt

   python ingestion/ingest_traffic.py
   python ingestion/ingest_rainfall.py
   python analysis/compute_risk_score.py
   python visualization/plot_dashboard.py


[Rainfall Dataset]         [Traffic Accidents Dataset]
        ↓                           ↓
 ┌─────────────────────────────────────────┐
 │       Notebook:ingestion.ipynb         │
 └─────────────────────────────────────────┘
        ↓
┌──────────────────────────────────────────┐
│ Notebook: Analysis.ipynb       │
└──────────────────────────────────────────┘
        ↓
 ┌─────────────────────────────────────────┐
 │    Notebook: risk score.ipynb          │
 └─────────────────────────────────────────┘
        ↓
 ┌─────────────────────────────────────────┐
 │  Notebook: Visualization.ipynb          │
 └─────────────────────────────────────────┘
        ↓
        📊 Output: Risk Scores & Charts

## 🧠 Challenges & Learnings

- Datasets had inconsistent formats, especially with date fields and null values.
- Rainfall data was sparse and required interpolation.
- Aligning datasets by region and time required extra cleaning steps.
- Learned how to build a modular pipeline using Python and BigQuery.
- Gained experience running code with GitHub Actions and structuring cloud-friendly pipelines.


## 🚀 Next Steps & Enhancements

- Integrate the third dataset (Real Estate Prices) to enhance risk granularity.
- Add a front-end dashboard (e.g., Streamlit or Dash) for stakeholder access.
- Schedule automated ingestion using Cloud Scheduler + Pub/Sub or Airflow.
- Expand risk scoring logic to support machine learning for predictive modeling.
- Enable alerting (email or Slack) for high-risk regions using risk thresholds.




