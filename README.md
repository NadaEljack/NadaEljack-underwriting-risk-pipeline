# Underwriting Risk Insight Pipeline

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





