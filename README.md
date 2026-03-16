# Fraud Detection Data Pipeline

This project implements a data pipeline to identify potential fraud cases
based on operational inspection and consumption datasets.

## Project Structure

src/ → pipeline code  
data/ → raw datasets  
output/ → generated results  
notebooks/ → exploratory analysis  

## Technologies

Python  
Pandas  
Parquet  
Data Engineering Pipelines  

## Pipeline Steps

1. Load operational datasets
2. Feature engineering for meter data
3. Inspection history filtering
4. Data integration
5. Business rule filtering
6. Export prioritized fraud cases
## Pipeline Architecture

The project implements a data pipeline to identify potential fraud cases from operational datasets.

                 Raw Datasets
        (targets, pde, supply, fraud,
         inspection, meter, refusal)
                        │
                        ▼
              Data Processing Layer
       - meter feature engineering
       - inspection history filtering
                        │
                        ▼
               Data Integration Layer
           merge operational datasets
                        │
                        ▼
               Business Rules Layer
         strategy validation and filters
                        │
                        ▼
                 Output Generation
        prioritized fraud investigation
