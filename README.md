# Nursing Home CMS Data Pipeline - A Very Covid Era Project
This project automates the extraction, transformation, and enrichment of COVID-19 Nursing Home data from the Centers for Medicare & Medicaid Services (CMS). It is designed to be run weekly to keep datasets up to date for analysis and reporting.

### Overview
* Data Source: Pulls the latest COVID-19 Nursing Home data from the CMS API.
* Processing: Extracts relevant columns, normalizes state/county names, and joins with FIPS codes for geographic analysis.
* Output: Produces cleaned and enriched datasets for further analysis or visualization.

### Main Scripts
* extract_cms_data.py: Main pipeline script for downloading, transforming, and joining CMS data with FIPS codes.
* transform_cms_data.py: Additional transformation utilities (see script for details).


### Cloud Infrastructure (Historical - creds/infra no longer active)
This project previously used Amazon EC2 for scheduled data extraction and processing.

### Usage
The main pipeline ran from:

python extract_cms_data.py

### Resources on CMS website
[COVID-19 Nursing Home Data info page - cms.gov](https://data.cms.gov/covid-19/covid-19-nursing-home-data)

[COVID-19 Nursing Home Methodology - cms.gov](https://data.cms.gov/resources/covid-19-nursing-home-methodology)

[NH Provider Information (qual scores, staffing turnover, rating variables) - cms.gov](https://data.cms.gov/provider-data/dataset/4pq5-n9py)

[NH data collection intervals - to supplement provider info above](https://data.cms.gov/provider-data/dataset/qmdc-9999)

[COVID-19 Nursing Home Data Dictionary - cms.gov](https://data.cms.gov/sites/default/files/2022-06/COVID-19%20Nursing%20Home%20Data%20Dictionary.pdf)


### Shapefile data
[Nursing Home shapefile](https://hifld-geoplatform.opendata.arcgis.com/datasets/geoplatform::nursing-homes/about)
