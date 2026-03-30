# To be run weekly, on Tues. Pulls data from CMS api.
import pandas as pd
import polars as pl
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import table
import s3fs
import requests
from urllib.request import urlopen
import json
from pandas.tseries.offsets import Week
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta, MO, SU
import time
import boto3
import argparse
import os

url = "https://data.cms.gov/data.json"
title = "COVID-19 Nursing Home Data"

def get_endpoint():
    response = requests.request("GET",url)
    if response.ok:
        response = response.json()
        dataset = response['dataset']
    for set in dataset:
        if title ==set['title']:
            for distro in set['distribution']:
                if 'format' in distro.keys() and 'description' in distro.keys():
                    if distro['format'] == "API" and distro['description'] == "latest":
                        latest_distro = distro['accessURL']
                        print(f"The latest data for {title} can be found at {latest_distro} or {set['identifier']}")
                        return latest_distro

def extract_raw_cms_data(cms_endpoint):
    latest_distro = cms_endpoint
    stats_endpoint = latest_distro + "/stats"
    print(f"stats endpoint: {stats_endpoint}")

    cms_raw_data = []
    stats_response = requests.request("GET", stats_endpoint).json()
    total_rows = stats_response['total_rows']
    print(f"total rows: {total_rows}")

    date_today = date.today()
    end_wk_end_date = date_today - relativedelta(weeks=1, weekday=SU)
    start_wk_end_date = end_wk_end_date - relativedelta(weeks=1, weekday=SU)
    week = timedelta(days=7)
    offset = 0
    size = 5000

    while start_wk_end_date <= end_wk_end_date:
        for offset in range(0,total_rows,size):
            offset_url = f"{latest_distro}?filter[week_ending]={start_wk_end_date}&offset={offset}&size={size}"
            offset_response = requests.request("GET", offset_url)
            data = offset_response.json()
            print(f"Made request for {size} results at offset {offset}")
            if len(data) == 0:
                    break
            cms_raw_data.extend(data)
            offset = offset + size

            time.sleep(3)
            print("---")
            current_url = f"{latest_distro}?filter[week_ending]={start_wk_end_date}&offset={offset}&size={size}"
            print("Requesting",current_url)

        start_wk_end_date = start_wk_end_date + week
    #save as df
    #df_latest_raw_data = pd.DataFrame(latest_raw_data)
    #save as json
    #json_latest_raw_data = json.dumps(latest_raw_data)
    #save as parquet file
    #pq_latest_raw_data = df_latest_raw_data.to_parquet("data_pre_proc/nh_pre_proc_raw.parquet", engine='auto', compression='snappy', index=None, partition_cols=None)
    return cms_raw_data

def extract_fips_data():
    # Initialize the S3 filesystem
    #s3 = s3fs.S3FileSystem()

    # Read the Parquet file into pl dataframe
    fips_codes_df = pl.read_parquet(
    's3://fips-codes-smw/fips-codes/data.parquet')
    return fips_codes_df


def transform_cms_data(cms_raw_data, fips_codes_df):
    #what is returned from extract_raw_data() is a list of dictionaries. This is very important!


# # If it's a dictionary, print the keys
#     if isinstance(raw_data, dict):
#         print("Keys in raw_data:", raw_data.keys())

#     # If it's a list, check if the first element is a dictionary, then print its keys
#     elif isinstance(raw_data, list) and len(raw_data) > 0:
#         if isinstance(raw_data[0], dict):
#             print("Keys in the first dictionary of raw_data:", raw_data[0].keys())
#         else:
#             print("The first element in raw_data is not a dictionary.")
#     else:
#         print("raw_data is neither a dictionary nor a list.")


    # cols to keep
    columns_to_keep = ['week_ending',
                    'federal_provider_number',
                    'provider_name',
                    'provider_address',
                    'provider_city',
                    'provider_state',
                    'provider_zip_code',
                    'provider_phone_number',
                    'county',
                    'submitted_data',
                    'passed_quality_assurance_check',
                    'residents_weekly_confirmed_covid_19',
                    'residents_total_confirmed_covid_19',
                    'residents_weekly_all_deaths',
                    'residents_total_all_deaths',
                    'residents_weekly_covid_19_deaths',
                    'residents_total_covid_19_deaths',
                    'number_of_all_beds',
                    'total_number_of_occupied_beds',
                    'staff_weekly_confirmed_covid_19',
                    'staff_total_confirmed_covid_19',
                    'number_of_residents_staying_in_this_facility_for_at_least_1_day_this_week',
                    'number_of_all_healthcare_personnel_eligible_to_work_in_this_facility_for_at_least_1_day_this_week',
                    'Number_of_Residents_Staying_in_this_Facility_for_At_Least_1_Day_This_Week_Up_to_Date_with_COVID_19_Vaccines',
                    'Number_of_Healthcare_Personnel_Eligible_to_Work_in_this_Facility_for_At_Least_1_Day_This_Week_Up_to_Date_with_COVID_19_Vaccines'
                    ]
    # filter dicts to keep only specified cols
    filtered_cms_data = [{key: row[key] for key in columns_to_keep if key in row} for row in cms_raw_data]

    cms_data_df = pl.DataFrame(filtered_cms_data)

# Display the Polars DataFrame

    #lr_df = pd.DataFrame(raw_data[0])
    #week_ending = data["week_ending"]

      #transformation inputs:
        #df_latest_raw_data
        #s3://fips-codes-smw/fips-codes/data.parquet
    # s3 = s3fs.S3FileSystem()
    # fips_codes_df = pq.ParquetDataset(
    # 's3://fips-codes-smw/fips-codes/data.parquet',
    # filesystem=s3).read_pandas().to_pandas()
    # print(fips_codes_df.head())
    #transformed_data = {"week_ending": week_ending}
    #transformed_data_list = [transformed_data]
    # df_data = pd.DataFrame(transformed_data_list)
    # Get rows where provider_state values from nh_pre_proc_df are not in fips_df

    # Filter rows where 'provider_state' is not in 'StateAbbr'
    nh_state_notin_fips = cms_data_df.filter(
    ~pl.col('provider_state').is_in(fips_codes_df.select('StateAbbr').to_series())
    )

    # Get unique values in the 'provider_state' column
    unique_states = nh_state_notin_fips.select('provider_state').unique()

    # Print or work with the unique states
    print(unique_states)



    #  # Remove 'Parish' from county names in LA and create the 'county_rev' column based on the 'provider_state'
    cms_data_df = cms_data_df.with_columns(
        pl.when(pl.col('provider_state') == 'LA')
        .then(pl.col('county').str.replace(r'(?i) parish$', '', literal=True))
        .otherwise(pl.col('county'))
        .alias('county_rev')
    )

    # create new col, state_county
    cms_data_df = cms_data_df.with_columns(
    (
    pl.col("provider_state").fill_null('') + pl.col("county_rev").fill_null(''))
    .str.to_lowercase()
    .alias("state_county")
    )

    # Perform left join
    cms_data_df_jn = cms_data_df.join( fips_codes_df,on="state_county",how="left")
    print(cms_data_df_jn.columns)

    # how many fips are null?
    null_fips = cms_data_df_jn.filter(pl.col("CountyFIPS").is_null())
    print(null_fips)
    print(len(null_fips))

# Print the merged DataFrame
    # print(cms_data_df.head())
    # print(len(cms_data_df))

    # print(cms_data_df_jn.head())
    # print(len(cms_data_df_jn))


def pipeline():
    cms_endpoint = get_endpoint()
    cms_raw_data = extract_raw_cms_data(cms_endpoint)
    fips_codes_df = extract_fips_data()
    transform_cms_data(cms_raw_data, fips_codes_df)

if __name__ == '__main__':
    pipeline()
#save as csv file
#df_latest_data.to_csv("data_pre_proc/nh_pre_proc_raw.csv", index=False)

