import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import missingno as msno
from collections import Counter 
import re
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import os


# Clean the data
def clean_data(df):
    df['Ratings'] = df['Ratings'].str.replace('out of 5 stars', '')
    df['Ratings'] = pd.to_numeric(df['Ratings'], errors='coerce')
    df['Ratings'] = df['Ratings'].fillna(df['Ratings'].median())

    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')  # Convert to numeric
    df['Price'] = df['Price'].fillna(df['Price'].median())

    #replace missing value of categorical data with the MODE¶
    mode_Qty_Sold = df['Qty Sold'].mode()[0]
    df['Qty Sold'] = df['Qty Sold'].fillna(mode_Qty_Sold)

    def process_qty_sold(value):
        value = str(value)
        if value.lower() == 'nan' or value == '':
            return value
        numeric_part = re.sub('[^0-9.k]', '', value)
        if numeric_part == '':
            return value
        if 'k' in value.lower():
            numeric_value = float(numeric_part.replace('k', '')) * 1000
            numeric_value = int(numeric_value)
        else:
            numeric_value = int(float(numeric_part))
        if '+' in value:
            return f"{numeric_value:,}"
        else:
            return f"{numeric_value:,}"
            
    # Apply the conversion to the 'Qty Sold' column
    df['Qty Sold'] = df['Qty Sold'].apply(process_qty_sold)
    df['Qty Sold'] = pd.to_numeric(df['Qty Sold'], errors='coerce')
    df['Qty Sold'] = df['Qty Sold'].fillna(mode_Qty_Sold) # ensures replacement of NA wiith the mode

    # remove trailing spaces
    df.columns = df.columns.str.strip()
    
    df['Ratings'] = df['Ratings'].astype(str).str.strip()

     # Force Ratings back to numeric
    df['Ratings'] = pd.to_numeric(df['Ratings'], errors='coerce') 
    

    def Price_cat(Price):
        if Price <=20:
            return "$0-20"
        elif Price <= 40:
            return "$21-40"
        elif Price <= 60:
            return "$41-60"
        elif Price <= 80:
            return "$61-80"
        elif Price <= 100:
            return "$81-100"
        else:
            return "$100+"
    df["Price_cat"] = df["Price"].apply(Price_cat)

    # Extract the month and year from the date column
    df['coll_date'] = pd.to_datetime(df['coll_date'])
    df['Month'] = df['coll_date'].dt.strftime('%B') # displays the Month's name
    df['Year'] = df['coll_date'].dt.year
   

    # Remove duplicates
    df.drop_duplicates(inplace=True)
    
    return df

# Append data to Google BigQuery
def append_to_bigquery(cleaned_data, table_id):
    # Load the service account key
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "google_cloud_credentials.json")
    
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project="amaz-project-438116")


    # Define schema to ensure 'coll_date' is interpreted as DATE in BigQuery
    schema = [
        bigquery.SchemaField("Product Name", "STRING"),
        bigquery.SchemaField("Product Category", "STRING"),
        bigquery.SchemaField("Ratings", "FLOAT"),
        bigquery.SchemaField("Price", "FLOAT"),
        bigquery.SchemaField("Qty Sold", "INTEGER"),
        bigquery.SchemaField("coll_date", "DATE"),  # Set coll_date as DATE
        bigquery.SchemaField("Rating_cat", "STRING"),
        bigquery.SchemaField("Price_cat", "STRING"),
        bigquery.SchemaField("Month", "STRING"),
        bigquery.SchemaField("Year", "INTEGER")
    ]

    # BigQuery job configuration to append data
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    # Load the data into the table
    job = client.load_table_from_dataframe(cleaned_data, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"Appended {cleaned_data.shape[0]} rows to {table_id}")

# Run the entire process
if __name__ == "__main__":
    from scraper import main_scrape # type: ignore

    new_data = main_scrape()


    if new_data is not None:
        print("DataFrame successfully created, cleaning data.")
        cleaned_data = clean_data(new_data)

        # BigQuery table ID:
        table_id = "amaz-project-438116.Existing_data.Sales"

        append_to_bigquery(cleaned_data, table_id)
        print("Process completed successfully!")
    else:
        print("No data was scraped.")
