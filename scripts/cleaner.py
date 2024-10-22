import pandas as pd
import re
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from scraper import main_scrape  # Assuming main_scrape function is defined in scraper.py

# Clean the data
def clean_data(df):
    df['Ratings'] = df['Ratings'].str.replace('out of 5 stars', '').astype(float)
    df['Ratings'] = df['Ratings'].fillna(df['Ratings'].median())

    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df['Price'] = df['Price'].fillna(df['Price'].median())

    # Replace missing values of categorical data with the mode
    mode_qty_sold = df['Qty Sold'].mode()[0]
    df['Qty Sold'] = df['Qty Sold'].fillna(mode_qty_sold)

    def process_qty_sold(value):
        if pd.isna(value) or value == '':
            return np.nan
        numeric_part = re.sub('[^0-9.k]', '', str(value))
        if numeric_part == '':
            return np.nan
        if 'k' in value.lower():
            numeric_value = float(numeric_part.replace('k', '')) * 1000
            return int(numeric_value)
        else:
            return int(float(numeric_part))
    
    # Apply the conversion to the 'Qty Sold' column
    df['Qty Sold'] = df['Qty Sold'].apply(process_qty_sold)

    # Remove trailing spaces and strip column names
    df.columns = df.columns.str.strip()

    # Ensure ratings are numeric
    df['Ratings'] = pd.to_numeric(df['Ratings'], errors='coerce').fillna(df['Ratings'].median())

    def price_cat(price):
        if price <= 20:
            return "$0-20"
        elif price <= 40:
            return "$21-40"
        elif price <= 60:
            return "$41-60"
        elif price <= 80:
            return "$61-80"
        elif price <= 100:
            return "$81-100"
        else:
            return "$100+"
    
    df["Price_cat"] = df["Price"].apply(price_cat)

    # Extract the month and year from the date column
    df['coll_date'] = pd.to_datetime(df['coll_date'], errors='coerce')
    df['Month'] = df['coll_date'].dt.strftime('%B')
    df['Year'] = df['coll_date'].dt.year

    # Remove duplicates
    df.drop_duplicates(inplace=True)
    
    return df

# Append data to Google BigQuery
def append_to_bigquery(cleaned_data, table_id):
    # Load the service account key
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    # Check if the credentials file exists
    if not os.path.exists(credentials_path):
        print(f"Credentials file not found: {credentials_path}")
        return

    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project="amaz-project-438116")

    # Define schema to ensure 'coll_date' is interpreted as DATE in BigQuery
    schema = [
        bigquery.SchemaField("Product Name", "STRING"),
        bigquery.SchemaField("Product Category", "STRING"),
        bigquery.SchemaField("Ratings", "FLOAT"),
        bigquery.SchemaField("Price", "FLOAT"),
        bigquery.SchemaField("Qty Sold", "INTEGER"),
        bigquery.SchemaField("coll_date", "DATE"),
        bigquery.SchemaField("Price_cat", "STRING"),
        bigquery.SchemaField("Month", "STRING"),
        bigquery.SchemaField("Year", "INTEGER")
    ]

    # BigQuery job configuration to append data
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema  # Ensure schema is defined
    )
    
    # Load the data into the table
    job = client.load_table_from_dataframe(cleaned_data, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"Appended {cleaned_data.shape[0]} rows to {table_id}")

# Run the entire process
if __name__ == "__main__":
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
