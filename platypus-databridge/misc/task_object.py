# %%
import concurrent.futures
import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from platypus.salesforce_client import SalesforceQueryExecutor
from platypus.utils import convert_csv_to_parquet


class SalesforceTaskFetcher:

    def __init__(self, csv_file="tasks.csv", parquet_file="tasks.parquet", batch_size=10000, days=7):
        self.csv_file = csv_file
        self.parquet_file = parquet_file
        self.batch_size = batch_size
        self.days = days
        self.max_workers = min(32, (os.cpu_count() or 1) * 2)  # Optimal thread count

    def fetch_tasks_for_date(self, query_date):

        query = f"""
            SELECT 
              AccountId
            , ActivityDate
            , CreatedById
            , CreatedDate
            , Description
            , LastModifiedById
            , LastModifiedDate
            , OwnerId
            , RecordTypeId
            , Relationship_Review
            , Status
            , Subject
            , Type
            , WhoCount
            , WhoId 
            FROM Task
            WHERE CreatedDate >= {query_date}T00:00:00Z
                AND CreatedDate < {query_date}T23:59:59Z
            ORDER BY CreatedDate DESC
         """

        return SalesforceQueryExecutor().execute_and_save(query, "task_test", "dataframe")

    @staticmethod
    def clean_text_column(df, column_name):
        """Cleans text columns by replacing characters and stripping spaces."""
        if column_name in df.columns:
            df[column_name] = (
                df[column_name]
                .astype(str)
                .str.replace(",", " ")
                .str.replace(r"[\r\n]+", " ", regex=True)  # Remove newlines and carriage returns
                .str.replace(r"[^a-zA-Z0-9\s.,!?]", "", regex=True)  # Keep only alphanumeric and common punctuation
                .str.strip()  # Remove leading/trailing spaces
            )

    def fetch_data_in_parallel(self, date_range):
        """Fetch data for multiple dates in parallel."""
        all_data = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_date = {executor.submit(self.fetch_tasks_for_date, date): date for date in date_range}

            for future in concurrent.futures.as_completed(future_to_date):
                date = future_to_date[future]
                try:
                    tasks = future.result()
                    if tasks:
                        df = pd.DataFrame(tasks)
                        self.clean_text_column(df, "StageName")
                        self.clean_text_column(df, "Sub_Line_of_Business")
                        all_data.append(df)
                        logging.info(f"Fetched {len(df)} records for {date}")
                except Exception as e:
                    logging.error(f"Error fetching data for {date}: {e}")

        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    def fetch_and_store_tasks(self):
        """Fetch Salesforce tasks for the past N days in parallel and store them in a Parquet file."""
        start_date = datetime.utcnow() - timedelta(days=self.days)
        date_range = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(self.days + 1)]

        file_exists = os.path.isfile(self.csv_file)

        for i in range(0, len(date_range), self.batch_size):
            batch_dates = date_range[i : i + self.batch_size]
            batch_data = self.fetch_data_in_parallel(batch_dates)

            if not batch_data.empty:
                batch_data.to_csv(self.csv_file, mode="a", header=not file_exists, index=False)
                batch_data.to_parquet(self.parquet_file, engine="pyarrow")  # Append to Parquet incrementally
                file_exists = True  # Ensure header is written only once

                logging.info(f"Batch {i//self.batch_size + 1} written with {len(batch_data)} records")

    def run(self):
        """Entry point to start fetching and storing Salesforce tasks."""
        self.fetch_and_store_tasks()


if __name__ == "__main__":
    fetcher = SalesforceTaskFetcher()
    fetcher.run()

# %%
