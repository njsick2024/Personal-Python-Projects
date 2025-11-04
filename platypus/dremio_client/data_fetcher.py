import os

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from platypus.utils.data_cleaning import clean_dataframe


class DremioDataFetcher:
    def __init__(self, username: str = None, password: str = None, endpoint: str = None, port: str = "32010"):

        self.username = os.getlogin()
        self.password = os.getenv("DREMIO_PASSWORD")
        self.endpoint = os.getenv("DREMIO_ENDPOINT")
        self.port = port
        self.headers = None
        self.client = None

        generic_options = []
        self.client = pa.flight.FlightClient(f"grpc+tcp://{self.endpoint}:{self.port}", generic_options=generic_options)
        token = self.client.authenticate_basic_token(self.username, self.password)
        self.options = pa.flight.FlightCallOptions(headers=[token])

    def fetch_chunk(self, query: str, chunk_size: int, full_output_path: str):
        descriptor = pa.flight.FlightDescriptor.for_command(query)
        flight_info = self.client.get_flight_info(descriptor, options=self.options)
        chunk_number = 0
        cumulative_rows = 0
        batch_buffer = []
        writer = None

        # Ensure the directory exists if saving to file
        if full_output_path:
            os.makedirs(os.path.dirname(full_output_path), exist_ok=True)

        try:
            for endpoint in flight_info.endpoints:
                ticket = endpoint.ticket
                reader = self.client.do_get(ticket, options=self.options)
                while True:
                    try:
                        batch, _ = reader.read_chunk()
                        batch_buffer.append(batch)
                        cumulative_rows += batch.num_rows

                        if cumulative_rows >= chunk_size:
                            table = pa.Table.from_batches(batch_buffer)
                            if writer is None and full_output_path:
                                writer = pq.ParquetWriter(full_output_path, table.schema)
                            if writer:
                                writer.write_table(table)
                            batch_buffer = []
                            cumulative_rows = 0
                            chunk_number += 1
                    except StopIteration:
                        break

            if batch_buffer:
                table = pa.Table.from_batches(batch_buffer)
                if writer is None and full_output_path:
                    writer = pq.ParquetWriter(full_output_path, table.schema)
                if writer:
                    writer.write_table(table)
        finally:
            if writer:
                writer.close()

    def fetch_and_clean_data(self, query: str, chunk_size: int, full_output_path: str, output_format: str):
        print("\nExecuting Query: ", query)
        if output_format == "polars":
            # Return as a Polars DataFrame without saving to file
            descriptor = pa.flight.FlightDescriptor.for_command(query)
            flight_info = self.client.get_flight_info(descriptor, options=self.options)
            batches = []
            for endpoint in flight_info.endpoints:
                ticket = endpoint.ticket
                reader = self.client.do_get(ticket, options=self.options)
                while True:
                    try:
                        batch, _ = reader.read_chunk()
                        batches.append(batch)
                    except StopIteration:
                        break
            table = pa.Table.from_batches(batches)
            df = pl.from_arrow(table)
            df = clean_dataframe(df)
            return df

        elif output_format == "dataframe":
            # Return as a Pandas DataFrame without saving to file
            descriptor = pa.flight.FlightDescriptor.for_command(query)
            flight_info = self.client.get_flight_info(descriptor, options=self.options)
            batches = []

            for endpoint in flight_info.endpoints:
                ticket = endpoint.ticket
                reader = self.client.do_get(ticket, options=self.options)
                while True:
                    try:
                        batch, _ = reader.read_chunk()
                        batches.append(batch)
                    except StopIteration:
                        break
            table = pa.Table.from_batches(batches)
            df = table.to_pandas()
            df = clean_dataframe(df)
            return df
        else:
            # Save the result to file and then read it back into a Polars DataFrame
            self.fetch_chunk(query, chunk_size, full_output_path)
            df = pl.read_parquet(full_output_path)
            df = clean_dataframe(df)
            df.write_parquet(full_output_path, compression="snappy", use_pyarrow=True)
            return df
