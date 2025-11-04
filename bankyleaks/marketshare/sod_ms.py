# %%
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd
import yaml
from tqdm import tqdm

# Get the absolute path to the project root directory
project_root = Path(__file__).resolve().parent.parent
metadata_path = project_root / 'metadata' / 'sod_properties.yaml'

# Add project root to Python path
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from api.api_client import APIClient

class SOD:
    def __init__(self, client: APIClient) -> None:
        """
        Initialize the SOD class with an API client.
        """
        self.client = client
        self.sod_fields = [
            "YEAR", "CERT", "BKCLASS", "CITY", "CLCODE", 
            "CNTYNAMB", "CNTYNUMB", "DEPDOM", "DEPSUM", 
            "DEPSUMBR", "STNAME", "STNAMEBR", "STNUMBR", 
            "UNINUMBR", "ZIP_RAW", "USA"
        ]
        
    def _load_sod_fields(self, selected_fields: List[str]) -> str:
        try:
            with open(metadata_path, 'r') as file:
                data = yaml.safe_load(file)
                available_fields = data['properties']['data']['properties'].keys()
                print("Available fields in YAML:", available_fields)  # Debug statement
                valid_fields = [field for field in selected_fields if field in available_fields]
                if not valid_fields:
                    raise ValueError("No valid fields found in YAML file")
                return ','.join(valid_fields)
        except FileNotFoundError:
            print(f"YAML file not found at: {metadata_path}")
            print(f"Current working directory: {os.getcwd()}")
            print(f"Project root: {project_root}")
            raise

    def get_sod_data_for_year(
        self,
        year: int,
        limit: int = 10000,
        offset: int = 0,
        sort_order: str = "DESC"
    ) -> pd.DataFrame:
        """
        Retrieve SOD data for a specific year, filtered by specific states and a single CERT number.
        """
        all_data = []
        total_records = None
        current_offset = offset
        
        fields = self._load_sod_fields(self.sod_fields)
        
        # Define the states to filter in uppercase
        states_to_filter = ["TEXAS"]
        state_filter = ','.join(states_to_filter)
        
        # Define a single CERT number to filter
        cert_number = '(""983"", ""1596"")'  # Use a single CERT number
        
        # Combine filters using the correct syntax with quotes
        combined_filter = f'CERT:{(""983"",""1596"")} AND YEAR:{year} AND STNAME:{state_filter}'
        
        while True:
            try:
                params = {
                    "filters": combined_filter,
                    "fields": fields,
                    "sort_by": "YEAR",
                    "sort_order": sort_order,
                    "limit": limit,
                    "offset": current_offset,
                    "format": "json",
                    "download": False,
                    "filename": "data_file"
                }
                
                print("Filter string:", combined_filter)  # Debug print
                
                # Make the API request using parameters
                response = self.client.get("sod", params)
                
                if not response or 'data' not in response:
                    break
                
                # Debug: Print the fields in the response
                if 'data' in response and response['data']:
                    print("Fields in response:", response['data'][0].keys())
                
                if total_records is None and 'total' in response:
                    total_records = response['total']
                
                data_list = [item['data'] for item in response['data']]
                all_data.extend(data_list)
                
                if len(data_list) < limit:
                    break
                    
                current_offset += limit
                
            except Exception as e:
                print(f"Error fetching data for year {year}, offset {current_offset}: {str(e)}")
                break
        
        return pd.DataFrame(all_data) if all_data else pd.DataFrame()

    def get_historical_sod_data(self, start_year: int = None, end_year: int = None) -> pd.DataFrame:
        """
        Fetch SOD data for a range of years and return as DataFrame.
        """
        current_year = datetime.now().year
        start_year = start_year or current_year - 9
        end_year = end_year or current_year
        
        all_data = []
        years = range(start_year, end_year + 1)
        
        with tqdm(total=len(years), desc="Fetching SOD data") as pbar:
            for year in years:
                try:
                    df = self.get_sod_data_for_year(year)
                    if not df.empty:
                        all_data.append(df)
                        print(f"\nSuccessfully fetched data for year {year}")
                        print(f"Records retrieved: {len(df)}")
                except Exception as e:
                    print(f"\nError processing year {year}: {str(e)}")
                finally:
                    pbar.update(1)
        
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            print(f"\nTotal records retrieved: {len(final_df)}")
            
            # Display summary statistics
            print("\nSummary by Year:")
            year_summary = final_df.groupby('YEAR').agg({
                'CERT': 'count',
                'DEPSUM': ['sum', 'mean'],
                'DEPSUMBR': ['sum', 'mean']
            }).round(2)
            print(year_summary)
            
            return final_df
        else:
            print("\nNo data retrieved")
            return pd.DataFrame()

# %%


from marketshare.sod_ms import SOD
from api.api_client import APIClient

client = APIClient()
sod = SOD(client)
# my_df = sod.get_historical_sod_data(start_year=2023, end_year=2024)
my_df = sod.get_sod_data_for_year(year=2024)
my_df.head()
