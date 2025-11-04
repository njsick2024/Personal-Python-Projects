
![alt text](docs/image-2.png)

# BankyLeaks Data API

This project provides a Python-based interface for interacting with a banking data API. It allows users to retrieve and process data related to banking institutions, locations, demographics, and more.
https://www.fdic.gov/resources/data-tools

## Project Structure

- **models/**: Contains modules for different data entities.
  - `sod.py`: Handles Summary of Deposits (SOD) data.
  - `institutions.py`: Manages institution-related data.
  - `locations.py`: Deals with location-specific data.
  - `demographics.py`: Retrieves demographic data.

- **api/**: Contains the `APIClient` class for handling API requests.

- **utils/**: Provides utility functions for common operations like loading fields and counting records.

- **metadata/**: YAML files defining properties and mappings for different data entities.

- **main.py**: The main script for executing various operations, such as fetching data and processing it into DataFrames.

## Usage

1. **Initialize the API Client**:
   ```python
   from api.api_client import APIClient
   client = APIClient()
   ```

2. **Fetch Data**:
   - Create an instance of the desired data class (e.g., `SOD`, `Institutions`).
   - Define the parameters for the API call.
   - Use the class methods to retrieve data.

3. **Example**:
   ```python
   from models.sod import SOD
   sod = SOD(client)
   params = {
       "filters": 'CERT:14',
       "fields": 'CERT,YEAR,ASSET',
       "sort_by": 'YEAR',
       "sort_order": 'DESC',
       "limit": 10000,
       "format": 'json'
   }
   sod_data = sod.get_sod(**params)
   ```

4. **Convert to DataFrame**:
   ```python
   import pandas as pd
   if 'data' in sod_data:
       data_list = [item['data'] for item in sod_data['data']]
       df = pd.DataFrame(data_list)
       print(df.head())
   ```

## Configuration

- **API Key**: Set your API key in `config/config.py`.
- **Field Definitions**: Modify YAML files in the `metadata/` directory to update field definitions.

