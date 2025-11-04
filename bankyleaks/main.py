
#%%
from api.api_client import APIClient
from models.sod import SOD
from utils.utils import load_fields, get_sod_record_count
import pandas as pd
    
client = APIClient()

# Create an instance of SOD
sod = SOD(client)

# Define which fields to include for SOD
selected_fields = ["CERT", "YEAR", "ASSET", "DEPSUMBR", "STALPBR"]

# Load fields from YAML for SOD
fields = load_fields('metadata/sod_properties.yaml', selected_fields)

# Define parameters for the API call
params = {
    "filters": 'CERT:14',
    "fields": fields,
    "sort_by": 'YEAR',
    "sort_order": 'DESC',
    "limit": 10000,
    "offset": 0,
    "agg_by": 'CERT',
    "agg_term_fields": 'YEAR',
    "agg_sum_fields": 'ASSET',
    "agg_limit": 1,
    "format": 'json',
    "download": False,
    "filename": 'sod_data_file'
}

# Get the total count of SOD records
total_records = get_sod_record_count(client, sod, params)
print(f"Total SOD Records: {total_records}")


# Fetch SOD data with specified parameters
sod_data = sod.get_sod(**params)

# Convert the response to a Pandas DataFrame
if 'data' in sod_data:
    data_list = [item['data'] for item in sod_data['data']]
    df = pd.DataFrame(data_list)
    print("SOD Data (DataFrame):")
    print(df.head())
else:
    print("No data found in the response.")




# !# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # c


# %%
from api.api_client import APIClient
from models.institutions import Institutions
from models.locations import Locations
from models.demographics import Demographics
from config.config import API_KEY
from utils.utils import load_fields, get_institutions_record_count, get_locations_record_count, get_demographics_record_count
import yaml
from typing import List
import pandas as pd


client = APIClient()

# Create an instance of Demographics
demographics = Demographics(client)

# Define parameters for the API call
params = {
    "filters": 'CERT:14 AND REPDTE:20230630',
    # "filters": '',
    "format": 'json',
    "download": False,
    "filename": 'demographics_data_file'
}

total_records = get_demographics_record_count(client, demographics, params)
print(f"Total Records: {total_records}")

# Fetch demographics with specified parameters
demographics_data = demographics.get_demographics(**params)

# Convert the response to a Pandas DataFrame
if 'data' in demographics_data:
    data_list = [item['data'] for item in demographics_data['data']]
    df = pd.DataFrame(data_list)
    print("Demographics Data (DataFrame):")
    print(df.head())
else:
    print("No data found in the response.")

total_records = get_demographics_record_count(client, demographics, params)
print(f"Total Records: {total_records}")


# %%
# Fetch demographics with specified parameters
demographics_data = demographics.get_demographics(**params)

# Convert the response to a Pandas DataFrame
if 'data' in demographics_data:
    data_list = [item['data'] for item in demographics_data['data']]
    df = pd.DataFrame(data_list)
    print("Demographics Data (DataFrame):")
    print(df.head())
else:
    print("No data found in the response.")


# !# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # c



# %%
from api.api_client import APIClient
from models.institutions import Institutions
from config.config import API_KEY
from utils.utils import load_fields, get_institutions_record_count, get_locations_record_count
import yaml
from typing import List
import pandas as pd
from models.locations import Locations
# Initialize the API client

# Initialize the API client
client = APIClient()

# Create an instance of Institutions
institutions = Institutions(client)

# Define which fields to include
selected_fields = ["ZIP", "OFFDOM", "CITY", "COUNTY", "STNAME", "STALP", "NAME", "ACTIVE", "CERT", "CBSA", "ASSET", "NETINC", "DEP", "DEPDOM", "ROE", "ROA", "DATEUPDT", "OFFICES"]

# Load fields from YAML
fields = load_fields('metadata/institution_properties.yaml', selected_fields)

# Print loaded fields to verify
print("Loaded Fields:", fields)

# Define parameters for the API call
params = {
    # "filters": 'STALP:"TX" AND ACTIVE:1 AND NAME:"Scotia Bank"',
    # "filters": "ACTIVE:1",
    "filters": "",
    "fields": fields,
    "sort_by": 'OFFICES',
    "sort_order": 'DESC',
    "limit": 10000,
    "offset": 0,
    "format": 'json',
    "download": False,
    "filename": 'data_file'
}

    # Get the total count of records
total_records = get_institutions_record_count(client, institutions, params)
print(f"Total Records: {total_records}")

# Construct and print the URL
url = client.construct_url("institutions", params)
print("Constructed URL:", url)

# Fetch institutions with specified parameters
institutions_data = institutions.get_institutions(**params)
print("Raw API Response:", institutions_data)

# Convert the response to a Pandas DataFrame
if 'data' in institutions_data:
    data_list = [item['data'] for item in institutions_data['data']]
    df = pd.DataFrame(data_list)
    print("Institutions Data (DataFrame):")
    df.head() # Ensure the DataFrame is printed
else:
    print("No data found in the response.")# %%



# !# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # c



# %%
from api.api_client import APIClient
from models.institutions import Institutions
from config.config import API_KEY
from utils.utils import load_fields, get_institutions_record_count, get_locations_record_count
import yaml
from typing import List
import pandas as pd
from models.locations import Locations
# Initialize the API client
client = APIClient()

# Create an instance of Locations
locations = Locations(client)

# Define which fields to include for locations
selected_fields = ["NAME", "UNINUM", "SERVTYPE", "RUNDATE", "CITY", "STNAME", "ZIP", "COUNTY"]

# Load fields from YAML for locations
fields = load_fields('metadata/location_properties.yaml', selected_fields)

params = {
    # "filters": 'NAME:"Comerica Bank"',
    "filters": "",
    "fields": fields,
    "sort_by": 'NAME',
    "sort_order": 'DESC',
    "limit": 10000,
    "offset": 0,
    "format": 'json',
    "download": False,
    "filename": 'location_data_file'
}

# Get the total count of location records
total_records = get_locations_record_count(client, locations, params)
print(f"Total Location Records: {total_records}")


# Reset the limit for actual data fetching
params['limit'] = 10000

# Construct and print the URL
url = client.construct_url("institutions", params)
print(url)

# %%

# Fetch locations with specified parameters
locations_data = locations.Locations.get_locations(**params)

# Convert the response to a Pandas DataFrame
if 'data' in locations_data:
    data_list = [item['data'] for item in locations_data['data']]
    df = pd.DataFrame(data_list)
    print(df.head())
else:
    print("No data found in the response.")



# %%
from models import locations

data = locations.Locations.get_locations()

locations.get_locations()

# %%
