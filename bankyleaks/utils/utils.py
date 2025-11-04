import yaml
from typing import List, Dict, Any

def get_institutions_record_count(client: Any, institutions: Any, params: Dict[str, Any]) -> int:
    """
    Get the total count of institution records.

    Args:
        client (Any): The API client instance.
        institutions (Any): The institutions instance.
        params (Dict[str, Any]): The parameters for the API call.

    Returns:
        int: The total count of institution records.
    """
    params['limit'] = 1
    params['offset'] = 0
    institutions_data = institutions.get_institutions(**params)
    if 'meta' in institutions_data and 'total' in institutions_data['meta']:
        return institutions_data['meta']['total']
    else:
        print("Total count not found in the response metadata.")
        return 0

def get_locations_record_count(client: Any, locations: Any, params: Dict[str, Any]) -> int:
    """
    Get the total count of location records.

    Args:
        client (Any): The API client instance.
        locations (Any): The locations instance.
        params (Dict[str, Any]): The parameters for the API call.

    Returns:
        int: The total count of location records.
    """
    params['limit'] = 1
    params['offset'] = 0
    locations_data = locations.get_locations(**params)
    if 'meta' in locations_data and 'total' in locations_data['meta']:
        return locations_data['meta']['total']
    else:
        print("Total count not found in the response metadata.")
        return 0

def get_demographics_record_count(client: Any, demographics: Any, params: Dict[str, Any]) -> int:
    """
    Get the total count of demographic records.

    Args:
        client (Any): The API client instance.
        demographics (Any): The demographics instance.
        params (Dict[str, Any]): The parameters for the API call.

    Returns:
        int: The total count of demographic records.
    """
    demographics_data = demographics.get_demographics(**params)
    if 'meta' in demographics_data and 'total' in demographics_data['meta']:
        return demographics_data['meta']['total']
    else:
        print("Total count not found in the response metadata.")
        return 0

def get_sod_record_count(client: Any, sod: Any, params: Dict[str, Any]) -> int:
    """
    Get the total count of SOD records.

    Args:
        client (Any): The API client instance.
        sod (Any): The SOD instance.
        params (Dict[str, Any]): The parameters for the API call.

    Returns:
        int: The total count of SOD records.
    """
    sod_data = sod.get_sod(**params)
    if 'meta' in sod_data and 'total' in sod_data['meta']:
        return sod_data['meta']['total']
    else:
        print("Total count not found in the response metadata.")
        return 0

def load_fields(yaml_file: str, selected_fields: List[str]) -> str:
    """
    Load and return the selected fields from a YAML file.

    Args:
        yaml_file (str): The path to the YAML file.
        selected_fields (List[str]): The list of fields to include.

    Returns:
        str: A comma-separated string of the selected fields.
    """
    with open(yaml_file, 'r') as file:
        data = yaml.safe_load(file)
    properties = data.get('properties', {}).get('data', {}).get('properties', {})
    fields = [field for field in selected_fields if field in properties]
    return ','.join(fields)