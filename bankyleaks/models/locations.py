from typing import Dict, Any
from api.api_client import APIClient
import yaml

class Locations:
    def __init__(self, client: APIClient) -> None:
        """
        Initialize the Locations class with an API client.

        :param client: An instance of APIClient to handle API requests.
        """
        self.client = client

    def _load_fields(self, yaml_file: str) -> str:
        """
        Load field names from a YAML file and return them as a comma-separated string.

        :param yaml_file: Path to the YAML file containing field definitions.
        :return: A comma-separated string of field names.
        """
        with open(yaml_file, 'r') as file:
            data = yaml.safe_load(file)
            return ','.join(data['properties'].keys())

    def get_locations(
        self,
        filters: str = "",
        fields: str = "",
        sort_by: str = "",
        sort_order: str = "",
        limit: int = 10,
        offset: int = 0,
        format: str = "json",
        download: bool = False,
        filename: str = "location_data_file"
    ) -> Dict[str, Any]:
        """
        Retrieve location data from the API with specified parameters.

        :param filters: A string of filters to apply to the data.
        :param fields: A string of fields to include in the response.
        :param sort_by: The field by which to sort the data.
        :param sort_order: The order of sorting ('ASC' or 'DESC').
        :param limit: The maximum number of records to retrieve.
        :param offset: The number of records to skip before starting to collect the result set.
        :param format: The format of the response ('json', 'xml', etc.).
        :param download: Whether to download the data as a file.
        :param filename: The name of the file to save the data if downloading.
        :return: A dictionary containing the API response data.
        """
        if not fields:
            fields = self._load_fields('metadata/location_properties.yaml')
        params = {
            "filters": filters,
            "fields": fields,
            "sort_by": sort_by,
            "sort_order": sort_order,
            "limit": limit,
            "offset": offset,
            "format": format,
            "download": str(download).lower(),
            "filename": filename
        }
        return self.client.get("locations", params)
    



