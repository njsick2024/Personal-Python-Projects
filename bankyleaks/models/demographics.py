from typing import Dict, Any
from api.api_client import APIClient
import yaml

class Demographics:
    def __init__(self, client: APIClient) -> None:
        """
        Initialize the Demographics class with an API client.

        :param client: An instance of APIClient to handle API requests.
        """
        self.client = client

    def get_demographics(
        self,
        filters: str = "",
        format: str = "json",
        download: bool = False,
        filename: str = "data_file"
    ) -> Dict[str, Any]:
        """
        Retrieve demographic data from the API with specified parameters.

        :param filters: A string of filters to apply to the data.
        :param format: The format of the response ('json', 'xml', etc.).
        :param download: Whether to download the data as a file.
        :param filename: The name of the file to save the data if downloading.
        :return: A dictionary containing the API response data.
        """
        params = {
            "filters": filters,
            "format": format,
            "download": str(download).lower(),
            "filename": filename
        }
        return self.client.get("demographics", params)