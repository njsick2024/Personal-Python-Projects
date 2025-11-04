import requests
from typing import Dict, Any
from urllib.parse import quote

class APIClient:
    BASE_URL = "https://banks.data.fdic.gov/api/"

    def __init__(self) -> None:
        """
        Initialize the APIClient.
        """
        pass  # No API key needed

    def construct_url(self, endpoint: str, params: Dict[str, Any] = None) -> str:
        """
        Construct the full URL for the API request.

        Args:
            endpoint (str): The API endpoint.
            params (Dict[str, Any], optional): The query parameters for the API call.

        Returns:
            str: The constructed URL.
        """
        if params is None:
            params = {}
        # Convert boolean values to lowercase strings
        for key, value in params.items():
            if isinstance(value, bool):
                params[key] = str(value).lower()
        query_string = '&'.join([f"{key}={quote(str(value))}" for key, value in params.items()])
        return f"{self.BASE_URL}{endpoint}?{query_string}"

    def get(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Make a GET request to the API.

        Args:
            endpoint (str): The API endpoint.
            params (Dict[str, Any], optional): The query parameters for the API call.

        Returns:
            Dict[str, Any]: The JSON response from the API.
        """
        url = self.construct_url(endpoint, params)
        headers = {
            "Accept": "application/json",
        }
        response = requests.get(url, headers=headers)
        print("Response Content:", response.content)  # Print raw response content
        response.raise_for_status()
        return response.json()