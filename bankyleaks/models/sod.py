from typing import Dict, Any
from api.api_client import APIClient
import yaml

class SOD:
    def __init__(self, client: APIClient) -> None:
        """
        Initialize the SOD class with an API client.

        :param client: An instance of APIClient to handle API requests.
        """
        self.client = client

    def get_sod(
        self,
        filters: str = "",
        fields: str = "",
        sort_by: str = "",
        sort_order: str = "",
        limit: int = 10,
        offset: int = 0,
        agg_by: str = "",
        agg_term_fields: str = "",
        agg_sum_fields: str = "",
        agg_limit: int = 1,
        format: str = "json",
        download: bool = False,
        filename: str = "data_file"
    ) -> Dict[str, Any]:
        """
        Retrieve SOD data from the API with specified parameters.

        :param filters: A string of filters to apply to the data.
        :param fields: A string of fields to include in the response.
        :param sort_by: The field by which to sort the data.
        :param sort_order: The order of sorting ('ASC' or 'DESC').
        :param limit: The maximum number of records to retrieve.
        :param offset: The number of records to skip before starting to collect the result set.
        :param agg_by: The field by which to aggregate data.
        :param agg_term_fields: Fields to use for term aggregation.
        :param agg_sum_fields: Fields to use for sum aggregation.
        :param agg_limit: The limit for aggregation results.
        :param format: The format of the response ('json', 'xml', etc.).
        :param download: Whether to download the data as a file.
        :param filename: The name of the file to save the data if downloading.
        :return: A dictionary containing the API response data.
        """
        params = {
            "filters": filters,
            "fields": fields,
            "sort_by": sort_by,
            "sort_order": sort_order,
            "limit": limit,
            "offset": offset,
            "agg_by": agg_by,
            "agg_term_fields": agg_term_fields,
            "agg_sum_fields": agg_sum_fields,
            "agg_limit": agg_limit,
            "format": format,
            "download": str(download).lower(),
            "filename": filename
        }
        return self.client.get("sod", params)