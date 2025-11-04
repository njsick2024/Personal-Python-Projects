import logging
import os
import time

import dotenv
from pyarrow import flight

from .auth import CookieMiddlewareFactory, DremioClientAuthMiddlewareFactory


class DremioDatabase:
    """
    A class to interact with a Dremio database using Apache Arrow Flight.

    This class handles authentication with the Dremio server and provides methods to execute SQL queries.

    Attributes:
        username (str): The username for authentication. Defaults to the current logged-in user if not provided.
        password (str): The password for authentication.
        endpoint (str): The Dremio server endpoint.
        port (str): The port for the Dremio server.
        headers (list): A list to store authentication headers.
        client (flight.FlightClient): The FlightClient instance for interacting with the Dremio server.

    Methods:
        get_client(max_retries: int = 2) -> flight.FlightClient:
            Authenticates with the Dremio server and returns a FlightClient instance.

        execute_query(query: str) -> flight.FlightStreamReader:
            Executes a SQL query on the Dremio server and returns a FlightStreamReader to read the results.
    """

    def __init__(self):

        self.username = os.getlogin()
        self.password = os.getenv("DREMIO_PASSWORD")
        self.endpoint = os.getenv("DREMIO_ENDPOINT")
        self.port = "32010"
        self.headers = None
        self.client = None

    def get_client(self, max_retries: int = 2) -> flight.FlightClient:
        client_auth_middleware = DremioClientAuthMiddlewareFactory()
        client_cookie_middleware = CookieMiddlewareFactory()
        headers = []
        retries = 0

        while retries < max_retries:
            try:
                client = flight.FlightClient(
                    f"grpc+tcp://{self.endpoint}:{self.port}",
                    middleware=[client_auth_middleware, client_cookie_middleware],
                )
                bearer_token = client.authenticate_basic_token(
                    self.username, self.password, flight.FlightCallOptions(headers=headers)
                )
                headers.append(bearer_token)
                self.client = client
                self.headers = headers
                return

            except Exception as e:
                retries += 1
                logging.error(f"Failed to authenticate with Dremio (attempt {retries}/{max_retries}): {e}")
                if retries >= max_retries:
                    raise e
                time.sleep(2**retries)  # Exponential backoff

    def execute_query(self, query: str) -> flight.FlightStreamReader:
        self.get_client()
        flight_desc = flight.FlightDescriptor.for_command(query)
        options = flight.FlightCallOptions(headers=self.headers)
        schema = self.client.get_schema(flight_desc, options)
        flight_info = self.client.get_flight_info(flight.FlightDescriptor.for_command(query), options=options)
        reader = self.client.do_get(flight_info.endpoints[0].ticket, options=options)
        return reader
