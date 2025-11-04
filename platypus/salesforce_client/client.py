from simple_salesforce import Salesforce
import os
from dotenv import load_dotenv


class SalesforceClient:
    """
    A client for connecting to Salesforce using credentials from a .env file.

    Attributes:
        sf (Salesforce): An instance of the Salesforce client.
    """

    def __init__(self, env_path=None) -> None:
        if env_path:
            load_dotenv(env_path)
        else:
            load_dotenv()

        username = os.getenv("SALESFORCE_USERNAME")
        password = os.getenv("SALESFORCE_PASSWORD")
        token = os.getenv("SALESFORCE_TOKEN")

        self.sf = Salesforce(
            username=username,
            password=password,
            security_token=token,
        )

    def get_client(self, env_path=None) -> Salesforce:
        """
        Returns the Salesforce client instance.

        Returns:
            Salesforce: The Salesforce client instance.
        """
        return self.sf
