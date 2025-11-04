"""
This module provides middleware for handling authentication and cookies when interacting with a Dremio server using Apache Arrow Flight.

Classes:
    DremioClientAuthMiddlewareFactory: A factory that creates DremioClientAuthMiddleware instances.
    DremioClientAuthMiddleware: Middleware for handling authentication headers.
    CookieMiddlewareFactory: A factory that creates CookieMiddleware instances.
    CookieMiddleware: Middleware for handling cookies.

Usage:
    The middleware classes in this module are used to manage authentication and cookies when making requests to a Dremio server.
    They are typically used in conjunction with a FlightClient instance to ensure that the necessary headers are included in each request.
"""

from http import cookies
from pyarrow import flight
from http.cookies import SimpleCookie
from functools import reduce

class DremioClientAuthMiddlewareFactory(flight.ClientMiddlewareFactory):
    """A factory that creates DremioClientAuthMiddleware instances.

    Attributes:
        call_credential (list): A list to store call credentials.

    Methods:
        start_call(info): Starts a new call and returns a DremioClientAuthMiddleware instance.
        set_call_credential(call_credential): Sets the call credentials.
    """

    def __init__(self):
        self.call_credential = []

    def start_call(self, info):
        return DremioClientAuthMiddleware(self)

    def set_call_credential(self, call_credential):
        self.call_credential = call_credential

class DremioClientAuthMiddleware(flight.ClientMiddleware):
    """Middleware for handling authentication headers.

    Attributes:
        factory (DremioClientAuthMiddlewareFactory): The factory that created this middleware instance.

    Methods:
        received_headers(headers): Processes received headers to extract the authorization token.
    """
    def __init__(self, factory):
        self.factory = factory

    def received_headers(self, headers):
        if self.factory.call_credential:
            return

        auth_header_key = "authorization"

        authorization_header = reduce(
            lambda result, header: header[1]
            if header[0] == auth_header_key
            else result,
            headers.items(),
        )
        if not authorization_header:
            raise Exception("Did not receive authorization header back from server.")
        bearer_token = authorization_header[1][0]
        self.factory.set_call_credential(
            [b"authorization", bearer_token.encode("utf-8")]
        )
class CookieMiddlewareFactory(flight.ClientMiddlewareFactory):
    """A factory that creates CookieMiddleware instances.

    Attributes:
        cookies (dict): A dictionary to store cookies.

    Methods:
        start_call(info): Starts a new call and returns a CookieMiddleware instance.
    """
    def __init__(self):
        self.cookies = {}

    def start_call(self, info):
        return CookieMiddleware(self)

class CookieMiddleware(flight.ClientMiddleware):
    """Middleware for handling cookies.

    Attributes:
        factory (CookieMiddlewareFactory): The factory that created this middleware instance.

    Methods:
        received_headers(headers): Processes received headers to extract cookies.
        sending_headers(): Returns the cookies to be sent in the request headers.
    """
    def __init__(self, factory):
        self.factory = factory

    def received_headers(self, headers):
        for key in headers:
            if key.lower() == 'set-cookie':
                cookie = SimpleCookie()
                for item in headers.get(key):
                    cookie.load(item)
                self.factory.cookies.update(cookie.items())

    def sending_headers(self):
        if self.factory.cookies:
            cookie_string = '; '.join("{!s}={!s}".format(key, val.value) for (key, val) in self.factory.cookies.items())
            return {b'cookie': cookie_string.encode('utf-8')}
        return {}