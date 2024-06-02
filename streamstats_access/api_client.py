import aiohttp

class APIClient:
    """
    APIClient is a client for interacting with the USGS Streamstats API.

    Attributes:
        session (aiohttp.ClientSession): An instance of aiohttp ClientSession.
    """

    def __init__(self, server_name='prodweba'):
        """
        Initializes the APIClient with an API key and creates an aiohttp ClientSession.

        Args:
            server_name (str): Which USGS server to send requests to (prodweba or prodwebb).
        """
        self.server_name = server_name


    async def get(self, url, params=None, headers=None):
        """
        Fetches data from the specified API endpoint with given parameters.

        Args:
            url (str): The URL to fetch data from.
            params (dict, optional): The query parameters. Defaults to None.
            headers (dict, optional): The request headers. Defaults to None.

        Returns:
            tuple: A tuple containing the JSON response from the API (dict) and response headers (aiohttp.ClientResponse.headers).

        Raises:
            aiohttp.ClientError: If the request fails.
        """
        async with aiohttp.ClientSession() as c:
            async with c.get(url, params=params, headers=headers) as response:
                response.raise_for_status()
                json = await response.json()
                return json, response.headers
        
    async def post(self, url, params=None, json=None):
        """
        Posts data to the specified API endpoint.

        Args:
            url (str): The URL to post data to.
            params (dict, optional): The query parameters. Defaults to None.
            json (dict, optional): The JSON payload to send in the request body. Defaults to None.

        Returns:
            tuple: A tuple containing the JSON response from the API (dict) and response headers (aiohttp.ClientResponse.headers).

        Raises:
            aiohttp.ClientError: If the request fails.
        """
        async with aiohttp.ClientSession() as c:
            async with c.post(url, params=params, json=json) as response:
                response.raise_for_status()
                json = await response.json()
                return json, response.headers
