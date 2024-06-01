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
            api_key (str): The API key for the USGS API.
        """
        self.server_name = server_name


    async def get(self, url, params=None, headers=None):
        """
        Fetches data from the specified API endpoint with given parameters.

        Args:
            endpoint (str): The endpoint to query.
            params (dict): The query parameters.

        Returns:
            dict: The JSON response from the API.

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
            endpoint (str): The endpoint to post to.
            data (dict): The data to post.

        Returns:
            dict: The JSON response from the API.

        Raises:
            aiohttp.ClientError: If the request fails.
        """
        async with aiohttp.ClientSession() as c:
            async with c.post(url, params=params, json=json) as response:
                response.raise_for_status()
                json = await response.json()
                return json, response.headers
