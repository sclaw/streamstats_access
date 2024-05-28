import aiohttp
import asyncio
import requests
from .config import config

class APIClient:
    """
    APIClient is a client for interacting with the USGS Streamstats API.

    Attributes:
        session (aiohttp.ClientSession): An instance of aiohttp ClientSession.
    """

    def __init__(self, max_concurrency=10):
        """
        Initializes the APIClient with an API key and creates an aiohttp ClientSession.

        Args:
            api_key (str): The API key for the USGS API.
        """
        self.max_concurrency = max_concurrency
        self.semaphore = asyncio.Semaphore(self.max_concurrency)

    async def get(self, url, params=None):
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
        async with self.semaphore as s, aiohttp.ClientSession() as c:
            print('making request')
            async with c.get(url, params=params) as response:
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
        async with self.semaphore as s, self.session as c:
            async with c.post(url, params=params, json=json) as response:
                response.raise_for_status()
                json = await response.json()
                return json, response.headers
