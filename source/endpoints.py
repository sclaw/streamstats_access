"""
Endpoints Module

This module contains classes and methods to interact with specific USGS API endpoints.
"""

from .api_client import APIClient
from .config import config
import asyncio

class USGSEndpoints(APIClient):
    """
    USGSEndpoints provides methods to interact with various USGS API endpoints.
    """
    
    async def get_watershed(self, rcode, x, y, crs):
        """
        Fetches watershed data from the USGS API.
        
        Args:
            rcode (str): The region code.
            x (float): The x-coordinate (longitude) of the point.
            y (float): The y-coordinate (latitude) of the point.
            crs (str): The coordinate reference system.
        
        Returns:
            tuple: A tuple containing the JSON response from the API and the response headers.
        """
        params = {
            'rcode': str(rcode),
            'xlocation': str(x),
            'ylocation': str(y),
            'crs': str(crs),
            'simplify': 'true',
            'includeparameters': 'true',
            'includeflowtypes': 'false',
            'includefeatures': 'true'
        }
        url = config['StreamStatsServiceURLS']['watershed'].format(self.server_name)
        return await self.get(url, params)
    
    async def get_regression_regions(self, delineated_basin):
        """
        Fetches regression regions from the USGS API.
        
        Args:
            delineated_basin (dict): The delineated basin geometry.
        
        Returns:
            tuple: A tuple containing the JSON response from the API and the response headers.
        """
        url = config['NSSServiceURlS']['regressionRegions']
        return await self.post(url, json=delineated_basin)
    
    async def get_scenarios(self, rcode, stat_group, regression_regions):
        """
        Fetches scenarios from the USGS API.
        
        Args:
            rcode (str): The region code.
            stat_group (str): The statistic group.
            regression_regions (str): The regression regions codes as a comma-separated string.
        
        Returns:
            tuple: A tuple containing the JSON response from the API and the response headers.
        """
        params = {
            'regions': str(rcode),
            'statisticgroups': str(stat_group),
            'regressionregions': regression_regions
        }
        url = config['NSSServiceURlS']['scenarios']
        return await self.get(url, params)
    
    async def _get_basin_characteristics_async(self, rcode, workspace_id=None, parameters=None):
        """
        Fetches basin characteristics from the USGS API.
        
        Args:
            rcode (str): The region code.
            workspace_id (str, optional): The workspace ID. Defaults to None.
            parameters (str, optional): The parameters to include. Defaults to None.
        
        Returns:
            tuple: A tuple containing the JSON response from the API and the response headers.
        """
        if parameters is None and workspace_id is None:
            params = {'rcode': str(rcode)}
        else:
            params = {
                'rcode': str(rcode),
                'workspaceID': str(workspace_id),
                'includeparameters': (parameters)
            }
        url = config['StreamStatsServiceURLS']['basinCharacteristics'].format(self.server_name)
        return await self.get(url, params)
    
    async def get_flow_statistics(self, rcode, scenarios):
        """
        Fetches flow statistics from the USGS API.
        
        Args:
            rcode (dict): The region code.
            scenarios (list): The scenarios.
        
        Returns:
            tuple: A tuple containing the JSON response from the API and the response headers.
        """
        url = config['NSSServiceURlS']['computeFlowStats']
        return await self.post(url, params=rcode, json=scenarios)

    def get_basin_characteristics(self, rcode, workspace_id=None, parameters=None):
        """
        Fetches basin characteristics from the USGS API synchronously.
        
        Args:
            rcode (str): The region code.
            workspace_id (str, optional): The workspace ID. Defaults to None.
            parameters (str, optional): The parameters to include. Defaults to None.
        
        Returns:
            dict: The JSON response from the API containing basin characteristics.
        """
        return asyncio.run(self._get_basin_characteristics_async(rcode, workspace_id, parameters))
