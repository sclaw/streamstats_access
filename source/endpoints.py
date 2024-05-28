"""
Endpoints Module

This module contains classes and methods to interact with specific USGS API endpoints.
"""

from .api_client import APIClient
from .config import config

class USGSEndpoints(APIClient):
    """
    USGSEndpoints provides methods to interact with various USGS API endpoints.
    """
    
    async def get_watershed(self, rcode, x, y, crs):
        """
        Fetches earthquake data from the USGS API.
        
        Args:
            start_time (str): The start time for the query.
            end_time (str): The end time for the query.
            min_magnitude (float): The minimum magnitude of earthquakes to fetch.
        
        Returns:
            dict: The JSON response from the API containing earthquake data.
        """
        params = {
            'rcode': str(rcode),
            'xlocation': str(x),
            'ylocation': str(y),
            'crs': str(crs)
        }
        url = config['StreamStatsServiceURLS']['watershed']
        return await self.get(url, params)
    
    async def get_regression_regions(self, delineated_basin):
        """
        Fetches regression regions from the USGS API.
        
        Args:
            delineated_basin (dict): The delineated basin geometry.
        
        Returns:
            dict: The JSON response from the API containing regression regions.
        """
        url = config['NSSServiceURlS']['regressionRegions']
        return await self.post(url, json=delineated_basin)
    
    async def get_scenarios(self, rcode, stat_group, regression_regions):
        """
        Fetches scenarios from the USGS API.
        
        Args:
            rcode (str): The region code.
            stat_group (str): The statistic group.
            regression_regions (dict): The regression regions.
        
        Returns:
            dict: The JSON response from the API containing scenarios.
        """
        params = {
            'regions': str(rcode),
            'statisticgroups': str(stat_group),
            'regressionregions': regression_regions
        }
        url = config['NSSServiceURlS']['scenarios']
        return await self.get(url, params)
    
    async def get_basin_characteristics(self, rcode, workspace_id, parameters, host_name):
        """
        Fetches basin characteristics from the USGS API.
        
        Args:
            rcode (str): The region code.
            workspace_id (str): The workspace ID.
            parameters (dict): The parameters.
        
        Returns:
            dict: The JSON response from the API containing basin characteristics.
        """
        params = {
            'rcode': str(rcode),
            'workspaceID': str(workspace_id),
            'includeparameters': parameters
        }
        url = config['StreamStatsServiceURLS']['basinCharacteristics'].format(host_name)
        return await self.get(url, params)
    
    async def get_flow_statistics(self, rcode, scenarios):
        """
        Fetches flow statistics from the USGS API.
        
        Args:
            rcode (str): The region code.
            scenarios (dict): The scenarios.
        
        Returns:
            dict: The JSON response from the API containing flow statistics.
        """
        url = config['NSSServiceURlS']['computeFlowStats']
        return await self.post(url, params=rcode, json=scenarios)

