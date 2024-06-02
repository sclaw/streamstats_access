
from .endpoints import USGSEndpoints
import geopandas as gpd
import pandas as pd

class Point:
    """
    Point represents a geographical point with associated watershed and hydrological data.

    Attributes:
        rcode (str): The region code.
        id (str): The unique identifier for the point.
        unique_id_label (str): The label for the unique identifier field.
        x (float): The x-coordinate (longitude) of the point.
        y (float): The y-coordinate (latitude) of the point.
        crs (str): The coordinate reference system.
        api_client (USGSEndpoints): The API client for interacting with USGS endpoints.
        attempts (int): The number of attempts made for API calls.
        wshed_json (dict): The JSON data for the delineated watershed.
        server_name (str): The name of the server handling the request.
        reg_json (dict): The JSON data for the regression regions.
        reg_regions (str): The regression regions codes as a comma-separated string.
        scenario_json (dict): The JSON data for the scenarios.
        scenarios (dict): The scenarios data.
        param_codes (str): The parameter codes as a comma-separated string.
        basin_char_json (dict): The JSON data for the basin characteristics.
        flow_stats (dict): The JSON data for the flow statistics.
    """

    def __init__(self, rcode, x, y, crs, uid=None, field_name='Name'):
        """
        Initializes a Point object.

        Args:
            rcode (str): The region code.
            x (float): The x-coordinate (longitude) of the point.
            y (float): The y-coordinate (latitude) of the point.
            crs (str): The coordinate reference system.
            uid (str, optional): The unique identifier for the point. Defaults to None.
            field_name (str, optional): The label for the unique identifier field. Defaults to 'Name'.
        """
        # User parameters
        self.rcode = rcode
        self.id = uid
        self.unique_id_label = field_name
        self.x = x
        self.y = y
        self.crs = crs
        self.api_client = USGSEndpoints()
        self.attempts = 0

        # Derived parameters
        self.wshed_json = None
        self.server_name = None
        self.reg_json = None
        self.reg_regions = None
        self.scenario_json = None
        self.scenarios = None
        self.param_codes = None
        self.basin_char_json = None
        self.flow_stats = None

    def __str__(self):
        """
        Returns a string representation of the Point object.

        Returns:
            str: String representation of the Point.
        """
        return f"Point {self.id}: {self.x}, {self.y}"
    
    def set_server_name(self, server_name):
        """
        Sets the server name for the API client.

        Args:
            server_name (str): The name of the server.
        """
        self.api_client = USGSEndpoints(server_name)
    
    async def _delineate_watershed_async(self):
        """
        Asynchronously delineates the watershed for the point.

        Returns:
            None
        """
        self.wshed_json, delin_headers = await self.api_client.get_watershed(self.rcode, self.x, self.y, self.crs)
        self.wshed_geom = self.wshed_json["featurecollection"][1]["feature"]["features"][0]["geometry"]
        self.server_name = delin_headers['USGSWiM-HostName'].lower()
    
    async def _get_regression_regions_async(self):
        """
        Asynchronously retrieves the regression regions for the point.

        Returns:
            None
        """
        reg_json, _ = await self.api_client.get_regression_regions(self.wshed_geom)
        self.reg_regions = ', '.join([sub['code'] for sub in reg_json])

    async def _get_scenarios_async(self, stat_group='2'):
        """
        Asynchronously retrieves the scenarios for the point.

        Args:
            stat_group (str, optional): The statistical group. Defaults to '2'.

        Returns:
            None
        """
        scenario_json, _ = await self.api_client.get_scenarios(self.rcode, stat_group, self.reg_regions)
        self.scenarios = scenario_json[0]
        self.param_codes = ','.join([sub['code'] for sub in scenario_json[0]["regressionRegions"][0]["parameters"]])

    async def _get_basin_characteristics_async(self, all_params=False):
        """
        Asynchronously retrieves the basin characteristics for the point.

        Args:
            all_params (bool, optional): Whether to retrieve all parameters. Defaults to False.

        Returns:
            None
        """
        if all_params:
            param_codes = 'true'
        else:
            param_codes = self.param_codes
        basin_char_json, _ = await self.api_client._get_basin_characteristics_async(self.rcode, self.wshed_json["workspaceID"], param_codes)
        self.basin_char_json = basin_char_json

    async def _get_flow_statistics_async(self):
        """
        Asynchronously retrieves the flow statistics for the point.

        Returns:
            None
        """
        for ind, x in enumerate(self.scenarios['regressionRegions'][0]['parameters']):
            for p in self.basin_char_json['parameters']:
                if x['code'].lower() == p['code'].lower():
                    self.scenarios['regressionRegions'][0]['parameters'][ind]['value'] = p['value']
        post_body = [self.scenarios]
        self.flow_stats, _ = await self.api_client.get_flow_statistics({'regions': self.rcode}, post_body)

    def wshed_gdf(self):
        """
        Converts the watershed JSON data to a GeoDataFrame.

        Returns:
            geopandas.GeoDataFrame: The GeoDataFrame containing the watershed data.
        """
        gdf = gpd.GeoDataFrame.from_features(self.wshed_json['featurecollection'][1]['feature']['features'])
        gdf[self.unique_id_label] = self.id
        gdf = gdf.set_index(self.unique_id_label)
        return gdf
    
    def pt_gdf(self):
        """
        Converts the outlet point JSON data to a GeoDataFrame.

        Returns:
            geopandas.GeoDataFrame: The GeoDataFrame containing the outlet point data.
        """
        gdf = gpd.GeoDataFrame.from_features(self.wshed_json['featurecollection'][0]['feature']['features'])
        gdf = gdf.drop(columns=['FID'])
        gdf[self.unique_id_label] = self.id
        gdf = gdf.set_index(self.unique_id_label)
        return gdf
    
    def characteristics_df(self):
        """
        Converts the basin characteristics JSON data to a DataFrame.

        Returns:
            pandas.DataFrame: The DataFrame containing the basin characteristics data.
        """
        df = pd.json_normalize(self.basin_char_json['parameters'])
        df = df.drop(columns=['name', 'ID'])
        df = df.rename(columns={'description': 'StatName', 'code': 'StatLabel', 'value': 'Value', 'units': 'Units'})
        df[self.unique_id_label] = self.id
        df = df.set_index(self.unique_id_label)
        return df
    
    def statistics_df(self):
        """
        Converts the flow statistics JSON data to a DataFrame.

        Returns:
            pandas.DataFrame: The DataFrame containing the flow statistics data.
        """
        df = pd.json_normalize(self.flow_stats[0]['regressionRegions'][0]['results'])
        df = df.drop(columns=['id'])
        rename_dict = {'name': 'StatName', 'code': 'StatLabel', 'value': 'Value', 'units': 'Units', 'equivalentYears': 'Years', 'intervalBounds.lower': 'Pll', 'intervalBounds.upper': 'Plu'}
        df = df.rename(columns=rename_dict)
        df = df.drop(columns=[c for c in df.columns if c not in list(rename_dict.values())])
        df[self.unique_id_label] = self.id
        df = df.set_index(self.unique_id_label)
        return df