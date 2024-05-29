"""
Data models for the Streamstats API.


"""
from endpoints import USGSEndpoints

class Point:

    def __init__(self, rcode, uid, x, y, crs):
        # User parameters
        self.rcode = rcode
        self.unique_id = uid
        self.x = x
        self.y = y
        self.crs = crs
        self.api_client = USGSEndpoints()

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
        return f"Point {self.unique_id}: {self.x}, {self.y}"
    
    async def _delineate_watershed_async(self):
        wshed_json, delin_headers = await self.api_client.get_watershed(self.rcode, self.x, self.y, self.crs)
        self.wshed_geom = wshed_json["featurecollection"][1]["feature"]["features"][0]["geometry"]
        self.server_name = delin_headers['USGSWiM-HostName'].lower()
    
    async def _get_regression_regions_async(self):
        reg_json, _ = await self.api_client.get_regression_regions(self.wshed_geom)
        self.reg_regions = ', '.join([sub['code'] for sub in reg_json])

    async def _get_scenarios_async(self, stat_group='2'):
        scenario_json, _ = await self.api_client.get_scenarios(self.rcode, stat_group, self.reg_regions)
        self.scenarios = scenario_json[0]
        self.param_codes = ','.join([sub['code'] for sub in scenario_json[0]["regressionRegions"][0]["parameters"]])

    async def _get_basin_characteristics_async(self):
        basin_char_json, _ = await self.api_client.get_basin_characteristics(self.rcode, self.wshed_json["workspaceID"], self.param_codes, self.server_name)
        self.basin_char_json = basin_char_json

    async def _get_flow_statistics_async(self):
        for ind, x in enumerate(self.scenarios['regressionRegions'][0]['parameters']):
            for p in self.basin_char_json['parameters']:
                if x['code'].lower() == p['code'].lower():
                    self.scenarios['regressionRegions'][0]['parameters'][ind]['value'] = p['value']
        post_body = [self.scenarios]
        self.flow_stats, _ = await self.api_client.get_flow_statistics({'regions': self.rcode}, post_body)