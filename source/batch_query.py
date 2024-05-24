"""
Batch Query Module

This module contains the BatchQueryTool class which provides functionality to process batch 
queries using the USGS Streamstats API.
"""

import json
import asyncio
import aiohttp
from .api_client import APIClient
from .endpoints import USGSEndpoints
import geopandas as gpd
import time

class BatchQueryTool:
    """
    BatchQueryTool is used to run batch queries against the USGS API.
    
    Attributes:
        input_file (str): Path to the input GIS file containing point queries.
        output_file (str): Path to the output file to save results.
    """
    
    def __init__(self, input_file, output_file, unique_field, rcode):
        """
        Initializes the BatchQueryTool with an API key, input file, and output file.
        
        Args:
            input_file (str): Path to the input file.
            output_file (str): Path to the output file.
        """
        self.api_client = USGSEndpoints()
        self.input_file = input_file
        self.output_file = output_file
        self.unique_field = unique_field
        self.rcode = rcode
        self.tmp_crs = '4326'
        self.og_crs = None
        self.stat_group = 2  # Full list at https://streamstats.usgs.gov/docs/nssservices/#/StatisticGroups/GET/StatisticGroups

    async def load_input(self):
        """
        Loads the input file containing batch queries.
        
        Returns:
            list: A list of queries from the input file.
        """
        in_file = gpd.read_file(self.input_file)
        self.og_crs = in_file.crs.srs.split(':')[1]
        in_file = in_file.to_crs(epsg=self.tmp_crs)
        in_file = in_file.set_index(self.unique_field)
        point_list = [(i, in_file.loc[i].geometry.x, in_file.loc[i].geometry.y) for i in in_file.index]
        return point_list

    async def save_output(self, results):
        """
        Saves the query results to the output file.
        
        Args:
            results (list): A list of results to save.
        """
        with open(self.output_file, 'w') as file:
            json.dump(results, file, indent=4)

    async def process_pt(self, pt, semaphore, max_attempts=5):
        """
        Processes a single point query by querying all necessary apis and saving the result.
        
        Args:
            pt (tuple): A tuple containing the query parameters.
        """
        async with aiohttp.ClientSession() as sess:
            api_client = USGSEndpoints()
            api_client.session = sess
            pt_id = pt[0]
            x = pt[1]
            y = pt[2]
            async with semaphore:
                print(f"Processing point {pt_id}...")

                # Delineate watershed
                attempts = 0
                working = True
                while working:
                    try:
                        wshed_json = await api_client.get_watershed(self.rcode, x, y, self.tmp_crs)
                        wshed_geom = wshed_json["featurecollection"][1]["feature"]["features"][0]["geometry"]
                        working = False
                    except Exception as e:
                        print(f'Error on delination attempt {attempts}. Retrying...')
                        if attempts == max_attempts:
                            return None
                        attempts += 1
                        continue

                # Get regression regions
                attempts = 0
                working = True
                while working:
                    try:
                        reg_json = await api_client.get_regression_regions(wshed_geom)
                        reg_regions = ', '.join([sub['code'] for sub in reg_json])
                        working = False
                    except Exception as e:
                        print(f'Error on regression region attempt {attempts}. Retrying...')
                        if attempts == max_attempts:
                            return None
                        attempts += 1
                        continue

                # Get scenarios
                attempts = 0
                working = True
                while working:
                    try:
                        scenario_json = await api_client.get_scenarios(self.rcode, self.stat_group, reg_regions)
                        scenarios = scenario_json[0]
                        parameterCodes = ', '.join([sub['code'] for sub in scenario_json[0]["regressionRegions"][0]["parameters"]])
                        working = False
                    except Exception as e:
                        print(f'Error on scenario attempt {attempts}. Retrying...')
                        if attempts == max_attempts:
                            return None
                        attempts += 1
                        continue

                # Get basin characteristics
                attempts = 0
                working = True
                while working:
                    try:
                        basin_char_json = await api_client.get_basin_characteristics(self.rcode, wshed_json["workspaceID"], parameterCodes)
                        working = False
                    except Exception as e:
                        print(f'Error on basin characteristics attempt {attempts}. Retrying...')
                        if attempts == max_attempts:
                            return None
                        attempts += 1
                        continue

                # Get flow statistics
                for ind, x in enumerate(scenarios['regressionRegions'][0]['parameters']): # from step 3 & step 4
                    for p in basin_char_json['parameters']:
                        if x['code'].lower() == p['code'].lower():
                            scenarios['regressionRegions'][0]['parameters'][ind]['value'] = p['value']
                post_body = [scenarios]
                attempts = 0
                working = True
                while working:
                    try:
                        flow_stats_json = await api_client.get_flow_statistics({'regions': self.rcode}, post_body)
                        working = False
                    except Exception as e:
                        print(f'Error on flow statistics attempt {attempts}. Retrying...')
                        if attempts == max_attempts:
                            return None
                        attempts += 1
                        continue
                print(f"Finished processing point {pt_id}.")
                [print(i) for i in basin_char_json['parameters']]
            return None

    async def _process_batch_async(self, max_concurrency):
        """
        Processes the batch queries by querying the API for each entry in the input file and saving 
        the results.
        """
        input_data = await self.load_input()
        semaphore = asyncio.Semaphore(max_concurrency)
        results = []
        tasks = []
        for item in input_data:
            task = asyncio.create_task(self.process_pt(item, semaphore))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        await self.save_output(results)

    def process_batch(self, max_concurrency=1):
        """
        Processes the batch queries synchronously by querying the API for each entry in the input file 
        and saving the results.
        """
        asyncio.run(self._process_batch_async(max_concurrency))

