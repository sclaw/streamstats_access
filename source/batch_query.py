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
import pandas as pd
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
        point_list = [(i, in_file.loc[i].geometry.x, in_file.loc[i].geometry.y, 0) for i in in_file.index]
        return point_list

    async def point_worker(self, in_q, out_q, max_retries=5):
        """
        Processes a single point query by querying all necessary apis and saving the result.
        
        Args:
            pt (tuple): A tuple containing the query parameters.
        """

        while not in_q.empty():
            pt = in_q.get_nowait()
            if pt[3] > max_retries:
                out_q.put_nowait(pt[0])

            pt_id = pt[0]
            x = pt[1]
            y = pt[2]

            print(f'{pt_id} - Delineating watershed')
            # Delineate watershed
            try:
                wshed_json, delin_headers = await self.api_client.get_watershed(self.rcode, x, y, self.tmp_crs)
                wshed_geom = wshed_json["featurecollection"][1]["feature"]["features"][0]["geometry"]
                server_name = delin_headers['USGSWiM-HostName'].lower()
            except Exception as e:
                in_q.put_nowait((pt_id, x, y, pt[3] + 1))
                continue

            
            print(f'{pt_id} - Getting regression regions')
            # Get regression regions
            try:
                reg_json, _ = await self.api_client.get_regression_regions(wshed_geom)
                reg_regions = ', '.join([sub['code'] for sub in reg_json])
            except Exception as e:
                in_q.put_nowait((pt_id, x, y, pt[3] + 1))
                continue
            
            print(f'{pt_id} - Getting scenarios')
            # Get scenarios
            try:
                scenario_json, _ = await self.api_client.get_scenarios(self.rcode, self.stat_group, reg_regions)
                scenarios = scenario_json[0]
                param_codes = ','.join([sub['code'] for sub in scenario_json[0]["regressionRegions"][0]["parameters"]])
            except Exception as e:
                in_q.put_nowait((pt_id, x, y, pt[3] + 1))
                continue
            
            print(f'{pt_id} - Getting basin characteristics')
            # Get basin characteristics
            working = True
            attempt = 1
            try:
                while working:
                    basin_char_json, _ = await self.api_client.get_basin_characteristics(self.rcode, wshed_json["workspaceID"], param_codes, server_name)
                    if not all(['value' in j for j in basin_char_json['parameters']]):
                        if attempt > 4:
                            raise RuntimeError
                        await asyncio.sleep(3 ** attempt)
                        attempt += 1
                    else:
                        working = False
            except Exception as e:
                in_q.put_nowait((pt_id, x, y, pt[3] + 1))
                continue
            
            print(f'{pt_id} - Getting flow statistics')
            # Get flow statistics
            for ind, x in enumerate(scenarios['regressionRegions'][0]['parameters']):
                for p in basin_char_json['parameters']:
                    if x['code'].lower() == p['code'].lower():
                        scenarios['regressionRegions'][0]['parameters'][ind]['value'] = p['value']
            post_body = [scenarios]
            try:
                flow_stats_json, _ = await self.api_client.get_flow_statistics({'regions': self.rcode}, post_body)
            except Exception as e:
                in_q.put_nowait((pt_id, x, y, pt[3] + 1))
                continue
            
            print(f"{pt_id} - Finished processing")
            out_dict = {'ID': pt_id,
                        'globalwatershedpoint': wshed_json['featurecollection'][0],
                        'globalwatershed': wshed_json['featurecollection'][1],
                        'characteristics': basin_char_json['parameters'],
                        'statistics': flow_stats_json[0]['regressionRegions'][0]['results']}
            out_q.put_nowait(out_dict)

    def export_data(self, out_q, id_field):
        # convert q to list
        q = list()
        while not out_q.empty():
            q.append(out_q.get_nowait())

        # put all watersheds into a geodataframe
        keep_fields = ['OBJECTID', 'HYDROID', 'DrainID', 'Descript', 'GlobalWshd', 'RELATEDOIDs', 'WarningMsg', 'HUCID', 'Edited', 'Shape_Length', 'Shape_Ares']
        wshed = [gpd.GeoDataFrame.from_features(i['globalwatershed']['feature']) for i in q]
        wshed = gpd.GeoDataFrame(pd.concat(wshed, ignore_index=True))
        
        # put all outlet points into a geodataframe
        pts = [gpd.GeoDataFrame.from_features(i['globalwatershedpoint']['feature']) for i in q]
        pts = gpd.GeoDataFrame(pd.concat(pts, ignore_index=True))

        # put all characteristics into a dataframe
        characteristics = [pd.json_normalize(i['characteristics'] ) for i in q]
        characteristics = pd.concat(characteristics, ignore_index=True)
        characteristics = gpd.GeoDataFrame(characteristics)

        # put all statistics into a dataframe
        statistics = [pd.json_normalize(i['statistics']) for i in q]
        statistics = pd.concat(statistics, ignore_index=True)
        statistics = gpd.GeoDataFrame(statistics)

        # Export to a geopackage
        wshed.to_file(self.output_file, layer='globalwatershed', driver='GPKG')
        pts.to_file(self.output_file, layer='globalwatershedpoint', driver='GPKG')
        characteristics.to_file(self.output_file, layer='characteristics', driver='GPKG')
        statistics.to_file(self.output_file, layer='statistics', driver='GPKG')
                             
    async def _process_batch_async(self, max_concurrency=10):
        """
        Processes the batch queries by querying the API for each entry in the input file and saving 
        the results.
        """
        print('Initiating batch query')
        input_data = await self.load_input()
        max_concurrency = min(max_concurrency, len(input_data))
        tasks = []
        q = asyncio.Queue()
        out_q = asyncio.Queue()

        for item in input_data[:5]:
            q.put_nowait(item)
        
        for i in range(max_concurrency):
            task = asyncio.create_task(self.point_worker(q, out_q))
            tasks.append(task)

        await asyncio.gather(*tasks)
        self.export_data(out_q, self.unique_field)


    def process_batch(self, max_concurrency=1):
        """
        Processes the batch queries synchronously by querying the API for each entry in the input file 
        and saving the results.
        """
        asyncio.run(self._process_batch_async(max_concurrency))

