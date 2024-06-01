"""
Batch Query Module

This module contains the BatchQueryTool class which provides functionality to process batch 
queries using the USGS Streamstats API.
"""

import asyncio
from .endpoints import USGSEndpoints
from .utils import load_datasource, export_data
import geopandas as gpd


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

async def point_worker(in_q, out_q, server_name, max_retries=5):
    """
    Processes a single point query by querying all necessary apis and saving the result.
    
    Args:
        pt (tuple): A tuple containing the query parameters.
    """

    while not in_q.empty():
        pt = in_q.get_nowait()
        if pt.attempts > max_retries:
            out_q.put_nowait(pt)
        pt.set_server_name(server_name)

        print(f'{pt.id} - Delineating watershed')
        # Delineate watershed
        try:
            await pt._delineate_watershed_async()
        except Exception as e:
            in_q.put_nowait(pt)
            continue

        
        print(f'{pt.id} - Getting regression regions')
        # Get regression regions
        try:
            await pt._get_regression_regions_async()
        except Exception as e:
            in_q.put_nowait(pt)
            continue
        
        print(f'{pt.id} - Getting scenarios')
        # Get scenarios
        try:
            await pt._get_scenarios_async()
        except Exception as e:
            in_q.put_nowait(pt)
            continue
        
        print(f'{pt.id} - Getting basin characteristics')
        # Get basin characteristics
        working = True
        attempt = 1
        try:
            while working:
                await pt._get_basin_characteristics_async()
                if not all(['value' in j for j in pt.basin_char_json['parameters']]):
                    if attempt > 4:
                        raise RuntimeError
                    await asyncio.sleep(3 ** attempt)
                    attempt += 1
                else:
                    working = False
        except Exception as e:
            in_q.put_nowait(pt)
            continue
        
        print(f'{pt.id} - Getting flow statistics')
        # Get flow statistics
        try:
            await pt._get_flow_statistics_async()
        except Exception as e:
            in_q.put_nowait(pt)
            continue
        
        print(f"{pt.id} - Finished processing")
        out_q.put_nowait(pt)


                             
async def _process_batch_async(in_path, out_path, rcode, unique_field, parallel=True):
    """
    Processes the batch queries by querying the API for each entry in the input file and saving 
    the results.
    """
    print('Initiating batch query')
    input_data = load_datasource(in_path, rcode, unique_field)
    tasks = []
    q = asyncio.Queue()
    out_q = asyncio.Queue()

    for item in input_data[:2]:
        q.put_nowait(item)
    
    if parallel:
        servers = ['prodweba', 'prodwebb']
    else:
        servers = ['prodweba']

    for s in servers:
        task = asyncio.create_task(point_worker(q, out_q, s))
        tasks.append(task)

    await asyncio.gather(*tasks)
    print('Finished processing batch queries.  Exporting data')
    export_data(out_path, out_q)


def process_batch(in_path, out_path, rcode, unique_field, parallel=True):
    """
    Processes the batch queries synchronously by querying the API for each entry in the input file 
    and saving the results.
    """
    asyncio.run(_process_batch_async(in_path, out_path, rcode, unique_field, parallel))

