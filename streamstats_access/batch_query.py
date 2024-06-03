"""
Batch Query Module

This module contains the BatchQueryTool class which provides functionality to process batch 
queries using the USGS Streamstats API.  This mimics the functionality of the Batch Processor Tool.
"""

import asyncio
from .utils import load_datasource, export_data
import os
import logging


async def point_worker(in_q, out_q, server_name, max_retries=3):
    """
    Processes a single point by querying all necessary APIs and logging the result.
    
    Args:
        in_q (asyncio.Queue): The queue containing points to process.
        out_q (asyncio.Queue): The queue to put processed points in.
        server_name (str): The name of the server to query.  Essentially, ID for worker.
        max_retries (int, optional): The maximum number of times to retry a failed request. Defaults to 3.
    """
    while not in_q.empty():
        pt = in_q.get_nowait()
        logging.info(f'{server_name}: Processing {pt} | Attempt: {pt.attempts}')
        if pt.attempts > max_retries:
            logging.info(f'{server_name}: Too many tries ({pt.attempts}) {pt}')
            out_q.put_nowait(pt)
            continue
        pt.set_server_name(server_name)

        # Delineate watershed
        try:
            logging.debug(f'{server_name}: Delineating {pt}')
            await pt._delineate_watershed_async()
            logging.debug(f'{server_name}: Finished delineating {pt}')
        except Exception as e:
            logging.debug(f'{server_name}: Failed delineating {pt} | {e}')
            pt.attempts += 1
            in_q.put_nowait(pt)
            continue

        
        # Get regression regions
        try:
            logging.debug(f'{server_name}: Getting regression region {pt}')
            await pt._get_regression_regions_async()
            logging.debug(f'{server_name}: Finished regression region {pt}')
        except Exception as e:
            logging.debug(f'{server_name}: Failed regression region {pt} | {e}')
            pt.attempts += 1
            in_q.put_nowait(pt)
            continue
        
        # Get scenarios
        try:
            logging.debug(f'{server_name}: Getting scenarios {pt}')
            await pt._get_scenarios_async()
            logging.debug(f'{server_name}: Finished scenarios {pt}')
        except Exception as e:
            logging.debug(f'{server_name}: Failed scenarios {pt} | {e}')
            pt.attempts += 1
            in_q.put_nowait(pt)
            continue
        
        # Get basin characteristics
        logging.debug(f'{server_name}: Getting characteristics {pt}')
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
                    logging.debug(f'{server_name}: Finished characteristics {pt}')
        except Exception as e:
            logging.debug(f'{server_name}: Failed characteristics {pt} | {e}')
            pt.attempts += 1
            in_q.put_nowait(pt)
            continue
        
        # Get flow statistics
        try:
            logging.debug(f'{server_name}: Getting flowstats {pt}')
            await pt._get_flow_statistics_async()
            logging.debug(f'{server_name}: Finished flowstats {pt}')
        except Exception as e:
            logging.debug(f'{server_name}: Failed flowstats {pt} | {e}')
            pt.attempts += 1
            in_q.put_nowait(pt)
            continue

        logging.info(f'{server_name}: Finished processing {pt}')
        out_q.put_nowait(pt)
        logging.debug(f'{server_name}: Finished putting in out queue {pt}')
        


                             
async def _process_batch_async(in_path, out_path, rcode, unique_field, parallel=True):
    """
    Processes the batch query by querying the API for each point in the input file and saving 
    the results.

    Args:
        in_path (str): filepath to load points from
        out_path (str): filepath to save results to
        rcode (str): the region code to use
        unique_field (str): the field in the input geospatial file that contains unique identifiers for each point
        parallel (bool, optional): whether to asynchronously query prodweba and prodwebb. Defaults to True.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.FileHandler(os.path.join(os.path.dirname(in_path), 'ssa.log')), logging.StreamHandler()])
    logging.info('Initiating batch query')
    input_data = load_datasource(in_path, rcode, unique_field)
    tasks = []
    q = asyncio.Queue()
    out_q = asyncio.Queue()

    for item in input_data:
        q.put_nowait(item)
    
    if parallel:
        servers = ['prodweba', 'prodwebb']
    else:
        servers = ['prodweba']

    for s in servers:
        task = asyncio.create_task(point_worker(q, out_q, s))
        tasks.append(task)

    await asyncio.gather(*tasks)
    logging.info('Finished processing batch queries')
    export_data(out_path, out_q)


def process_batch(in_path, out_path, rcode, unique_field, parallel=True):
    """
    User entrypoint to the batch processor tool.

    Args:
        in_path (str): filepath to load points from
        out_path (str): filepath to save results to
        rcode (str): the region code to use
        unique_field (str): the field in the input geospatial file that contains unique identifiers for each point
        parallel (bool, optional): whether to asynchronously query prodweba and prodwebb. Defaults to True.
    """
    asyncio.run(_process_batch_async(in_path, out_path, rcode, unique_field, parallel))

