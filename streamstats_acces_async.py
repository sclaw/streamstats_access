import requests
from osgeo import ogr, osr
import os
import asyncio


IN_PATH = r"C:\Users\Kensf\Documents\StreamStats\Missisquoi\batches\scott_r&d\round_2.shp"
OUT_PATH = r"C:\Users\Kensf\Documents\StreamStats\Missisquoi\batches\scott_r&d\round_2.csv"


async def delineation_worker(in_queue, out_queue, session, url, rcode, crs, include_params=True, include_flow_types=False, include_features=False,
                              simplify=True, w_credentials=True):
    while True:
        data_in = await in_queue.get()
        id = data_in['code']
        x = data_in['x']
        y = data_in['y']
        print(f'Delineating watershed for point @ x = {x} and y = {y}')

        delin_params = {
            'rcode': rcode,
            'xlocation': str(x),
            'ylocation': str(y),
            'crs': str(crs),
            'includeparameters': str(include_params),
            'includeflowtypes': str(include_flow_types),
            'includefeatures': str(include_features),
            'simplify': str(simplify),
            'withCredentials': str(w_credentials)
        }
        loop = asyncio.get_event_loop()
        future = loop.run_in_executor(session.get(url, params=delin_params))
        print('here')
        delin_response = await future
        #delin_response = await session.get(url, params=delin_params)
        print(delin_response.json)
        out_queue.put(delin_response.json)

        in_queue.task_done()


async def flowstat_worker(in_queue, out_queue, session, url, rcode, include_flow_types=True, w_credentials=True):
    while True:
        workspace_id = await in_queue.get()
        print(f'Calculating flowstats for {workspace_id}')
        flow_params = {
            'rcode': rcode,
            'workspaceID': workspace_id,
            'includeflowtypes': str(include_flow_types),
            'withCredentials': str(w_credentials)
        }

        flow_response = session.get(url, params=flow_params)
        out_queue.put(flow_response.json())

        in_queue.task_done()


async def output_worker(in_queue, out_shapefile):
    while True:
        params = await in_queue.get()
        print(params)

        in_queue.task_done()


class StreamStats:

    def __init__(self, rcode, unique_code, server='prodweba'):
        self.delin_base_url = f'https://{server}.streamstats.usgs.gov/streamstatsservices/watershed.geojson'
        self.flow_base_url = f'https://{server}.streamstats.usgs.gov/streamstatsservices/flowstatistics.json'
        self.rcode = rcode
        self.unique_code = unique_code
        self.crs = None
        self.session = requests.Session()

        self.previously_computed = list()
        self.point_queue = None
        self.ws_list = None
        self.flow_list = None

    async def init_queue(self, workers):
        tasks = []
        for i in range(workers):
            pt_task = asyncio.create_task(
                delineation_worker(self.point_queue, self.ws_list, self.session, self.delin_base_url, self.rcode,
                                   self.crs))
            tasks.append(pt_task)
            ws_task = asyncio.create_task(
                flowstat_worker(self.ws_list, self.flow_list, self.session, self.flow_base_url, self.rcode))
            tasks.append(ws_task)
        output_task = asyncio.create_task(output_worker(self.flow_list, 'tmp'))
        tasks.append(output_task)

        await self.point_queue.join()
        await self.ws_list.join()
        await self.flow_list.join()

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

    def shp_to_list(self, shp_path):
        driver = ogr.GetDriverByName("ESRI Shapefile")
        dataSource = driver.Open(shp_path, 0)
        layer = dataSource.GetLayer()
        wkt = layer.GetSpatialRef().ExportToWkt()
        proj = osr.SpatialReference()
        proj.ImportFromWkt(wkt)
        self.crs = proj.GetAuthorityCode(None)

        for feature in layer:
            geom = feature.GetGeometryRef()
            id_code = feature.GetField(self.unique_code)
            if id_code not in self.previously_computed:
                self.point_queue.put_nowait({'code': id_code, 'x': geom.GetX(), 'y': geom.GetY()})

        print(f'imported {self.point_queue.qsize()} points from {shp_path}')

    def get_previously_computed(self, csv_path):
        if not os.path.exists(csv_path):
            return
        with open(csv_path) as csv_file:
            self.previously_computed = csv_file.readlines()

    async def run_batch(self, in_path, out_path, worker_count=4):
        # initiate within current loop
        self.point_queue = asyncio.Queue()
        self.ws_list = asyncio.Queue()
        self.flow_list = asyncio.Queue()

        self.get_previously_computed(out_path)
        self.shp_to_list(in_path)

        await self.init_queue(worker_count)


def main():
    ss = StreamStats('vt', 'Name')
    asyncio.run(ss.run_batch(IN_PATH, OUT_PATH))


if __name__ == '__main__':
    main()
