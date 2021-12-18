import time
import logging
import requests
from osgeo import ogr, osr
import os
import csv


IN_PATH = r"C:\Users\Kensf\Documents\StreamStats\access_r&d\MSQ_r1_b1.shp"


class StreamStats:

    def __init__(self, rcode, unique_code, server='prodweba'):
        self.delin_base_url = f'https://{server}.streamstats.usgs.gov/streamstatsservices/watershed.geojson'
        self.flow_base_url = f'https://{server}.streamstats.usgs.gov/streamstatsservices/flowstatistics.json'
        self.rcode = rcode
        self.unique_code = unique_code
        self.crs = None
        self.session = requests.Session()

        self.previously_computed = list()
        self.data_list = dict()

    def delineate_ws(self, code, include_params=True, include_flow_types=False, include_features=True, simplify=True, w_credentials=True):
        x = self.data_list[code]['x']
        y = self.data_list[code]['y']
        logging.info(f'Delineating watershed for point @ x = {x} and y = {y}')

        delin_params = {
            'rcode': self.rcode,
            'xlocation': str(x),
            'ylocation': str(y),
            'crs': str(self.crs),
            'includeparameters': str(include_params),
            'includeflowtypes': str(include_flow_types),
            'includefeatures': str(include_features),
            'simplify': str(simplify),
            'withCredentials': str(w_credentials)
        }

        # Query API  timeout after 5 tries
        attempts = 1
        response_valid = False
        while attempts < 5 and not response_valid:
            t1 = time.perf_counter()
            try:
                delin_response = self.session.get(self.delin_base_url, params=delin_params, timeout=61)
            except requests.exceptions.ReadTimeout:
                pass
            else:
                logging.debug(
                    f'{code} took {round(time.perf_counter() - t1, 3)} seconds for delineation attempt {attempts}')
                if delin_response.status_code == 200:
                    response_valid = True
            attempts += 1
        if not response_valid:
            raise RuntimeError(f'status code error or timeout:  delineation | statuscode: {delin_response.status_code}')

        response_json = delin_response.json()
        self.data_list[code]['workspaceID'] = response_json['workspaceID']
        if include_params:
            for i in response_json['parameters']:
                parameter = i['name']
                self.data_list[code][parameter] = i['value']
        self.data_list[code]['ws'] = response_json['featurecollection'][1]['feature']['features'][0]['geometry']['coordinates'][0]
        return

    def generate_flowstats(self, code, include_flow_types=True, w_credentials=True):
        workspace_id = self.data_list[code]['workspaceID']
        logging.info(f'Calculating flowstats for {workspace_id}')
        flow_params = {
            'rcode': self.rcode,
            'workspaceID': workspace_id,
            'includeflowtypes': str(include_flow_types),
            'withCredentials': str(w_credentials)
        }
        # Query API  timeout after 5 tries
        attempts = 1
        response_valid = False
        while attempts < 5 and not response_valid:
            t1 = time.perf_counter()
            try:
                flow_response = self.session.get(self.flow_base_url, params=flow_params, timeout=61)
            except requests.exceptions.ReadTimeout:
                pass
            else:
                logging.debug(
                    f'{code} took {round(time.perf_counter() - t1, 3)} seconds for flowstats attempt {attempts}')
                if flow_response.status_code == 200:
                    response_valid = True
            attempts += 1
        if not response_valid:
            raise RuntimeError(f'status code error or timeout:  flowstats | statuscode: {flow_response.status_code}')

        response_json = flow_response.json()
        self.data_list[code]['region_code'] = response_json[0]['RegressionRegions'][0]['Code']
        for i in response_json[0]['RegressionRegions'][0]['Parameters']:
            parameter = i['Name']
            self.data_list[code][parameter] = i['Value']
        self.data_list[code]['flowstats'] = response_json[0]['RegressionRegions'][0]['Results']

    def export_outlets(self, code, out_path):
        driver = ogr.GetDriverByName("ESRI Shapefile")
        layer_name = 'ss_pts'
        out_path = os.path.join(out_path, f'{layer_name}.shp')

        if os.path.exists(out_path):
            out_data_source = driver.Open(out_path, 1)
            out_layer = out_data_source.GetLayer()
        else:
            out_data_source = driver.CreateDataSource(out_path)
            out_layer = out_data_source.CreateLayer(layer_name, geom_type=ogr.wkbLineString)

            id_field = ogr.FieldDefn('Code', ogr.OFTString)
            out_layer.CreateField(id_field)

        # Create line geometry
        line = ogr.Geometry(ogr.wkbLineString)
        line.AddPoint(self.data_list[code]['x'], self.data_list[code]['y'])
        line.AddPoint(self.data_list[code]['OUTLETX'], self.data_list[code]['OUTLETY'])

        # Create the feature and set values
        feature_definition = out_layer.GetLayerDefn()
        out_feature = ogr.Feature(feature_definition)
        out_feature.SetGeometry(line)
        out_feature.SetField('Code', code)
        out_layer.CreateFeature(out_feature)

        out_feature = None

    def export_watersheds(self, code, out_path):
        driver = ogr.GetDriverByName("ESRI Shapefile")
        layer_name = 'ss_watersheds'
        out_path = os.path.join(out_path, f'{layer_name}.shp')

        if os.path.exists(out_path):
            out_data_source = driver.Open(out_path, 1)
            out_layer = out_data_source.GetLayer()
        else:
            out_data_source = driver.CreateDataSource(out_path)
            out_layer = out_data_source.CreateLayer(layer_name, geom_type=ogr.wkbPolygon)
            id_field = ogr.FieldDefn('Code', ogr.OFTString)
            out_layer.CreateField(id_field)

        # Create line geometry
        ring = ogr.Geometry(ogr.wkbLinearRing)
        for point in self.data_list[code]['ws']:
            ring.AddPoint(*point)
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)

        # Create the feature and set values
        feature_definition = out_layer.GetLayerDefn()
        out_feature = ogr.Feature(feature_definition)
        out_feature.SetGeometry(poly)
        out_feature.SetField('Code', code)
        out_layer.CreateFeature(out_feature)

        out_feature = None

    def export_flowstats(self, code, out_dir):
        out_path = os.path.join(out_dir, 'ss_stats.csv')
        if os.path.exists(out_path):
            add_headers = False
        else:
            add_headers = True
        with open(out_path, mode='a', newline='') as output_file:
            writer = csv.writer(output_file)
            if add_headers:
                writer.writerow(['Name', 'RegionID', 'AreaSqMi', 'StatLabel', 'StatName', 'Value', 'Units', 'PII', 'PIu'])
            for stat in self.data_list[code]['flowstats']:
                writer.writerow([code, self.data_list[code]['region_code'],
                                 self.data_list[code]['Drainage Area'],
                                 stat['code'],
                                 stat['Name'],
                                 stat['Value'],
                                 'cubic feet per second'])
                                 #stat['IntervalBounds']['Lower'],
                                 #stat['IntervalBounds']['Upper']]
                                #)

    def export_parameters(self, code, out_dir):
        out_path = os.path.join(out_dir, 'ss_parameters.csv')
        if os.path.exists(out_path):
            add_headers = False
        else:
            add_headers = True
        with open(out_path, mode='a', newline='') as output_file:
            writer = csv.writer(output_file)
            if add_headers:
                writer.writerow(['Code', 'Drainage Area (sqmi)', 'Percent Storage', 'Mean Annual Precipitation (in)'])
            writer.writerow([code, self.data_list[code]['Drainage Area'], self.data_list[code]['Percent Storage from NLCD2006'], self.data_list[code]['Mean Annual Precip PRISM 1981 2010']])

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
                self.data_list[id_code] = {'x': geom.GetX(), 'y': geom.GetY()}

        print(f'imported {len(self.data_list)} points from {shp_path}')

    def get_previously_computed(self, out_dir):
        out_path = os.path.join(out_dir, 'ss_parameters.csv')
        if not os.path.exists(out_path):
            return
        with open(out_path) as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                if row:
                    self.previously_computed.append(row[0])
            del self.previously_computed[0]

    def run_batch(self, in_path):
        working_directory = os.path.split(in_path)[0]
        logging.basicConfig(filename=os.path.join(working_directory, 'ss.log'), level=logging.DEBUG)
        logging.info(f'Run started {time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())}')

        self.get_previously_computed(working_directory)
        self.shp_to_list(in_path)

        # Run batch.
        counter = 1
        t_start = time.perf_counter()
        time_left = 0
        for code in self.data_list:
            print(f'processing basin {counter} / {len(self.data_list)} | estimated time remaining: {time_left} seconds')
            try:
                self.delineate_ws(code)
                self.generate_flowstats(code)
            except (RuntimeError, KeyError) as error:
                logging.error(f'code {code} failed:  {type(error)}')
            else:
                self.export_outlets(code, working_directory)
                if self.data_list[code]['ws']:
                    self.export_watersheds(code, working_directory)
                self.export_flowstats(code, working_directory)
                self.export_parameters(code, working_directory)
            time_left = round(((time.perf_counter() - t_start) / counter) * (len(self.data_list) - counter), 2)
            counter += 1

    def run_batch_debug(self, in_path):
        working_directory = os.path.split(in_path)[0]
        logging.basicConfig(filename=os.path.join(working_directory, 'ss.log'), level=logging.DEBUG)
        logging.info(f'Run started {time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())}')

        self.get_previously_computed(working_directory)
        self.shp_to_list(in_path)

        # Run batch.
        counter = 1
        t_start = time.perf_counter()
        time_left = 0
        for code in self.data_list:
            print(f'processing basin {counter} / {len(self.data_list)} | estimated time remaining: {time_left} seconds')

            self.delineate_ws(code)
            self.generate_flowstats(code)

            self.export_outlets(code, working_directory)
            if self.data_list[code]['ws']:
                self.export_watersheds(code, working_directory)
            self.export_flowstats(code, working_directory)
            self.export_parameters(code, working_directory)
            time_left = round(((time.perf_counter() - t_start) / counter) * (len(self.data_list) - counter), 2)
            counter += 1


def main():
    ss = StreamStats('vt', 'Code')
    ss.run_batch(IN_PATH)


if __name__ == '__main__':
    main()
