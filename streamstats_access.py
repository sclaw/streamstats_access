import time
import logging
import requests
from osgeo import ogr, osr
import os
import csv
from http.cookies import SimpleCookie


IN_PATH = r"C:\Users\Kensf\Documents\StreamStats\access_r&d\round_2\round_2.shp"


class RegressionPoint:

    def __init__(self, name, x, y, crs):
        self.name = name
        self.input_x = x
        self.input_y = y
        self.crs = crs
        self.rcode = None

        self.attempts = 0
        self.workspace_id = None
        self.server_name = None
        self.delin_url = 'https://streamstats.usgs.gov/streamstatsservices/watershed.geojson'
        self.flow_base_url = 'https://{}.streamstats.usgs.gov/streamstatsservices/flowstatistics.json'
        self.flow_url = None
        self.cookies = dict()

        self.region_id = None
        self.region_name = None

        self.watershed = None

        self.outlet_x = None
        self.outlet_y = None

        self.area_percent = None
        self.area = None
        self.area_unit = None
        self.area_min_applicable = None
        self.area_max_applicable = None

        self.percent_storage = None
        self.storage_min_applicable = None
        self.storage_max_applicable = None

        self.mean_precipitation = None
        self.precipitation_unit = None
        self.precipitation_min_applicable = None
        self.precipitation_max_applicable = None

        self.flow_regressions = dict()

    def append_watershed(self, response):
        # Parse and add cookies
        cookie = SimpleCookie()
        cookie.load(response.headers['set-cookie'])
        self.cookies = {key: value.value for key, value in cookie.items()}

        # Determine server from header
        self.server_name = response.headers['USGSWiM-HostName'].lower()
        self.flow_url = self.flow_base_url.format(self.server_name)

        # Parse response content
        json = response.json()
        self.workspace_id = json['workspaceID']
        self.watershed = json['featurecollection'][1]['feature']['features'][0]['geometry']['coordinates'][0]

        for param in json['parameters']:
            if param['name'] == 'OUTLETX':
                self.outlet_x = param['value']
            if param['name'] == 'OUTLETY':
                self.outlet_y = param['value']
        return

    def append_flowstats(self, response):
        json = response.json()
        self.region_id = json[0]['RegressionRegions'][0]['Code']
        self.region_name = json[0]['RegressionRegions'][0]['Name']
        self.area_percent = json[0]['RegressionRegions'][0].get('PercentWeight')
        for param in json[0]['RegressionRegions'][0]['Parameters']:
            if param['Name'] == 'Drainage Area':
                self.area = param['Value']
                self.area_unit = param['UnitType']['unit']
                self.area_min_applicable = param['Limits']['min']
                self.area_max_applicable = param['Limits']['max']
            if param['Name'] == 'Percent Storage from NLCD2006':
                self.percent_storage = param['Value']
                self.storage_min_applicable = param['Limits']['min']
                self.storage_max_applicable = param['Limits']['min']
            if param['Name'] == 'Mean Annual Precip PRISM 1981 2010':
                self.mean_precipitation = param['Value']
                self.precipitation_unit = param['UnitType']['unit']
                self.precipitation_min_applicable = param['Limits']['min']
                self.precipitation_max_applicable = param['Limits']['max']
        self.flow_regressions = json[0]['RegressionRegions'][0]['Results']


def get_previously_computed(working_drectory):
    previously_computed = list()
    out_path = os.path.join(working_drectory, 'ss_parameters.csv')
    if not os.path.exists(out_path):
        return []
    with open(out_path) as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            if row:
                previously_computed.append(row[0])
        del previously_computed[0]
    return previously_computed


def shp_to_list(shp_path, unique_field, ignore_list):
    point_list = list()

    driver = ogr.GetDriverByName("ESRI Shapefile")
    data_source = driver.Open(shp_path, 0)
    layer = data_source.GetLayer()
    wkt = layer.GetSpatialRef().ExportToWkt()
    proj = osr.SpatialReference()
    proj.ImportFromWkt(wkt)
    crs = proj.GetAuthorityCode(None)

    for feature in layer:
        geom = feature.GetGeometryRef()
        id_code = feature.GetField(unique_field)
        if id_code not in ignore_list:
            point_list.append(RegressionPoint(id_code, geom.GetX(), geom.GetY(), crs))

    return point_list


def delineate_ws(session, queues, include_params=True, include_flow_types=False, include_features=True, simplify=True):
    #  Get first point of queue and check if it is valid.
    point = queues['input_queue'][0]
    if point.attempts == 5:
        queues['error_list'].append(point)
        del queues['input_queue'][0]
        return
    logging.info(f'Delineating watershed for point @ x = {point.input_x} and y = {point.input_y}')

    # Set up request
    delin_params = {
        'rcode': str(point.rcode),
        'xlocation': str(point.input_x),
        'ylocation': str(point.input_y),
        'crs': str(point.crs),
        'includeparameters': str(include_params),
        'includeflowtypes': str(include_flow_types),
        'includefeatures': str(include_features),
        'simplify': str(simplify)
    }

    # Query API
    t1 = time.perf_counter()
    try:
        delineation_response = session.get(point.delin_url, params=delin_params, timeout=61)
    except requests.exceptions.ReadTimeout:
        print(f'timeout on {point.name}')
        point.attempts += 1
        queues['input_queue'].append(point)
        del queues['input_queue'][0]
        return
    else:
        logging.debug(
            f'{point.name} took {round(time.perf_counter() - t1, 3)} seconds for delineation attempt {point.attempts}')
        if delineation_response.status_code != 200:
            print(f'response code {delineation_response.status_code} on {point.name} delineation')
            point.attempts += 1
            queues['input_queue'].append(point)
            del queues['input_queue'][0]
            return
    # Parse response data and append to point class
    try:
        point.append_watershed(delineation_response)
    except (KeyError, IndexError):
        print(f'key error on delineation for {point.name}')
        point.attempts += 1
        queues['input_queue'].append(point)
        del queues['input_queue'][0]
        return
    else:
        print(f'succesful delineation for {point.name}')
        point.attempts = 0
        queues['flowstat_queue'].append(point)
        del queues['input_queue'][0]
        return


def generate_flowstats(session, queues, include_flow_types=True):
    #  Get first point of queue and check if it is valid.
    point = queues['flowstat_queue'][0]
    if point.attempts == 5:
        queues['error_list'].append(point)
        del queues['flowstat_queue'][0]
        return
    logging.info(f'Calculating flowstats for {point.workspace_id}')

    flow_params = {
        'rcode': str(point.rcode).upper(),
        'workspaceID': str(point.workspace_id).upper(),
        'includeflowtypes': str(include_flow_types).lower()
    }

    t1 = time.perf_counter()
    try:
        flow_response = session.get(point.flow_url, params=flow_params, timeout=61)
    except requests.exceptions.ReadTimeout:
        print(f'timeout on {point.name}')
        point.attempts += 1
        queues['flowstat_queue'].append(point)
        del queues['flowstat_queue'][0]
        return
    else:
        logging.debug(
            f'{point.name} took {round(time.perf_counter() - t1, 3)} seconds for flowstats attempt {point.attempts}')
        if flow_response.status_code != 200:
            print(f'response {flow_response.status_code} on {point.name} flowstats')
            point.attempts += 1
            queues['flowstat_queue'].append(point)
            del queues['flowstat_queue'][0]
            return
    try:
        point.append_flowstats(flow_response)
    except KeyError:
        print(f'key error on flowstats for {point.name}')
        point.attempts += 1
        queues['flowstat_queue'].append(point)
        del queues['flowstat_queue'][0]
        return
    else:
        print(f'succesful flowstats for {point.name}')
        queues['export_queue'].append(point)
        del queues['flowstat_queue'][0]
        return


def export_outlets(point, out_path):
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
    line.AddPoint(point.input_x, point.input_y)
    line.AddPoint(point.outlet_x, point.outlet_y)

    # Create the feature and set values
    feature_definition = out_layer.GetLayerDefn()
    out_feature = ogr.Feature(feature_definition)
    out_feature.SetGeometry(line)
    out_feature.SetField('Code', point.name)
    out_layer.CreateFeature(out_feature)

    out_feature = None


def export_watersheds(point, out_path):
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
    for point in point.watershed:
        ring.AddPoint(*point)
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)

    # Create the feature and set values
    feature_definition = out_layer.GetLayerDefn()
    out_feature = ogr.Feature(feature_definition)
    out_feature.SetGeometry(poly)
    out_feature.SetField('Code', point.name)
    out_layer.CreateFeature(out_feature)

    out_feature = None


def export_flowstats(point, out_dir):
    out_path = os.path.join(out_dir, 'ss_stats.csv')
    if os.path.exists(out_path):
        add_headers = False
    else:
        add_headers = True
    with open(out_path, mode='a', newline='') as output_file:
        writer = csv.writer(output_file)
        if add_headers:
            writer.writerow(['Name', 'RegionID', 'RegionName', 'AreaPercent', 'AreaSqMi', 'StatLabel', 'StatName', 'Value', 'Units', 'PII', 'PIu'])
        for stat in point.flow_regressions:
            writer.writerow([point.name,
                             point.region_id,
                             point.region_name,
                             point.area_percent,
                             point.area,
                             stat['Label'],
                             stat['Name'],
                             stat['Value'],
                             stat['Units'],
                             stat['lower'],
                             stat['upper']])


def export_parameters(point, out_dir):
    out_path = os.path.join(out_dir, 'ss_parameters.csv')
    if os.path.exists(out_path):
        add_headers = False
    else:
        add_headers = True
    with open(out_path, mode='a', newline='') as output_file:
        writer = csv.writer(output_file)
        if add_headers:
            writer.writerow(['Code', 'Drainage Area (sqmi)', 'Percent Storage', 'Mean Annual Precipitation (in)'])
        writer.writerow([point.name, point.area, point.percent_storage, point.mean_precipitation])


def run_batch(rcode, unique_field, in_path):
    # Set up logging
    working_directory = os.path.split(in_path)[0]
    logging.basicConfig(filename=os.path.join(working_directory, 'ss.log'), level=logging.DEBUG)
    logging.info(f'Run started {time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())}')

    # Import data
    ignore_list = get_previously_computed(working_directory)
    point_inputs = shp_to_list(in_path, unique_field, ignore_list)

    # Create queues
    queue_cluster = {'input_queue': point_inputs, 'flowstat_queue': list(), 'export_queue': list(), 'error_list': list()}
    for pt in queue_cluster['input_queue']:
        pt.rcode = rcode

    session = requests.Session()

    done = False
    while not done:
        print(len(queue_cluster['input_queue']))
        if queue_cluster['input_queue']:
            delineate_ws(session, queue_cluster)
        if queue_cluster['flowstat_queue']:
            generate_flowstats(session, queue_cluster)
        if queue_cluster['export_queue']:
            point = queue_cluster['export_queue'][0]
            export_outlets(point, working_directory)
            export_watersheds(point, working_directory)
            export_watersheds(point, working_directory)
            export_flowstats(point, working_directory)
            del queue_cluster['export_queue'][0]

        if not queue_cluster['input_queue'] and not queue_cluster['flowstat_queue'] and not queue_cluster['export_queue']:
            done = True


def main():
    run_batch('VT', 'Name', IN_PATH)


if __name__ == '__main__':
    main()
