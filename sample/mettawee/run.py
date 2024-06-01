from source import batch_query
import os


IN_PATH = os.path.join(os.path.dirname(__file__), 'in_pts.shp')
OUT_PATH = os.path.join(os.path.dirname(__file__), 'out_pts.gpkg')


batch_query.process_batch(IN_PATH, OUT_PATH, 'VT', 'Name')