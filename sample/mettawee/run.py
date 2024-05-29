from source import streamstats_access, batch_query
import os

IN_PATH = os.path.join(os.path.dirname(__file__), 'in_pts.shp')


# streamstats_access.run_batch('VT', 'Name', IN_PATH)
bq = batch_query.BatchQueryTool(IN_PATH, r"C:\Users\klawson1\OneDrive - University of Vermont\Desktop\vt_ss\vt_streamstats_batch\sample\mettawee\out_pts.json", 'Name', 'VT')
bq.process_batch(5)