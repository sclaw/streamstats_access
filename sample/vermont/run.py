import streamstats_access as ssa
import os


IN_PATH = os.path.join(os.path.dirname(__file__), 'vt_test.gpkg')
OUT_PATH = os.path.join(os.path.dirname(__file__), 'out_pts.gpkg')


ssa.process_batch(IN_PATH, OUT_PATH, rcode='VT', unique_field='UID')