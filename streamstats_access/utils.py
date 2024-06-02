import geopandas as gpd
import pandas as pd
import sqlite3
from .models import Point


def load_datasource(in_path, rcode, unique_field):
    """
    Exports data from the output queue to a GeoPackage file.

    Args:
        out_path (str): The path to the output GeoPackage file.
        out_q (queue.Queue): The output queue containing the data to export.

    Returns:
        None
    """
    in_file = gpd.read_file(in_path)
    in_file = in_file.to_crs(epsg=4326)
    crs = in_file.crs.srs.split(':')[1]
    in_file = in_file.set_index(unique_field)
    point_list = [Point(rcode, in_file.loc[i].geometry.x, in_file.loc[i].geometry.y, crs, i, unique_field) for i in in_file.index]
    return point_list

def export_data(out_path, out_q):
    # convert q to list
    q = list()
    while not out_q.empty():
        q.append(out_q.get_nowait())

    # put all watersheds into a geodataframe
    keep_fields = [i['code'] for i in q[0].basin_char_json['parameters']]
    keep_fields.extend(['OBJECTID', 'WarningMsg', 'HUCID', 'Edited', 'geometry'])
    wshed = [i.wshed_gdf() for i in q]
    wshed = gpd.GeoDataFrame(pd.concat(wshed, ignore_index=False))
    wshed = wshed[keep_fields]
    
    # put all outlet points into a geodataframe
    pts = [i.pt_gdf() for i in q]
    pts = gpd.GeoDataFrame(pd.concat(pts, ignore_index=False))

    # put all characteristics into a dataframe
    characteristics = [i.characteristics_df() for i in q]
    characteristics = pd.concat(characteristics, ignore_index=False)
    characteristics = gpd.GeoDataFrame(characteristics)

    # put all statistics into a dataframe
    statistics = [i.statistics_df() for i in q]
    statistics = pd.concat(statistics, ignore_index=False)
    statistics = gpd.GeoDataFrame(statistics)

    # Export to a geopackage
    wshed.to_file(out_path, layer='globalwatershed', driver='GPKG')
    pts.to_file(out_path, layer='globalwatershedpoint', driver='GPKG')
    con = sqlite3.connect(out_path)
    characteristics.to_sql('characteristics', con, if_exists='replace')
    statistics.to_sql('statistics', con, if_exists='replace')
    con.close()
    