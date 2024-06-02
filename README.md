# streamstats_access: Python access to the USGS Streamstats API
<p align=center>
<img src="images/logo_nobg_text.tif" alt="StreamStats API Python library" width="350">
</p>

## Overview

This library provides allows users access to most functionality of the USGS Streamstats application from within Python.  The Streamstats app includes a suite of GIS-based hydrologic tools that are useful for water resources planning/management, engineering, and design purposes.

## Key Features

- **Watershed Delineation:** Use streamstats GIS services to delineate watershed polygons from an outlet point.
- **Basin Characteristics:** Retrieve basin characteristics such as drainage area, percent wetlands, slope, mean annual precipitation from a delineated watershed.
- **Flow Statistics:** Use basin characteristics to access USGS flow frequency estimates.
- **Batch Point Tool:** Submit large batches of points at once, and avoid the 250 point limit of the USGS Streamstats Batch Tool.  The library implements asynchronous requests offering 2x speed compared to the batch tool (plus no queue wait times)!

## Installation

To install and run the probHAND model, you may use either pip or Anaconda:

1. Pip Installation:
    ```sh
    pip install streamstats_access
    ```
2. Anaconda Installation:
    ```bash
    conda install conda_forge:streamstats_access
    ```


## Usage


## Speed Comparison

## License



## Contact

For any questions or issues, please direct inquiries to Scott Lawson([Scott.Lawson@uvm.edu](mailto:Scott.Lawson@uvm.edu)).