# streamstats_access: Python access to the USGS Streamstats API
<p align=center>
<img src="images/logo_nobg_txt.png" alt="StreamStats API Python library" width="350">
</p>

## Overview

This library provides users access to most functionality of the USGS Streamstats application from within Python.  The Streamstats app includes a suite of GIS-based hydrologic tools that are useful for water resources planning/management, engineering, and design purposes.

## Key Features

- **Watershed Delineation:** Use streamstats GIS services to delineate watershed polygons from an outlet point.
- **Basin Characteristics:** Retrieve basin characteristics such as drainage area, percent wetlands, slope, mean annual precipitation, etc from a delineated watershed.
- **Flow Statistics:** Use basin characteristics to derive USGS flow frequency estimates.
- **A Better Batch Point Tool:**   Submit large batches of points at once.  This library mimics the functionality of the StreamStats batch tool.
  - Avoid the 250 point limit of the USGS Streamstats Batch Tool.
  - The library implements asynchronous requests offering 2x speed compared to the batch tool.
  - Avoid the queue.  The library directly queries USGS API endpoints, meaning you don't need to wait in the batch point queue.

## Installation

To install and run the probHAND model, you may use either pip or Anaconda:

1. Pip Installation:
    ```sh
    pip install streamstats-access
    ```
2. Anaconda Installation (coming soon!):
    ```bash
    # conda install conda_forge:streamstats_access
    ```

## Example Usage

See samples folder for data and scripts.

```python
import streamstats_access as ssa
import os


IN_PATH = os.path.join(os.path.dirname(__file__), 'vt_test.gpkg')
OUT_PATH = os.path.join(os.path.dirname(__file__), 'out_pts.gpkg')


ssa.process_batch(IN_PATH, OUT_PATH, rcode='VT', unique_field='UID')
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contact

For any questions or issues, please direct inquiries to Scott Lawson([Scott.Lawson@uvm.edu](mailto:Scott.Lawson@uvm.edu)).