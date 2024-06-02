"""
streamstats_access Package

This package provides tools to access and process data from the USGS StreamStats API.

Modules:
    batch_query: Contains functions for processing batch queries.
    endpoints: Contains classes and methods to interact with USGS API endpoints.
    models: Contains data models used in the package.

Exports:
    process_batch (function): Processes batch queries.
    USGSEndpoints (class): Provides methods to interact with USGS API endpoints.
    Point (class): Represents a geographical point with associated USGS data.
"""

from .batch_query import process_batch
from .endpoints import USGSEndpoints
from .models import Point

__all__ = ['process_batch', 'USGSEndpoints', 'Point']
