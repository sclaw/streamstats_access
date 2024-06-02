"""
Configuration Module

This module handles the configuration settings for the USGS API client, including loading settings 
from a JSON file.
"""

import json
import os

CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.json')

def load_config():
    """
    Loads configuration settings from a JSON file.
    
    Returns:
        dict: A dictionary of configuration settings.
    """
    with open(CONFIG_PATH) as config_file:
        return json.load(config_file)

config = load_config()