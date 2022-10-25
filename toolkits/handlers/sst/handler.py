import os
import json
import fsspec
import numpy as np
import xarray as xr
from typing import Union
from .. import collections


dir_path = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(dir_path, 'config.json'), 'r') as config_file:
    config_data = json.load(config_file)
    STORE_PATH = config_data.get('store_path')
    CONSISTENT_VARS = set(config_data.get('constants')['consistent_vars'])


class SST(collections.Dataset):
    def __init__(self, logger):
        self.store_path = STORE_PATH
        self.logger = logger

    def processor(self, dataset: xr.Dataset) -> xr.Dataset:
        available_vars = set(dataset.variables)
        to_drop_vars = list(available_vars - CONSISTENT_VARS)
        processed_ds = dataset.drop_vars(to_drop_vars)
        return processed_ds

    def _get_dim_value(self, ds: xr.Dataset) -> str:
        return ds.time.values[0]

    def get_region_index(self, zarr_store: fsspec.FSMap, file_ds: xr.Dataset) -> Union[dict[str, slice], None]:
        zarr_ds = xr.open_zarr(zarr_store)
        try:
            self.logger.info(f"identifying region index of the NetCDF if it's previously ingested to Zarr store")
            idx = np.argmax(zarr_ds.time.values == self._get_dim_value(file_ds))
            self.logger.info(f"region index: {idx}")
            dict_obj = {"time": slice(idx, idx + 1),  # only process 1 file at a time
                        "lat": slice(0, file_ds.dims['lat']),
                        "lon": slice(0, file_ds.dims['lon'])}
            return dict_obj
        except:
            return None

    def get_store_path(self):
        return self.store_path
