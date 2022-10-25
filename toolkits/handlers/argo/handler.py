import os
import json
import fsspec
import numpy as np
import xarray as xr
from typing import Union
from functools import partial
from .. import collections


dir_path = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(dir_path, 'config.json'), 'r') as config_file:
    config_data = json.load(config_file)
    STORE_PATH = config_data.get('store_path')
    APPEND_DIMENSION = config_data.get('append_dim')
    DIMS = set(config_data.get('dims'))
    CHUNKS = config_data.get('chunks')
    DATA_TYPES = config_data.get('constants')['data_types']
    DATA_LEVELS = config_data.get('constants')['data_levels']


class Argo(collections.Dataset):
    def __init__(self, logger):
        self.chunks = CHUNKS
        self.store_path = STORE_PATH
        self.append_dim = APPEND_DIMENSION
        self.dims = DIMS
        self.logger = logger

    @staticmethod
    def _process_mf(dsinput, levels, data_types=DATA_TYPES, data_levels=DATA_LEVELS):
        """
        Argo Processor
        """
        ds = xr.Dataset()
        dims = ('N_PROF', 'N_LEVELS')
        """
        The number of profiles is indicated by the N_PROF dimension
        The number of pressure levels is indicated by the N_LEVELS dimension
        """
        pading = xr.DataArray(np.ones((len(dsinput.N_PROF), levels - len(dsinput.N_LEVELS))) * np.nan, dims=dims)
        pad_qc = xr.DataArray(np.chararray((len(dsinput.N_PROF), levels - len(dsinput.N_LEVELS))), dims=dims)
        pad_qc[:] = b' '
        for varname in data_types.keys():
            if varname in dsinput.data_vars:
                da = dsinput[varname]
                if 'N_LEVELS' in da.dims:
                    if varname in dsinput.data_vars:
                        if varname.endswith('QC'):
                            da = xr.concat([dsinput[varname], pad_qc], dim='N_LEVELS').astype(data_types[varname])
                        else:
                            da = xr.concat([dsinput[varname], pading], dim='N_LEVELS').astype(data_types[varname])
                else:
                    da = dsinput[varname].astype(data_types[varname])
            else:
                if varname in data_levels:
                    if data_types[varname] == 'float32':
                        da = xr.DataArray(np.ones((len(dsinput.N_PROF), levels), dtype='float32') * np.nan,
                                          name=varname,
                                          dims=['N_PROF', 'N_LEVELS'])
                    else:
                        p = np.chararray((len(dsinput.N_PROF), levels))
                        p[:] = b'0'
                        da = xr.DataArray(p.astype(data_types[varname]), name=varname, dims=['N_PROF', 'N_LEVELS'])
                else:
                    if data_types[varname] == 'float32':
                        da = xr.DataArray(np.ones(len(dsinput.N_PROF), dtype="float32") * np.nan, name=varname,
                                          dims=['N_PROF'])
                    else:
                        p = np.chararray((len(dsinput.N_PROF)))
                        p[:] = b'0'
                        da = xr.DataArray(p.astype(data_types[varname]), name=varname, dims=['N_PROF'])
            if not ('HISTORY' in varname) and ('N_CALIB' not in da.dims) and ('N_PARAM' not in da.dims) and (
                    'N_PROF' in da.dims):
                ds[varname] = da
        return ds.chunk({'N_LEVELS': levels})

    def processor(self, dataset: xr.Dataset) -> xr.Dataset:
        preproc = partial(self._process_mf, levels=3000)
        data = preproc(dataset)
        chunked_ds = data.chunk(chunks=self.chunks)
        return chunked_ds

    def _get_dim_value(self, ds: xr.Dataset) -> str:
        return ds.DC_REFERENCE.values[0]

    def get_region_index(self, zarr_store: fsspec.FSMap, file_ds: xr.Dataset) -> Union[dict[str, slice], None]:
        zarr_ds = xr.open_zarr(zarr_store)
        try:
            self.logger.info(f"identifying region index of the NetCDF if it's previously ingested to Zarr store")
            idx = np.argmax(zarr_ds.DC_REFERENCE.values == self._get_dim_value(file_ds))
            self.logger.info(f"region index: {idx}")
            dict_obj = {"N_PROF": slice(idx, idx + 1)}
            return dict_obj
        except:
            return None

    def generate_empty_ds(self, file_ds: xr.Dataset) -> xr.Dataset:
        ds = xr.Dataset({var: xr.DataArray(None,
                                           {'N_PROF': file_ds.N_PROF.values,
                                            'N_LEVELS': range(file_ds.dims['N_LEVELS'])},
                                           dims=self.dims)
                         for var in set(file_ds.data_vars) - self.dims})
        return ds

    def get_store_path(self):
        return self.store_path

    def get_append_dim(self):
        return self.append_dim
