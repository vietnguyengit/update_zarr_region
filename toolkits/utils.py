import io
import os
import re
import zarr
import json
import boto3
import fsspec
import numcodecs
import xarray as xr
from typing import Callable, Union
from prefect import task, get_run_logger
from toolkits.handlers.argo.handler import Argo
from toolkits.handlers.sst.handler import SST


numcodecs.blosc.use_threads = False
SYNC_PATH = "/mnt/lambda-efs/update_zarr_region.sync"
PIPELINE_MASKS_JSON = 'handler_masks.json'
pipeline_handlers = {
    "process_argo": Argo,
    "process_sst": SST
}


@task
def find_handler(object_key: str) -> Union[Argo, SST]:
    logger = get_run_logger()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dir_path, PIPELINE_MASKS_JSON), 'r') as f:
        datasets = json.load(f).get('datasets')
    for d in datasets:
        for regex in d.get('regex'):
            pattern = re.compile(f"^({regex})$")
            if pattern.match(object_key):
                logger.info(f"handler_name: {d.get('name')}")
                dataset = pipeline_handlers.get(d.get('handler'))(logger)
                return dataset


@task
def is_first_write(zarr_store: fsspec.FSMap):
    try:
        xr.open_zarr(zarr_store)
        first_write = False
    except:
        first_write = True
    return first_write


@task
def get_zarr_store(store_path: str) -> fsspec.FSMap:
    """
    Input parameter is existing Zarr store's S3 path
    """
    mapper = fsspec.get_mapper(store_path, check=False)
    return mapper


@task
def read_netcdf(bucket: str, object_key: str, processor: Callable) -> xr.Dataset:
    """
    Create a dataframe and xarray data from NetCDF file. Loaded in memory
    :param processor:
    :param object_key:
    :return: df: dataframe
             ds: xarray
    """
    inmemoryfile = io.BytesIO()
    s3 = boto3.client("s3")
    s3.download_fileobj(bucket, object_key, inmemoryfile)
    inmemoryfile.seek(0)
    ds = xr.open_dataset(inmemoryfile)
    processed_ds = processor(ds)
    for var in processed_ds.data_vars:
        processed_ds[var].encoding = {}
    return processed_ds


@task
def overwrite_zarr_region(zarr_store: fsspec.FSMap, file_ds: xr.Dataset, region: dict):
    file_ds.to_zarr(zarr_store, mode='r+',
                    region=region,
                    consolidated=True,
                    synchronizer=zarr.ProcessSynchronizer(SYNC_PATH))


@task
def append_zarr(zarr_store: fsspec.FSMap, file_ds: xr.Dataset, append_dim: str):
    file_ds.to_zarr(zarr_store, mode='a',
                    append_dim=append_dim,
                    consolidated=True,
                    synchronizer=zarr.ProcessSynchronizer(SYNC_PATH))


@task
def write_zarr(zarr_store: fsspec.FSMap, file_ds: xr.Dataset):
    file_ds.to_zarr(zarr_store, mode='w',
                    consolidated=True,
                    synchronizer=zarr.ProcessSynchronizer(SYNC_PATH))
