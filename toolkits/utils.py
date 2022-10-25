import io
import boto3
import fsspec
import xarray as xr
from typing import Callable
from prefect import task


@task
def is_first_write(store_path: str):
    try:
        xr.open_zarr(store_path, consolidated=True)
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
    file_ds.to_zarr(zarr_store, mode='r+', region=region, consolidated=True)


@task
def append_zarr(zarr_store: fsspec.FSMap, file_ds: xr.Dataset, append_dim: str):
    file_ds.to_zarr(zarr_store, mode='a', append_dim=append_dim, consolidated=True)


@task
def write_zarr(zarr_store: fsspec.FSMap, file_ds: xr.Dataset):
    file_ds.to_zarr(zarr_store, mode='w', consolidated=True)
