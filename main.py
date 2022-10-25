import json
import logging
from prefect import flow, get_run_logger
from toolkits.handlers.sst.handler import SST
from toolkits.handlers.argo.handler import Argo
from toolkits.utils import *


aws_logger = logging.getLogger()
aws_logger.setLevel(logging.INFO)


@flow
def update_zarr_store(bucket: str, object_key: str, event_name: str):
    logger = get_run_logger()

    if 'argo' in object_key.lower():
        dataset = Argo(logger)
    else:
        dataset = SST(logger)

    store_path = dataset.get_store_path()
    zarr_store = get_zarr_store(store_path)

    logger.info(f"reading new NetCDF file to xarray dataset")
    file_ds = read_netcdf(bucket, object_key, dataset.processor)

    if is_first_write(store_path):
        logger.info(f"writing the NetCDF file to the Zarr store")
        write_zarr(zarr_store, file_ds)
    else:
        region = dataset.get_zarr_region(zarr_store, file_ds)
        if region is not None:
            if event_name == 'ObjectCreated':
                logger.info(f"overwriting zarr store region by new NetCDF file")
            else:
                logger.info(f"removing NetCDF file contents from existing Zarr store")
                file_ds = dataset.generate_empty_ds(file_ds)
            overwrite_zarr_region(zarr_store, file_ds, region)
        else:
            logger.info(f"appending NetCDF file to current Zarr store")
            append_dim = dataset.get_append_dim()
            append_zarr(zarr_store, file_ds, append_dim)

    logger.info("completed!")


def handler(event, context):
    """
    Lambda's handler method that should keep method signature unchanged
    """
    aws_logger.info("Received event: " + json.dumps(event))
    bucket = event['Records'][0]['s3']['bucket']['name']
    tmp_event_name = event['Records'][0]['eventName']
    event_name = tmp_event_name.split(':')[0]
    object_key = event["Records"][0]["s3"]["object"]["key"]
    update_zarr_store(bucket, object_key, event_name)


# FOR LOCAL DEV
if __name__ == '__main__':
    bucket = 'imos-data'
    # object_key = 'IMOS/SRS/SST/ghrsst/L3S-1d/day/2022/20220317032000-ABOM-L3S_GHRSST-SSTskin-AVHRR_D-1d_day.nc'
    object_key = 'IMOS/SRS/SST/ghrsst/L3S-1d/day/2022/20220113032000-ABOM-L3S_GHRSST-SSTskin-AVHRR_D-1d_day.nc'
    # object_key = 'IMOS/SST/updated/out.nc'  # this file has doubled sea_surface_temperature values
    # bucket = 'vhnguyen'
    event_name = 'ObjectCreated'
    update_zarr_store(bucket, object_key, event_name)
