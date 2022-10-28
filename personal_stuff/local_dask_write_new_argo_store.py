import io
import boto3
import s3fs
import time
import dask
import getpass
import numpy as np
import xarray as xr
from functools import partial
from tqdm import tqdm
import zarr
from dask.distributed import Client, LocalCluster
import asyncio


CHUNK_SHAPE = {
    "N_PROF": 500,
    "N_LEVELS": 800
}
SOURCE_PATH = 's3://imos-data/IMOS/Argo/dac/csiro/1901126/profiles/*.nc'
LOCAL_STORE_PATH = './local_store/argo_new'
S3_STORE_PATH = 's3://imos-data-lab-optimised/4428/argo'
FILES_PER_CHUNK = 500

data_types = {'CONFIG_MISSION_NUMBER': 'float32', 'CYCLE_NUMBER': 'float32', 'DATA_CENTRE': '|U2', 'DATA_MODE': '|U1',
              'DATA_STATE_INDICATOR': '|U4', 'DC_REFERENCE': '|U32', 'DIRECTION': '|U1', 'FIRMWARE_VERSION': '|U32',
              'FLOAT_SERIAL_NO': '|U32', 'JULD': 'float32', 'JULD_LOCATION': 'float32', 'JULD_QC': '|U1',
              'LATITUDE': 'float32',
              'LONGITUDE': 'float32', 'PI_NAME': '|U64', 'PLATFORM_NUMBER': '|U8', 'PLATFORM_TYPE': '|U32',
              'POSITIONING_SYSTEM': '|U8',
              'POSITION_QC': '|U1', 'PRES': 'float32', 'PRES_ADJUSTED': 'float32', 'PRES_ADJUSTED_ERROR': 'float32',
              'PRES_ADJUSTED_QC': '|U1', 'PRES_QC': '|U1', 'PROFILE_PRES_QC': '|U1', 'PROFILE_PSAL_QC': '|U1',
              'PROFILE_TEMP_QC': '|U1',
              'PROJECT_NAME': '|U64', 'PSAL': 'float32', 'PSAL_ADJUSTED': 'float32', 'PSAL_ADJUSTED_ERROR': 'float32',
              'PSAL_ADJUSTED_QC': '|U1', 'PSAL_QC': '|U1', 'TEMP': 'float32', 'TEMP_ADJUSTED': 'float32',
              'TEMP_ADJUSTED_ERROR': 'float32',
              'TEMP_ADJUSTED_QC': '|U1', 'TEMP_QC': '|U1', 'VERTICAL_SAMPLING_SCHEME': '|U256', 'WMO_INST_TYPE': '|U4'}

data_levels = ['PRES', 'PRES_ADJUSTED', 'PRES_ADJUSTED_ERROR', 'PRES_ADJUSTED_QC', 'PRES_QC', 'PSAL', 'PSAL_ADJUSTED',
               'PSAL_ADJUSTED_ERROR', 'PSAL_ADJUSTED_QC', 'PSAL_QC', 'TEMP', 'TEMP_ADJUSTED', 'TEMP_ADJUSTED_ERROR',
               'TEMP_ADJUSTED_QC', 'TEMP_QC']


def process_mf(dsinput, levels, data_types=data_types, data_levels=data_levels):
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
                    da = xr.DataArray(np.ones((len(dsinput.N_PROF), levels), dtype='float32') * np.nan, name=varname,
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


def process_float(s3_uri):
    preproc = partial(process_mf, levels=3000)
    file = read_dataset_inmemory(s3_uri)
    data = preproc(file)
    return data


def create_dask_cluster():
    dask_user = getpass.getuser()
    dask_address = 'localhost'
    dask_port = '0'
    with dask.config.set({'temporary_directory': f'/home/{dask_user}/dask/'}):
        # set up cluster and workers
        cluster = LocalCluster(n_workers=2, memory_limit='8GB', processes=True,
                               threads_per_worker=5, dashboard_address=f':{dask_port}', ip=dask_address)
        client = Client(address=cluster.scheduler_address)
        return cluster, client


def clean_up_cluster(client, cluster):
    client.close()
    cluster.close()


def read_dataset_inmemory(s3_path: str) -> xr.Dataset:
    """Read a NetCDF as an XArray using in-memory data"""
    inmemoryfile = io.BytesIO()
    session = boto3.Session(profile_name='nonproduction-admin')
    s3 = session.client('s3')
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    s3.download_fileobj(bucket, key, inmemoryfile)
    inmemoryfile.seek(0)
    return xr.open_dataset(inmemoryfile)


def chunks(lst, n):
    """
    Break the big list of NetCDF files to be ingested into smaller sub-list
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def get_essentials(source_path, store_type="s3", anon=False):
    s3 = s3fs.S3FileSystem(anon=anon)
    glob_result = s3.glob(source_path)
    if store_type == "s3":
        store_path = S3_STORE_PATH
        store = s3fs.S3Map(root=f'{store_path}', s3=s3, check=False)
    if store_type == "local":
        store_path = LOCAL_STORE_PATH
        store = zarr.DirectoryStore(f"{store_path}.zarr")
    input_paths = []
    input_paths.extend(['s3://' + path for path in glob_result])
    all_chunked_paths = list(
        chunks(input_paths, FILES_PER_CHUNK))  # indicates how many files should go into a single chunk
    # all_chunked_paths = all_chunked_paths[25:26]
    return store_path, store, all_chunked_paths


def collect():
    """
    Garbage collection
    """
    import gc
    gc.collect()


async def dask_argo_zarr(store_type="s3"):
    print("⚙️  Creating Dask Cluster")
    cluster, client = create_dask_cluster()
    print(f"🌐 Dask Dashboard: {client.dashboard_link}")
    store_path, store, all_chunked_paths = get_essentials(SOURCE_PATH)
    # print(all_chunked_paths[0][1], all_chunked_paths[0][60], all_chunked_paths[0][150],)
    print("🚀 Completed getting essentials variables")
    start_time = time.time()
    for i in tqdm(range(len(all_chunked_paths))):
        overwrite = True if i == 0 else False
        futures = []
        for path in all_chunked_paths[i]:
            futures.append(client.submit(process_float, path, retries=10))
        zarrs = client.gather(futures)  # result return to local from the cluster
        ds = xr.concat(zarrs, dim='N_PROF', coords='minimal', compat='override', combine_attrs='override',
                       fill_value='')
        chunked = ds.chunk(chunks=CHUNK_SHAPE)
        for var in chunked.data_vars:
            chunked[var].encoding = {}

        zarr.blosc.use_threads = False

        if store_type == 's3':
            sync_path = ".local_store/zarr.sync"
        else:
            sync_path = f"{store_path}.sync"
        synchronizer = zarr.ProcessSynchronizer(sync_path)
        if overwrite:
            z = chunked.to_zarr(store, mode='w', consolidated=True, compute=False, synchronizer=synchronizer)
        else:
            z = chunked.to_zarr(store, mode='a', append_dim='N_PROF', consolidated=True, compute=False,
                                synchronizer=synchronizer)
        z.compute()

    print("⏱  Total: %.2f seconds " % (time.time() - start_time))
    clean_up_cluster(client, cluster)


if __name__ == "__main__":
    asyncio.run(dask_argo_zarr())
