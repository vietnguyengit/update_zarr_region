import fsspec
import xarray as xr
from typing import Union
from abc import ABC, abstractmethod


class Dataset(ABC):
    @abstractmethod
    def processor(self, dataset: xr.Dataset):
        pass

    @abstractmethod
    def _get_dim_value(self, ds: xr.Dataset):
        pass

    @abstractmethod
    def get_region_index(self, zarr_store: fsspec.FSMap, file_ds: xr.Dataset) -> Union[dict[str, slice], None]:
        pass

    @abstractmethod
    def get_store_path(self) -> str:
        pass
