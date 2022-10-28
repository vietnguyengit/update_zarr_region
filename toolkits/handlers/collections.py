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
    def get_zarr_region(self, zarr_store: fsspec.FSMap, file_ds: xr.Dataset) -> Union[dict[str, slice], None]:
        pass

    @abstractmethod
    def generate_empty_ds(self, file_ds: xr.Dataset) -> xr.Dataset:
        """
        Zarr has write_empty_chunks={True | False (default)} arguments when writing to the Zarr store
        but it is restricted to use with mode='w' (write from scratch) or mode='a' (append)
        which we can't if we want to update a region (mode='r+')

        This method is a workaround way to create empty dataset filled with `None` to replace an identified region

        Different data collections have different implementations.
        """
        pass

    @abstractmethod
    def get_store_path(self) -> str:
        pass

    @abstractmethod
    def get_append_dim(self) -> str:
        pass
