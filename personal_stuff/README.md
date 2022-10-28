# DEV playground - Local Dask writing new Zarr store

For the ease of development purposes, files in this directory are not related to the `UpdateZarrRegion` pipeline and algorithms.

They are completely isolated and ignored in `.dockerignore`

There are some duplicated methods between files, but ignore them, refactoring is not really important at the moment **for files in this directory**!

### Use Cases:

Besides helping to pre-generate a Zarr store of a data collection, test attempts to write/update regions of a Zarr store can destroy/damage its data. 

Files in this directory will help to re-generate a new Zarr store from scratch to play around with again.

