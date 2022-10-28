FROM public.ecr.aws/lambda/python:3.9
RUN python -m pip install --upgrade pip
RUN pip install prefect pandas xarray[complete] numpy h5netcdf netCDF4 fsspec boto3 s3fs zarr pysqlite3
ADD . .
CMD ["main.lambda_handler"]
