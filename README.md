[![Automate ECR image build steps](https://github.com/vietnguyengit/update_zarr_region/actions/workflows/ecr-build.yml/badge.svg)](https://github.com/vietnguyengit/update_zarr_region/actions/workflows/ecr-build.yml)

# Update Zarr store pipeline
POC pipeline to update a region of a zarr store, running from AWS Lambda and integrated with Prefect

#### Notes: the Zarr stores used are subsets of the actual big ones that we have for experimental purposes.

The subsets contain aggregated data of multiple NetCDF files (in smaller amounts) into Zarr stores, which is good enough to demonstrate how a Zarr store's regions can be updated.

---

#### :books: More details are commented inside the relevant code files.

#### :bar_chart: Pipeline run's effects (`appending new data`, `overwriting with new data`, `overwriting with empty data`) on the Zarr stores (`SST`, `Argo`) are demonstrated in a separated Jupyter notebook.

#### Concerned about writing/appending files in chronological order?

https://aws.amazon.com/premiumsupport/knowledge-center/lambda-sqs-scaling

> The maximum batch size for a standard Amazon SQS queue is 10,000 records. For FIFO queues, the maximum batch size is 10 records.

SQS FIFO was implemented but limited to 10 messages in a single batch, uploading lots of files will get the order we don't expect, see more: https://aws.amazon.com/blogs/compute/new-for-aws-lambda-sqs-fifo-as-an-event-source/

SQS standard queue doesn't guarantee order:

> Standard queues provide best-effort ordering

---

### How do serverless services connect to each other?

![update_zarr_Region drawio (2)](https://user-images.githubusercontent.com/26201635/198423280-f34d2838-f0db-4ab1-a43c-8565b6f2b96e.png)

- Raw bucket is `update-zarr-region-raw`, when a NetCDF file being removed or uploaded to this bucket, it'll invoke ~~the Lambda function `UpdateZarrRegionS3Watch` that sends event details as a message to a SQS FIFO Queue.~~ the pipeline, which is a Lambda function `UpdateZarrRegion` ~~subscribes to the SQS FIFO queue and invoked when receiving new messages.~~ to process the file.

- The pipeline powered by Prefect, is deployed by an image hosted on an AWS ECR private repository.

- New GitHub code commits will trigger Github Actions to automatically build and push the latest image to ECR repository.

- Untagged images (older images) will be cleaned up by an ECR Lifecycle policy settings.

- An AWS CodePipeline entity watches the ECR repository changes and invokes a generic Lambda function (`UpdateLambdaFunctionCode`) that refreshes the pipeline with latest ECR image to make sure upcoming invocations will run with latest code changes.

---

### What do the pipeline invocations look like?

Batch of multiple files uploaded/removed will trigger multiple Lambda processes running in parallel as Lambda is a distributed system.

#### At high-level:

![logics drawio](https://user-images.githubusercontent.com/26201635/198423340-31db0cbc-37ad-469e-9fb4-405563ddcf05.png)

#### Prefect UI:

![image](https://user-images.githubusercontent.com/26201635/198177692-c8daff39-94e3-4834-ab1f-d45f30449e19.png)

#### Why EFS here?

We need to use `zarr.sync.ProcessSynchronizer(path)` to HOPEFULLY ensure write consistency and avoid data corruptions.

* Zarr Docs:

  https://zarr.readthedocs.io/en/stable/api/sync.html

  > Path to a directory on a file system that is shared by all processes. N.B., this should be a different path to where you store the array.

  Mutilple Lambda invocations need to access a shared directory to update and look for Zarr sync files while writing actual data to **S3** Zarr store. `EFS` provides this feature out-of-the-box, **NO** "hackaround" here as EFS is specically designed for this purpose.

* AWS Docs:

  https://aws.amazon.com/blogs/compute/using-amazon-efs-for-aws-lambda-in-your-serverless-applications/

  > Amazon EFS is a fully managed, elastic, shared file system designed to be consumed by other AWS services, such as Lambda. With the release of Amazon EFS for Lambda, you can now easily share data across function invocations.

  > Multiple compute instances, including ... AWS Lambda, can access an Amazon EFS file system at the same time, providing a common data source for workloads

---

### Adding more data collections?

Go to `toolkits/handlers` and create a new package for new data collection. 3 files required:

- `__init__py` with empty content.
- `config.json` where you store information of the data collection essential information such as Zarr store path, constants, dimensions, chunks etc.
- `handler.py` takes abstract class `Dataset` as a parameter, you'll need to provide specific implementations depends on the data collections to the abstract methods.

Multi-dimensions data of different data collections may have different implementations to locate a Zarr store's region, generate empty dataset, and so on. For example, you really CANNOT make the implementations for ARGO to be generic and applicable for other collections such as SST, they have completely different structures.

Then update `handler_masks.json` with new data collection handler package information including the `Regex` pattern of the S3 `object_key`.

And finally, import new handler package to `utils.py`, methods in this file are wrapped with `Prefect` task decorator and are generic.

---

 ## Conclusion
 
- Update Zarr store regions is doable when ingested NetCDF files in `raw` bucket removed or updated, as long as there are no other processs writing data to the Zarr stores, even `ProcessSynchronizer()` applied on a distributed system like Lambda. 

- How to do it in large scale is a topic to discuss further, processing multiple files in order 1-by-1 in the same process is a safe approach.

- Updating a region requires to process one file only per transaction, it helps identify which regions to be updated correctly. E.g Zarr store with 5 regions: [1, 2, 3, 4, 5], imagine only [2,5] to be updated, concatinated datasets of [2,5] cannot help identifying appropriate region, only if we want to overwrite all regions sitting next to each other, start from [2] and stop at [5]: [2,3,4,5], but this is not ideal, what if the Zarr stores has 5000 regions and only [100] and [4001] need to be updated? - Stick with processing 1 file per transaction is a safe approach.

- Appending new files coming in a batch can be done with multiple files per transaction. Just because we can, we should?

- Event-driven with Lambda that triggering by every single file to start a process writing to Zarr will cause messy results even the processes have shared access to Synchronizer files on EFS. 

- Consider aggregating data pipelines to Zarr to be on scheduled flows OR some hackaround methods to let the pipelines wait until receiving enough files, eg. wait until receiving 100 files then firing the flows, while there's a processing writing to Zarr, block other processes. If do this way, investigate how to check if there is an existing process writing to Zarr store?
