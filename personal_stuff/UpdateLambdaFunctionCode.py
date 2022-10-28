import json
import boto3

aws_lambda = boto3.client('lambda')
code_pipeline = boto3.client('codepipeline')


def put_job_success(job, message):
    """Notify CodePipeline of a successful job

    Args:
        job: The CodePipeline job ID
        message: A message to be logged relating to the job status

    Raises:
        Exception: Any exception thrown by .put_job_success_result()

    """
    print('Putting job success')
    print(message)
    code_pipeline.put_job_success_result(jobId=job)


def put_job_failure(job, message):
    """Notify CodePipeline of a failed job

    Args:
        job: The CodePipeline job ID
        message: A message to be logged relating to the job status

    Raises:
        Exception: Any exception thrown by .put_job_failure_result()

    """
    print('Putting job failure')
    print(message)
    code_pipeline.put_job_failure_result(jobId=job, failureDetails={'message': message, 'type': 'JobFailed'})


def lambda_handler(event, context):
    print(event)
    job_id = event['CodePipeline.job']['id']

    user_parameters = json.loads(event['CodePipeline.job']['data']['actionConfiguration']['configuration']['UserParameters'])

    func_name = user_parameters['FunctionName']
    image_uri = user_parameters['ImageUri']

    print(func_name)
    print(image_uri)

    try:
        response = aws_lambda.update_function_code(FunctionName=func_name, ImageUri=image_uri)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            put_job_success(job_id, 'Succeeded')
        else:
            put_job_failure(job_id, 'Failed')
    except:
        put_job_failure(job_id, 'Failed')

    return 'Completed'
