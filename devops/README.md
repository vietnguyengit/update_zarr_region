# DevOps

Files in this directory are not for the pipeline algorithms but for how the pipeline is deployed and running on the AWS infrastructure.

The `devops` directory is ignored in `.dockerignore`.

### AWS CodePipeline

Deploy stage with AWS Lambda config:

| **Input artifact** | **FunctionName**                   | **UserParameters**                                                                                                                |
|--------------------|------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| SourceArtifact     | <update-lambda-code-function-name> | {"FunctionName": "<pipeline-function-name>", "ImageUri": "<account-id>.dkr.ecr.ap-southeast-2.amazonaws.com/<repository>:latest"} |
