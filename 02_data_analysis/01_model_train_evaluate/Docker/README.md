# SageMaker Dockerfile



[Sagemaker](https://sagemaker.readthedocs.io/en/stable/) module  is the way to go when we want to train a model on Sagemaker. The AWS team makes it straightforward to use the library and focus on the training rather than engineering. Although, one of the most important to ask is, **Do I need my own Docker image?** 

Sagemaker team maintains a vast amount of Docker images, which in most cases, it is enough for our need. However, if we need to use a Scikit learn version above 0.20 , then we might need to use our own image. 



There are two ways to store the Dockerfile:

- AWS ECR
  - Prerequisite: 
    - Docker
    - AWS CLI
- Docker Hub:
  - Docker

## Push Dockerfile

1. Create the Dockerfile
2. Build the Docker image from your Dockerfile.
   1. `docker build -t sagemaker-xgboost-container .`
3. Run **docker images** to verify that the image was created correctly.
   1. `docker images --filter reference=sagemaker-xgboost-container` 

### Host in AWS ECR


1. Authenticate to your default registry

After you have installed and configured the AWS CLI, authenticate the Docker CLI to your default registry. That way, the docker command can push and pull images with Amazon ECR. The AWS CLI provides a get-login-password command to simplify the authentication process.

To authenticate Docker to an Amazon ECR registry with get-login-password, run the aws ecr get-login-password command. When passing the authentication token to the docker login command, use the value AWS for the username and specify the Amazon ECR registry URI you want to authenticate to. If authenticating to multiple registries, you must repeat the command for each registry

   1. `aws ecr get-login-password --region eu-west-2 --profile optimum | docker login --username AWS --password-stdin 869881768412.dkr.ecr.eu-west-2.amazonaws.com`

2. Create a repositor

Now that you have an image to push to Amazon ECR, you must create a repository to hold it. In this example, you create a repository called `sagemaker-xgboost-container` to which you later push the `sagemaker-xgboost-container:latest_image`. To create a repository, run the following command:

```
aws ecr create-repository \
    --repository-name sagemaker-xgboost-container \
    --image-scanning-configuration scanOnPush=true \
    --region eu-west-2 \
    --profile optimum
```

The response is json file:

```
{
    "repository": {
        "repositoryArn": "arn:aws:ecr:eu-west-2:869881768412:repository/sagemaker-xgboost-container",
        "registryId": "869881768412",
        "repositoryName": "sagemaker-xgboost-container",
        "repositoryUri": "869881768412.dkr.ecr.eu-west-2.amazonaws.com/sagemaker-xgboost-container",
        "createdAt": 1599120257.0,
        "imageTagMutability": "MUTABLE",
        "imageScanningConfiguration": {
            "scanOnPush": true
        },
        "encryptionConfiguration": {
            "encryptionType": "AES256"
        }
    }
}
```

The repository is available at this URL: https://eu-west-2.console.aws.amazon.com/ecr/repositories?region=eu-west-2

3. Push an image to Amazon ECR

Now you can push your image to the Amazon ECR repository you created in the previous section. You use the docker CLI to push images, but there are a few prerequisites that must be satisfied for this to work properly:

The minimum version of docker is installed: 1.7

The Amazon ECR authorization token has been configured with docker login.

The Amazon ECR repository exists and the user has access to push to the repository.

After those prerequisites are met, you can push your image to your newly created repository in the default registry for your account.

To tag and push an image to Amazon ECR

- Tag the image to push to your repository
    - `docker tag sagemaker-xgboost-container:latest 869881768412.dkr.ecr.eu-west-2.amazonaws.com/sagemaker-xgboost-container:latest` 	
- Push the image
    - `docker push 869881768412.dkr.ecr.eu-west-2.amazonaws.com/sagemaker-xgboost-container:latest`
## Host in Docker hub

the Docker image is now ready to be built. Make sure Docker runs on your machine and run the following command:

```shell
docker build --tag dask-container:py-38 .
```

The image is named `dask-container` and tagged `py-38` . 

if you want [cloudprovider](https://github.com/dask/dask-cloudprovider) to build the image on Fargate, you need to push the image on [Docker Hub](https://docs.docker.com/docker-hub/). Once again, if you are not familiar with it, read the tutorial [here](https://docs.docker.com/get-started/part3/). 

Since the image is already build, we need to tag it and push it to our public repositories

``` shell
docker tag dask-container:py-38 thomaspernet/dask-container:py-38
docker push thomaspernet/dask-container:py-38
```

Repeat step 2 and 3 for each change in the DockerFile. To check your running images, run `docker image ls` . 



## Documentation

*Fargate and Dask*

- https://cloudprovider.dask.org/en/latest/
- https://github.com/rsignell-usgs/sagemaker-fargate-test
- https://medium.com/rapids-ai/getting-started-with-rapids-on-aws-ecs-using-dask-cloud-provider-b1adfdbc9c6e
- https://github.com/rsignell-usgs/dask-docker
- https://travis-ci.org/

*Docker*

- https://docs.docker.com/docker-hub/
- https://docs.docker.com/get-started/part2/
- https://docs.docker.com/get-started/part3/
- http://www.science.smith.edu/dftwiki/index.php/Tutorial:_Docker_Anaconda_Python_--_4
- https://docs.dask.org/en/latest/remote-data-services.html

**ECR**

- https://docs.aws.amazon.com/AmazonECR/latest/userguide/getting-started-cli.html

