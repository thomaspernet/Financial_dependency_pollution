# ETL, Training and Prediction

The workflow is broken into 5 parts triggered sequentially by a Lambda function.

![03_workflow.png](https://codahosted.io/docs/irgN8QLrCk/blobs/bl-tPXE2au6AN/331e787669b826d0d5f06d2606e2663c21c9215a72294023324f486ce73d652fddd9aacbf4ccb86f6170d3b0fc0e7b7a6d25d89f009ebe8cf63e933528d8d02beadf52bd86789e055b86596987a01fd43cd29adafc25f4a372544d1ee3b75fb195193638)

The workflow works as follow:

1. Download the data from the FTP using the python script → [creditsafe_ftp_extraction.py](https://github.com/Optimum-Finance/creditsafePrediction/blob/master/04_lambda_function/01_download_from_FTP/creditsafe_ftp_extraction.py)

2. 1. Trigger by lambda → [handler.py](https://github.com/Optimum-Finance/creditsafePrediction/blob/master/04_lambda_function/01_download_from_FTP/handler.py) 

   2. 1. Two layers 

      2. 1. Requirements → [00_download_from_FTP](https://github.com/Optimum-Finance/creditsafePrediction/tree/master/04_lambda_function/Layers/00_download_from_FTP)
         2. Binaries → [01_unzip_rar](https://github.com/Optimum-Finance/creditsafePrediction/tree/master/04_lambda_function/Layers/01_unzip_rar)

   3. A log file is saved in S3 → [creditsafedata/LOGS_FTP_EXECUTION](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/LOGS_FTP_EXECUTION/?region=eu-west-2)

3. Prepare the training/prediction tables using the notebook → [00_ETL_invoice_finance.md](https://github.com/Optimum-Finance/creditsafePrediction/blob/master/03_production/00_ETL_invoice_finance.md)

4. 1. Trigger by lambda using CloudTemplate → [cloudformation.yml](https://github.com/Optimum-Finance/creditsafePrediction/blob/master/03_production/cloudformation.yml)
   2. A log file is saved in S3 → [creditsafedata/LOGS_QUERY_EXECUTION/](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/LOGS_QUERY_EXECUTION/?region=eu-west-2&tab=overview)
   3. Notebook saved in S3 → [creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT/?region=eu-west-2&tab=overview)

5. Analyse the training table using the notebook → [01_insights_train_table.md](https://github.com/Optimum-Finance/creditsafePrediction/blob/master/03_production/01_insights_train_table.md)

6. 1. Trigger by lambda using CloudTemplate → [cloudformation.yml](https://github.com/Optimum-Finance/creditsafePrediction/blob/master/03_production/cloudformation.yml)
   2. Notebook saved in S3 → [creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT/?region=eu-west-2&tab=overview)

7. Train the mode using  the notebook → [02_train_XGBOOST_algo.md](https://github.com/Optimum-Finance/creditsafePrediction/blob/master/03_production/02_train_XGBOOST_algo.md)

8. 1. Trigger by lambda using CloudTemplate → [cloudformation.yml](https://github.com/Optimum-Finance/creditsafePrediction/blob/master/03_production/cloudformation.yml)

   2. Notebook saved in S3 → [creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT/?region=eu-west-2&tab=overview)

   3. 1. Model Precision saved in S3 → [creditsafedata/ALGORITHM/YYYYMMDD/XGBOOST/MODELS/model_precision_1](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/ALGORITHM/20200916/XGBOOST/MODELS/?region=eu-west-2&tab=overview)
      2. Model Recall saved in S3 → [creditsafedata/ALGORITHM/YYYYMMDD/XGBOOST/MODELS/model_recall_1](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/ALGORITHM/20200916/XGBOOST/MODELS/?region=eu-west-2&tab=overview)

   4. Model Evaluation logs saved in S3 → [creditsafedata/ALGORITHM/EVALUATION](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/ALGORITHM/EVALUATION/?region=eu-west-2&tab=overview)

9. Predict potential candidate using the notebook → [03_predict_potential_candidates.md](https://github.com/Optimum-Finance/creditsafePrediction/blob/master/03_production/03_predict_potential_candidates.md)

10. 1. Trigger by lambda using CloudTemplate → [cloudformation.yml](https://github.com/Optimum-Finance/creditsafePrediction/blob/master/03_production/cloudformation.yml)

    2. Notebook saved in S3 → [creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT/?region=eu-west-2&tab=overview)

    3. Three CSV files saved in S3:

    4. 1. Bucket creditsafedata 

       2. 1. [ALGORITHM/YYYYMMDD/DATA/PREDICT/LIST_POSITIVE_CLASS/full_prediction.csv](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/ALGORITHM/20200916/DATA/PREDICT/LIST_POSITIVE_CLASS/?region=eu-west-2&tab=overview)
          2. [ALGORITHM/YYYYMMDD/DATA/PREDICT/LIST_POSITIVE_CLASS/full_prediction_proba_75.csv](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/ALGORITHM/20200916/DATA/PREDICT/LIST_POSITIVE_CLASS/?region=eu-west-2&tab=overview)
          3. [ALGORITHM/YYYYMMDD/DATA/PREDICT/LIST_POSITIVE_CLASS/reg_prediction_proba_75.csv](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/ALGORITHM/20200916/DATA/PREDICT/LIST_POSITIVE_CLASS/?region=eu-west-2&tab=overview)

       3. Bucket creditsafeprediction 

       4. 1. [YYYYMMDD](https://s3.console.aws.amazon.com/s3/buckets/creditsafeprediction/20200916/?region=eu-west-2)

    5. List of potential users saved in S3 to create a table → [DATA/LIST_USERS_PREDICTED](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/DATA/LIST_USERS_PREDICTED/?region=eu-west-2&tab=overview)

Python scripts to train and predict are available in 

- Github:

- - [preprocessing_training.py](https://github.com/Optimum-Finance/creditsafePrediction/blob/master/02_Data_analysis/01_model_train_evaluate/preprocessing_training.py)
  - [evaluation.py](https://github.com/Optimum-Finance/creditsafePrediction/blob/master/02_Data_analysis/01_model_train_evaluate/evaluation.py)
  - [prediction.py](https://github.com/Optimum-Finance/creditsafePrediction/blob/master/02_Data_analysis/03_model_prediction/prediction.py)

- S3 → [ALGORITHM/PYTHON_SCRIPTS](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/ALGORITHM/PYTHON_SCRIPTS/?region=eu-west-2&tab=overview)

# Setup AWS services

To deploy a workflow in AWS, you need to setup and use different services.

First of all, you need to create a template that CloudFormation will use to model and provision, in an automated and secure manner, all the resources needed for your applications across all regions and accounts. This template includes the IAM policy and a lambda function to run a Sagemaker notebook on demand. The lambda function makes use of the way Sagemaker works to save the notebook in the container (local path) then uploads it to the S3 (S3URI). [Papermill](https://github.com/nteract/papermill) is used to pass arguments in the notebook.

Secondly, you will create a Docker image with the configuration needed to run a Sagemaker notebook on demand using a Lambda function.

Third, you create a trail in CloudTrail to log any event in the S3. We will use the event to trigger the Lambda function based on an S3 event. A given trail can have up to 250 events.

Then, you create an event rule in EventBridge that runs the lambda function. An event can be the upload of a file in a bucket.

Finally, create the target which is your Lambda function. Include the paramaters to pass in the payload of the lambda function.

The full configuration is available here:

- [CloudFormation](https://eu-west-2.console.aws.amazon.com/cloudformation/home?region=eu-west-2#/stacks?filteringText=&filteringStatus=active&viewNested=true&hideStacks=false)
- [Amazon ECS](https://eu-west-2.console.aws.amazon.com/ecr/repositories?region=eu-west-2)
- [Lambda](https://eu-west-2.console.aws.amazon.com/lambda/home?region=eu-west-2#/functions)
- [CloudTrail](https://eu-west-2.console.aws.amazon.com/cloudtrail/home?region=eu-west-2#/trails)
- [Eventbridge](https://eu-west-2.console.aws.amazon.com/events/home?region=eu-west-2#/rules)
- [SageMaker](https://eu-west-2.console.aws.amazon.com/sagemaker/home?region=eu-west-2#/processing-jobs)

###  IAM permission

- Cloudformation
- Lambda
- Cloudtrail
- EventBridge

Through the documentation, we are using optimum user. It’s a local config, prepared from the AWS CLI config file.

## Cloudformation 

Run CloudFormation template to set up roles, policies, and the Lambda function

You'll need to have AWS credentials set up that give you full permission on SageMaker, IAM, CloudFormation, Lambda, Cloudwatch Events, and ECR. You will also need to have Docker installed locally.

You'll need two files from the release on [GitHub](https://github.com/Optimum-Finance/creditsafePrediction/tree/master/03_production/container) : cloudformation.yml  and container.tar.gz .

```
aws cloudformation create-stack --stack-name sagemaker-run-notebook --template-body file://cloudformation.yml --capabilities CAPABILITY_NAMED_IAM --profile optimum
```

If you need to update the stack

```
aws cloudformation update-stack --stack-name sagemaker-run-notebook --template-body file://cloudformation.yml --capabilities CAPABILITY_NAMED_IAM --profile optimum
```

To see the status of the stack

```
aws cloudformation describe-stacks --stack-name sagemaker-run-notebook --profile optimum
```

You can go check  

- Cloudformation: [sagemaker-run-notebook](https://eu-west-2.console.aws.amazon.com/cloudformation/home?region=eu-west-2#/stacks/stackinfo?filteringText=&filteringStatus=active&viewNested=true&hideStacks=false&stackId=arn%3Aaws%3Acloudformation%3Aeu-west-2%3A869881768412%3Astack%2Fsagemaker-run-notebook%2F04267170-fa99-11ea-b9ee-0a6f7b6b7836)
-  Lambda function: [RunNotebook](https://eu-west-2.console.aws.amazon.com/lambda/home?region=eu-west-2#/functions/RunNotebook?tab=configuration) 

The CloudFormation template includes the Lambda function that provision a Sagemaker to run a notebook on demand. The lambda has the following arguments:

```
image,
input_path,
output_prefix,
notebook,
parameters,
role,
instance_type,
rule_name
```

We need to use input_path  and output_prefix  to define which notebook to trigger and where to save it. 

## Amazon ECS

Create a container image to run your notebook

Jobs run in SageMaker Processing Jobs run inside a Docker container. For this project, we have defined the container to include a script to set up the environment and run Papermill on the input notebook.

The container.tar.gz  file contains everything you need to build and customize the container. You can edit the requirements.txt file to specify Python libraries that your notebooks will need as described in the pip documentation.

```
cd container
bash ./build_and_push.sh notebook-runner optimum
```

## Copy  notebooks to S3

We upload the notebooks in S3, [creditsafedata/NOTEBOOKS_WORKFLOW](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/NOTEBOOKS_WORKFLOW/?region=eu-west-2&tab=overview) 

```
cd ../
aws s3 cp 00_ETL_invoice_finance.ipynb s3://creditsafedata/NOTEBOOKS_WORKFLOW/ --profile optimum
aws s3 cp 01_insights_train_table.ipynb s3://creditsafedata/NOTEBOOKS_WORKFLOW/ --profile optimum
aws s3 cp 02_train_XGBOOST_algo.ipynb s3://creditsafedata/NOTEBOOKS_WORKFLOW/ --profile optimum
aws s3 cp 03_predict_potential_candidates.ipynb s3://creditsafedata/NOTEBOOKS_WORKFLOW/ --profile optimum
```

Let's try to run the notebook named 01_insights_train_table.ipynb . We pass the credentials in the payload.

```
aws events put-targets --rule RunNotebook-insights \
--targets \
'[{"Id": "Default", "Arn": "arn:aws:lambda:eu-west-2:869881768412:function:RunNotebook", "Input": "{ \"input_path\": \"s3://creditsafedata/NOTEBOOKS_WORKFLOW/01_insights_train_table.ipynb\", \"output_prefix\": \"s3://creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT/\", \"parameters\": {\"key\": \"AKIA4VCH6NHOORJF4DX2\", \"secret\": \"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\"}}"}]' --profile optimum
```

see the job, open the result.json and copy the job name, or go https://eu-west-2.console.aws.amazon.com/sagemaker/home?region=eu-west-2#/processing-jobs 

## EventBridge

EventBridge uses a variety of AWS services as native event sources. For other AWS services, such as https://aws.amazon.com/s3/, it consumes events via https://aws.amazon.com/cloudtrail/ . You must first enable [CloudTrail logging](https://docs.aws.amazon.com/eventbridge/latest/userguide/log-s3-data-events.html) for the service you want to use with EventBridge. Once enabled, you can filter on any of the attributes available in an AWS event.

### Create trail

It is recommended to log the trails in a separate bucket. For each trail, you can add up to 250 Amazon S3 object

Trails are available here: https://eu-west-2.console.aws.amazon.com/cloudtrail/home?region=eu-west-2#/trails 

The trail will be used for our three event base:

- Analysis
- Training
- Prediction

```
aws cloudtrail create-trail --name logs-query-execution --s3-bucket-name aws-cloudtrail-logs-notebooks --no-is-multi-region-trail --profile optimum
```

We need to attach a data event. 

- To log data events for all objects in an S3 bucket, specify the bucket and an empty object prefix such as arn:aws:s3:::bucket-1 . The trail logs data events for all objects in this S3 bucket.
- To log data events for specific objects, specify the S3 bucket and object prefix such as arn:aws:s3:::bucket-1/example-image . The trail logs data events for objects in this S3 bucket that match the prefix

The event is attached to the bucket creditsafedata . We will specify the rule in EventBridge.

```
aws cloudtrail put-event-selectors --trail-name logs-query-execution --event-selectors '[{"ReadWriteType":"All","IncludeManagementEvents":true,"DataResources": [{"Type":"AWS::S3::Object", "Values": ["arn:aws:s3:::creditsafedata/"]}]}]' --profile optimum
```

Last, you need to activate the logging

```
aws cloudtrail start-logging --name logs-query-execution --profile optimum
```

## EventBridge

We use EventBridge Events to trigger the notebook executions. As soon as a file reaches the key   

creditsafedata/LOGS_FTP_EXECUTION/ ,creditsafedata/LOGS_QUERY_EXECUTION/  or creditsafedata/ALGORITHM/EVALUATION/ ,  lambda  RunNotebook  is triggered.

There are three steps:

1. Create the rules → the trigger
2. Add permission in Lambda
3. Create the target → Define what to do

CloudTrail available here: https://eu-west-2.console.aws.amazon.com/events/home?region=eu-west-2#/rules 

**Preparation Tables**

```
aws events put-rule --name "RunNotebook-preparation" --event-pattern "{\"source\": [\"aws.s3\"],\"detail-type\": [\"AWS API Call via CloudTrail\"],\"detail\":{\"eventSource\": [\"s3.amazonaws.com\"],\"eventName\": [\"PutObject\"],\"requestParameters\": {\"bucketName\": [ \"creditsafedata\"], \"key\": [{ \"prefix\": \"LOGS_FTP_EXECUTION/\" }]}}}"  --profile optimum
```

**Logs query execution** 

- Analysis

```
aws events put-rule --name "RunNotebook-insights" --event-pattern "{\"source\": [\"aws.s3\"],\"detail-type\": [\"AWS API Call via CloudTrail\"],\"detail\":{\"eventSource\": [\"s3.amazonaws.com\"],\"eventName\": [\"PutObject\"],\"requestParameters\": {\"bucketName\": [ \"creditsafedata\"], \"key\": [{ \"prefix\": \"LOGS_QUERY_EXECUTION/\" }]}}}"  --profile optimum
```

- Training

```
aws events put-rule --name "RunNotebook-training" --event-pattern "{\"source\": [\"aws.s3\"],\"detail-type\": [\"AWS API Call via CloudTrail\"],\"detail\":{\"eventSource\": [\"s3.amazonaws.com\"],\"eventName\": [\"PutObject\"],\"requestParameters\": {\"bucketName\": [ \"creditsafedata\"], \"key\": [{ \"prefix\": \"LOGS_QUERY_EXECUTION/\" }]}}}"  --profile optimum
```

**Logs evaluation**

```
 aws events put-rule --name "RunNotebook-predicion" --event-pattern "{\"source\": [\"aws.s3\"],\"detail-type\": [\"AWS API Call via CloudTrail\"],\"detail\":{\"eventSource\": [\"s3.amazonaws.com\"],\"eventName\": [\"PutObject\"],\"requestParameters\": {\"bucketName\": [ \"creditsafedata\"], \"key\": [{ \"prefix\": \"ALGORITHM/EVALUATION/\" }]}}}"  --profile optimum
```

Here is all the event rules:

-  [RunNotebook-insights](https://eu-west-2.console.aws.amazon.com/events/home?region=eu-west-2#/eventbus/default/rules/RunNotebook-insights)
-  [RunNotebook-predicion](https://eu-west-2.console.aws.amazon.com/events/home?region=eu-west-2#/eventbus/default/rules/RunNotebook-predicion)
-  [RunNotebook-preparation](https://eu-west-2.console.aws.amazon.com/events/home?region=eu-west-2#/eventbus/default/rules/RunNotebook-preparation)
- [RunNotebook-training](https://eu-west-2.console.aws.amazon.com/events/home?region=eu-west-2#/eventbus/default/rules/RunNotebook-training)

We need to add the permission

Lambda available here https://eu-west-2.console.aws.amazon.com/lambda/home?region=eu-west-2#/functions  

- Preparation

```
aws lambda add-permission --statement-id EB-RunNotebook-preparation \
              --action lambda:InvokeFunction \
              --function-name RunNotebook \
              --principal events.amazonaws.com \
              --source-arn arn:aws:events:eu-west-2:869881768412:rule/RunNotebook-preparation --profile optimum
```

- Analysis

```
aws lambda add-permission --statement-id EB-RunNotebook-insights \
              --action lambda:InvokeFunction \
              --function-name RunNotebook \
              --principal events.amazonaws.com \
              --source-arn arn:aws:events:eu-west-2:869881768412:rule/RunNotebook-insights --profile optimum
```

- Training

```
aws lambda add-permission --statement-id EB-RunNotebook-training \
              --action lambda:InvokeFunction \
              --function-name RunNotebook \
              --principal events.amazonaws.com \
              --source-arn arn:aws:events:eu-west-2:869881768412:rule/RunNotebook-training --profile optimum
```

- Prediction

```
aws lambda add-permission --statement-id RunNotebook-predicion \
              --action lambda:InvokeFunction \
              --function-name RunNotebook \
              --principal events.amazonaws.com \
              --source-arn arn:aws:events:eu-west-2:869881768412:rule/RunNotebook-predicion --profile optimum
```

The permission json is: https://eu-west-2.console.aws.amazon.com/lambda/home?region=eu-west-2#/functions/RunNotebook?tab=permissions

```
{
  "Version": "2012-10-17",
  "Id": "default",
  "Statement": [
    {
      "Sid": "869881768412_event_permissions_from_creditsafedata_for_RunNotebook",
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:eu-west-2:869881768412:function:RunNotebook",
      "Condition": {
        "StringEquals": {
          "AWS:SourceAccount": "869881768412"
        },
        "ArnLike": {
          "AWS:SourceArn": "arn:aws:s3:::creditsafedata"
        }
      }
    },
    {
      "Sid": "EB-RunNotebook-preparation",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:eu-west-2:869881768412:function:RunNotebook",
      "Condition": {
        "ArnLike": {
          "AWS:SourceArn": "arn:aws:events:eu-west-2:869881768412:rule/RunNotebook-preparation"
        }
      }
    },
    {
      "Sid": "EB-RunNotebook-insights",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:eu-west-2:869881768412:function:RunNotebook",
      "Condition": {
        "ArnLike": {
          "AWS:SourceArn": "arn:aws:events:eu-west-2:869881768412:rule/RunNotebook-insights"
        }
      }
    },
    {
      "Sid": "EB-RunNotebook-training",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:eu-west-2:869881768412:function:RunNotebook",
      "Condition": {
        "ArnLike": {
          "AWS:SourceArn": "arn:aws:events:eu-west-2:869881768412:rule/RunNotebook-training"
        }
      }
    },
    {
      "Sid": "RunNotebook-predicion",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:eu-west-2:869881768412:function:RunNotebook",
      "Condition": {
        "ArnLike": {
          "AWS:SourceArn": "arn:aws:events:eu-west-2:869881768412:rule/RunNotebook-predicion"
        }
      }
    }
  ]
}
```

to remove a permission:

```
aws lambda remove-permission  \
              --statement-id EB-RunNotebook-preparation \
              --function-name RunNotebook \
              --profile optimum
```

At last, we can create the event rule

- Preparation

```
aws events put-targets --rule RunNotebook-preparation \
--targets \
'[{"Id": "Default", "Arn": "arn:aws:lambda:eu-west-2:869881768412:function:RunNotebook", "Input": "{ \"input_path\": \"s3://creditsafedata/NOTEBOOKS_WORKFLOW/00_ETL_invoice_finance.ipynb\",\"output_prefix\": \"s3://creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT\",\"parameters\": {\"key\": \"AKIA4VCH6NHOORJF4DX2\",\"secret\": \"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\"}}"}]' --profile optimum
```

You can now see the full event for the notebook 00_ETL_invoice_finance → https://eu-west-2.console.aws.amazon.com/events/home?region=eu-west-2#/eventbus/default/rules/RunNotebook-preparation

- Event

![00_event.png](https://codahosted.io/docs/irgN8QLrCk/blobs/bl-p0Y8J9nF1E/0c237de2e1cc0646d657e6b8d287550d87ed9801cf453e8a6ff5d7dee15a91d09f736bc32251ee643541e80bb87336d94ce2b53e5670b9e08ebf99980ab4777f3faa25688382119dcdb29587eb71a247d29c6dc903a6f6654f81387cee99593d2f0c1486)

- Target

![01_target.png](https://codahosted.io/docs/irgN8QLrCk/blobs/bl-8fBc0qEiEo/221ebd0ffb697558c5839a0cbe85b21a521c20036c57bc076d6781eb2f8d212d66a8fd6c2c373dc12e8e074b0d27c9074a20f505835421223b6c1f1c8996eb351fdc2476b978ba461a6b64e7ae9eb1c96368bf8bbb198405f6707103f68263abfd5a070d)

- Analysis

```
aws events put-targets --rule RunNotebook-insights \
--targets \
'[{"Id": "Default", "Arn": "arn:aws:lambda:eu-west-2:869881768412:function:RunNotebook", "Input": "{ \"input_path\": \"s3://creditsafedata/NOTEBOOKS_WORKFLOW/01_insights_train_table.ipynb\",\"output_prefix\": \"s3://creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT\",\"parameters\": {\"key\": \"AKIA4VCH6NHOORJF4DX2\",\"secret\": \"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\"}}"}]' --profile optimum
```

- Training

```
aws events put-targets --rule RunNotebook-training \
--targets \
'[{"Id": "Default", "Arn": "arn:aws:lambda:eu-west-2:869881768412:function:RunNotebook", "Input": "{ \"input_path\": \"s3://creditsafedata/NOTEBOOKS_WORKFLOW/02_train_XGBOOST_algo.ipynb\",\"output_prefix\": \"s3://creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT\",\"parameters\": {\"key\": \"AKIA4VCH6NHOORJF4DX2\",\"secret\": \"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\"}}"}]' --profile optimum
```

- Prediction

```
aws events put-targets --rule RunNotebook-predicion \
--targets \
'[{"Id": "Default", "Arn": "arn:aws:lambda:eu-west-2:869881768412:function:RunNotebook", "Input": "{ \"input_path\": \"s3://creditsafedata/NOTEBOOKS_WORKFLOW/03_predict_potential_candidates.ipynb\",\"output_prefix\": \"s3://creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT\",\"parameters\": {\"key\": \"AKIA4VCH6NHOORJF4DX2\",\"secret\": \"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\"}}"}]' --profile optimum
```

To see if it works, add a file to the S3 bucket, in the folder [LOGS_QUERY_EXECUTION](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/LOGS_QUERY_EXECUTION/?region=eu-west-2&tab=overview)

And the job is available in Sagemaker https://eu-west-2.console.aws.amazon.com/sagemaker/home?region=eu-west-2#/processing-jobs . Wait a couple of second, and refresh the page if needed.

## Lambda

We use Serverless to deploy the Lambda function to download the data from the FTP because it’s easier to setup. Everything is in the serverless.yml file. The python script creditsafe_ftp_extraction.py is too large to be rendered in AWS lambda console so we moved the requirement in a Layer. Besides, the files in the FTP are compressed using software that requires a licence. Linux does not have Unrar pre-installed. Once again, we make use of Layer to install the dependencies we need

1. Deploy the function

The function uses the handler python script to download the data from the FTP to the S3. The argument of the function is the following:

```
{
  "host": "mft.creditsafe.com",
  "password": "trBjK8XZnA772c",
  "username": "S487438",
  "key": "AKIA4VCH6NHOORJF4DX2",
  "secret": "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",
  "stock": "FALSE"
}
```

where host, password and username are the login to connect to the FTP and the key and secret are the criticalfuture login password

 

```
serverless deploy --aws-profile optimum
```

the lambda function is available here: https://eu-west-2.console.aws.amazon.com/lambda/home?region=eu-west-2#/functions/optimum-dev-etl_data?tab=configuration

and in the S3 https://s3.console.aws.amazon.com/s3/buckets/lambda-ftp-s3/?region=eu-west-2

\2. Create the layer containing the requirements

 We need to create the first layer that contains the requirements. 

- Libray should be in a folder named python 

- Docker will install the dependencies you specify in requirements.txt. Docker has images to replicate the AWS Lambda environment. Using those images Docker will install the libraries you specified in requirements.txt that are compatible with the Lambda runtime. In the terminal enter the following code. Make sure you have Docker running before entering this command!

- After running the command you should see the site-packages directory you created in the previous step populated with all your dependencies.

- AWS requires all the layer code to be in a zip archive, so we need to zip everything in the python directory.

- - ![00_layer.png](https://codahosted.io/docs/irgN8QLrCk/blobs/bl-OEKGGYKVKB/9848e8f14aefec32946e303dc7ff773a22c9ec084a0d8f3802a74bdf0d9be8161e8de043fd3321ddb5739e4b90f06b7713d14644dcd5d39c823fbed10c7e8b83947f82b6ea93e0b8a98a5c3401eeefb0bd4076c100a494244f9bc5bebe0abde94c04ad2e)

```
# Create python env
mkdir -pv python/lib/python3.7/site-packages
# Create docker
docker run -v "$PWD":/var/task "lambci/lambda:build-python3.7" /bin/sh -c "pip install -r requirements.txt -t python/lib/python3.7/site-packages/; exit"
# zip 
zip -r 00_download_from_FTP.zip python
```

- All the contents of the python directory are included in the zip archive called 00_download_from_FTP.zip .

- Now the layer can be uploaded to AWS using the AWS CLI. You need to provide a few parameters in this step:

- - layer-name  is the name your want to give your layer
  - description  to briefly summarize the layer
  - zip-file  is the path to the zip archive you created in the previous step
  - compatible-runtimes  details the Python versions your layer is compatible with

```
aws lambda publish-layer-version \
    --layer-name "ftp-connection" \
    --description "Lambda Layer for to connect to sftp" \
    --zip-file "fileb://00_download_from_FTP.zip" \
    --compatible-runtimes "python3.7" \
    --profile optimum
```

- The layer is available here https://eu-west-2.console.aws.amazon.com/lambda/home?region=eu-west-2#/layers/ftp-connection/versions/1

\3. Create the layer containing the binaries

We need to zip the binaries in the paths:

```
/bin/unar
/usr/lib64/
```

It is mandatory to use the official AWS linux image to create the zip file. We created a DockerFile that install the binaries and move them to the right folder

```
FROM amazonlinux:latest

RUN amazon-linux-extras install epel

RUN yum install unar -y && \
    yum install p7zip -y && \
    yum install zip -y && \
    yum install unzip -y

RUN mkdir -p /root/unrar-lambda-layer

CMD cd /root/unrar-lambda-layer && \
    mkdir -p bin && \
    mkdir -p lib && \
    cp /bin/unar ./bin && \
    cp /bin/unzip ./bin && \
    cp /usr/lib64/libmenuw.so.6.0 ./lib && \
    cp /usr/lib64/libpcreposix.so.0 ./lib && \
    cp /usr/lib64/libpth.so.20 ./lib && \
    cp /usr/lib64/libgnustep-base.so.1.24 ./lib && \
    cp /usr/lib64/libustr-1.0.so.1 ./lib && \
    cp /usr/lib64/liblber-2.4.so.2 ./lib && \
    cp /usr/lib64/libblkid.so.1 ./lib && \
    cp /usr/lib64/libanl-2.26.so ./lib && \
    cp /usr/lib64/libpcre32.so.0 ./lib && \
    cp /usr/lib64/libcom_err.so.2 ./lib && \
    cp /usr/lib64/libudev.so.1 ./lib && \
    cp /usr/lib64/libgobject-2.0.so.0 ./lib && \
    cp /usr/lib64/libnss_compat-2.26.so ./lib && \
    cp /usr/lib64/libicutu.so.50.2 ./lib && \
    cp /usr/lib64/libkrad.so.0 ./lib && \
    cp /usr/lib64/libmenuw.so.6 ./lib && \
    cp /usr/lib64/libavahi-common.so.3.5.3 ./lib && \
    cp /usr/lib64/libpopt.so.0 ./lib && \
    cp /usr/lib64/libhogweed.so.2 ./lib && \
    cp /usr/lib64/libSegFault.so ./lib && \
    cp /usr/lib64/libpcrecpp.so.0.0.0 ./lib && \
    cp /usr/lib64/libstdc++.so.6 ./lib && \
    cp /usr/lib64/libmenu.so.6.0 ./lib && \
    cp /usr/lib64/libpanel.so.6 ./lib && \
    cp /usr/lib64/libsqlite3.so.0 ./lib && \
    cp /usr/lib64/libpam.so.0 ./lib && \
    cp /usr/lib64/libcom_err.so.2.1 ./lib && \
    cp /usr/lib64/libnsl-2.26.so ./lib && \
    cp /usr/lib64/libcrypt-2.26.so ./lib && \
    cp /usr/lib64/ld-linux-x86-64.so.2 ./lib && \
    cp /usr/lib64/libcap-ng.so.0 ./lib && \
    cp /usr/lib64/libavahi-client.so.3.2.9 ./lib && \
    cp /usr/lib64/libpwquality.so.1.0.2 ./lib && \
    cp /usr/lib64/libglib-2.0.so.0.5600.1 ./lib && \
    cp /usr/lib64/libunistring.so.0.1.2 ./lib && \
    cp /usr/lib64/libasm-0.176.so ./lib && \
    cp /usr/lib64/libattr.so.1 ./lib && \
    cp /usr/lib64/libicuuc.so.50 ./lib && \
    cp /usr/lib64/libslapi-2.4.so.2.10.7 ./lib && \
    cp /usr/lib64/libmount.so.1 ./lib && \
    cp /usr/lib64/libpanel.so.6.0 ./lib && \
    cp /usr/lib64/libattr.so.1.1.0 ./lib && \
    cp /usr/lib64/libm-2.26.so ./lib && \
    cp /usr/lib64/libgssapi_krb5.so.2 ./lib && \
    cp /usr/lib64/libnsspem.so ./lib && \
    cp /usr/lib64/libform.so.6.0 ./lib && \
    cp /usr/lib64/libsystemd-id128.so.0.0.28 ./lib && \
    cp /usr/lib64/libreadline.so.6 ./lib && \
    cp /usr/lib64/libmount.so.1.1.0 ./lib && \
    cp /usr/lib64/libcurl.so.4 ./lib && \
    cp /usr/lib64/libgssrpc.so.4.2 ./lib && \
    cp /usr/lib64/libkdb5.so.8 ./lib && \
    cp /usr/lib64/libpam_misc.so.0.82.0 ./lib && \
    cp /usr/lib64/libgssapi_krb5.so.2.2 ./lib && \
    cp /usr/lib64/libfdisk.so.1.1.0 ./lib && \
    cp /usr/lib64/libsmime3.so ./lib && \
    cp /usr/lib64/libicui18n.so.50 ./lib && \
    cp /usr/lib64/libxslt.so.1 ./lib && \
    cp /usr/lib64/libffi.so.6.0.1 ./lib && \
    cp /usr/lib64/libpython2.7.so.1.0 ./lib && \
    cp /usr/lib64/libicutu.so.50 ./lib && \
    cp /usr/lib64/libpcrecpp.so.0 ./lib && \
    cp /usr/lib64/libcryptsetup.so.4 ./lib && \
    cp /usr/lib64/libcidn-2.26.so ./lib && \
    cp /usr/lib64/libtspi.so.1 ./lib && \
    cp /usr/lib64/libmenu.so.6 ./lib && \
    cp /usr/lib64/libpcre16.so.0.2.0 ./lib && \
    cp /usr/lib64/liblzma.so.5 ./lib && \
    cp /usr/lib64/libgcrypt.so.11 ./lib && \
    cp /usr/lib64/libtspi.so.1.2.0 ./lib && \
    cp /usr/lib64/libncurses.so.6.0 ./lib && \
    cp /usr/lib64/libicuio.so.50.2 ./lib && \
    cp /usr/lib64/libsystemd-login.so.0.9.3 ./lib && \
    cp /usr/lib64/libsasl2.so.3.0.0 ./lib && \
    cp /usr/lib64/libutempter.so.0 ./lib && \
    cp /usr/lib64/libssl.so.1.0.2k ./lib && \
    cp /usr/lib64/libpwquality.so.1 ./lib && \
    cp /usr/lib64/libkrb5support.so.0 ./lib && \
    cp /usr/lib64/libncurses.so.6 ./lib && \
    cp /usr/lib64/libmagic.so.1.0.0 ./lib && \
    cp /usr/lib64/libgpgme-pthread.so.11.8.1 ./lib && \
    cp /usr/lib64/libicudata.so.50.2 ./lib && \
    cp /usr/lib64/libgobject-2.0.so.0.5600.1 ./lib && \
    cp /usr/lib64/libelf.so.1 ./lib && \
    cp /usr/lib64/libform.so.6 ./lib && \
    cp /usr/lib64/libpth.so.20.0.27 ./lib && \
    cp /usr/lib64/libidn2.so.0.3.7 ./lib && \
    cp /usr/lib64/libnss_dns.so.2 ./lib && \
    cp /usr/lib64/libnss_compat.so.2 ./lib && \
    cp /usr/lib64/libc.so.6 ./lib && \
    cp /usr/lib64/liblber-2.4.so.2.10.7 ./lib && \
    cp /usr/lib64/libsystemd-login.so.0 ./lib && \
    cp /usr/lib64/libkrb5.so.3.3 ./lib && \
    cp /usr/lib64/libz.so.1 ./lib && \
    cp /usr/lib64/libncursesw.so.6 ./lib && \
    cp /usr/lib64/libpamc.so.0.82.1 ./lib && \
    cp /usr/lib64/librpmio.so.3 ./lib && \
    cp /usr/lib64/libnghttp2.so.14.20.0 ./lib && \
    cp /usr/lib64/libassuan.so.0.4.0 ./lib && \
    cp /usr/lib64/libblkid.so.1.1.0 ./lib && \
    cp /usr/lib64/libp11-kit.so.0 ./lib && \
    cp /usr/lib64/libselinux.so.1 ./lib && \
    cp /usr/lib64/libaudit.so.1 ./lib && \
    cp /usr/lib64/libresolv.so.2 ./lib && \
    cp /usr/lib64/libnssdbm3.so ./lib && \
    cp /usr/lib64/libkrb5support.so.0.1 ./lib && \
    cp /usr/lib64/p11-kit-trust.so ./lib && \
    cp /usr/lib64/libverto.so.1.0.0 ./lib && \
    cp /usr/lib64/libpanelw.so.6.0 ./lib && \
    cp /usr/lib64/libtinfo.so.6 ./lib && \
    cp /usr/lib64/libcryptsetup.so.4.7.0 ./lib && \
    cp /usr/lib64/libicuuc.so.50.2 ./lib && \
    cp /usr/lib64/libpam.so.0.83.1 ./lib && \
    cp /usr/lib64/libnssutil3.so ./lib && \
    cp /usr/lib64/libgssrpc.so.4 ./lib && \
    cp /usr/lib64/libgpgme.so.11 ./lib && \
    cp /usr/lib64/libmemusage.so ./lib && \
    cp /usr/lib64/libmvec-2.26.so ./lib && \
    cp /usr/lib64/libtasn1.so.6.5.3 ./lib && \
    cp /usr/lib64/libkmod.so.2.3.3 ./lib && \
    cp /usr/lib64/libnss_files.so.2 ./lib && \
    cp /usr/lib64/libgio-2.0.so.0.5600.1 ./lib && \
    cp /usr/lib64/libnss_myhostname.so.2 ./lib && \
    cp /usr/lib64/liblua-5.1.so ./lib && \
    cp /usr/lib64/libcap-ng.so.0.0.0 ./lib && \
    cp /usr/lib64/libsoftokn3.so ./lib && \
    cp /usr/lib64/libuuid.so.1 ./lib && \
    cp /usr/lib64/librpm.so.3.2.2 ./lib && \
    cp /usr/lib64/libthread_db-1.0.so ./lib && \
    cp /usr/lib64/librpmsign.so.1.2.2 ./lib && \
    cp /usr/lib64/libk5crypto.so.3.1 ./lib && \
    cp /usr/lib64/libasm.so.1 ./lib && \
    cp /usr/lib64/libssl3.so ./lib && \
    cp /usr/lib64/libgcc_s.so.1 ./lib && \
    cp /usr/lib64/libformw.so.6 ./lib && \
    cp /usr/lib64/libicutest.so.50.2 ./lib && \
    cp /usr/lib64/libfdisk.so.1 ./lib && \
    cp /usr/lib64/librpm.so.3 ./lib && \
    cp /usr/lib64/libxml2.so.2 ./lib && \
    cp /usr/lib64/libunistring.so.0 ./lib && \
    cp /usr/lib64/libkrad.so.0.0 ./lib && \
    cp /usr/lib64/libmagic.so.1 ./lib && \
    cp /usr/lib64/libresolv-2.26.so ./lib && \
    cp /usr/lib64/libtic.so.6 ./lib && \
    cp /usr/lib64/libavahi-client.so.3 ./lib && \
    cp /usr/lib64/libpthread-2.26.so ./lib && \
    cp /usr/lib64/libgnustep-base.so.1.24.9 ./lib && \
    cp /usr/lib64/libauparse.so.0.0.0 ./lib && \
    cp /usr/lib64/libanl.so.1 ./lib && \
    cp /usr/lib64/libplc4.so ./lib && \
    cp /usr/lib64/libcrypto.so.10 ./lib && \
    cp /usr/lib64/libauparse.so.0 ./lib && \
    cp /usr/lib64/libcurl.so.4.5.0 ./lib && \
    cp /usr/lib64/libutil.so.1 ./lib && \
    cp /usr/lib64/libsystemd.so.0.6.0 ./lib && \
    cp /usr/lib64/libgdbm_compat.so.4 ./lib && \
    cp /usr/lib64/libicuio.so.50 ./lib && \
    cp /usr/lib64/libtinfo.so.6.0 ./lib && \
    cp /usr/lib64/libglib-2.0.so.0 ./lib && \
    cp /usr/lib64/libssh2.so.1.0.1 ./lib && \
    cp /usr/lib64/ld-2.26.so ./lib && \
    cp /usr/lib64/libcrack.so.2.9.0 ./lib && \
    cp /usr/lib64/libnss3.so ./lib && \
    cp /usr/lib64/libkdb5.so.8.0 ./lib && \
    cp /usr/lib64/libiculx.so.50.2 ./lib && \
    cp /usr/lib64/librpmbuild.so.3 ./lib && \
    cp /usr/lib64/libldap_r-2.4.so.2.10.7 ./lib && \
    cp /usr/lib64/libsemanage.so.1 ./lib && \
    cp /usr/lib64/libiculx.so.50 ./lib && \
    cp /usr/lib64/libsystemd-id128.so.0 ./lib && \
    cp /usr/lib64/libgthread-2.0.so.0.5600.1 ./lib && \
    cp /usr/lib64/libslapi-2.4.so.2 ./lib && \
    cp /usr/lib64/libexslt.so.0 ./lib && \
    cp /usr/lib64/libpcre16.so.0 ./lib && \
    cp /usr/lib64/libkrb5.so.3 ./lib && \
    cp /usr/lib64/libmvec.so.1 ./lib && \
    cp /usr/lib64/libnsssysinit.so ./lib && \
    cp /usr/lib64/libdw-0.176.so ./lib && \
    cp /usr/lib64/libz.so.1.2.7 ./lib && \
    cp /usr/lib64/libcrypt.so.1 ./lib && \
    cp /usr/lib64/libgnutls.so.28 ./lib && \
    cp /usr/lib64/libpcre.so.1 ./lib && \
    cp /usr/lib64/libxslt.so.1.1.28 ./lib && \
    cp /usr/lib64/libgthread-2.0.so.0 ./lib && \
    cp /usr/lib64/libexslt.so.0.8.17 ./lib && \
    cp /usr/lib64/libm.so.6 ./lib && \
    cp /usr/lib64/libldap-2.4.so.2.10.7 ./lib && \
    cp /usr/lib64/libgpg-error.so.0.10.0 ./lib && \
    cp /usr/lib64/libgdbm.so.4.0.0 ./lib && \
    cp /usr/lib64/libpthread.so.0 ./lib && \
    cp /usr/lib64/libsystemd-journal.so.0.11.5 ./lib && \
    cp /usr/lib64/libnettle.so.4 ./lib && \
    cp /usr/lib64/libexpat.so.1 ./lib && \
    cp /usr/lib64/libfreeblpriv3.so ./lib && \
    cp /usr/lib64/libacl.so.1.1.0 ./lib && \
    cp /usr/lib64/libmetalink.so.3.0.0 ./lib && \
    cp /usr/lib64/libk5crypto.so.3 ./lib && \
    cp /usr/lib64/libdw.so.1 ./lib && \
    cp /usr/lib64/libcrack.so.2 ./lib && \
    cp /usr/lib64/libgnutls.so.28.43.3 ./lib && \
    cp /usr/lib64/liblz4.so.1.7.5 ./lib && \
    cp /usr/lib64/libdl-2.26.so ./lib && \
    cp /usr/lib64/libnghttp2.so.14 ./lib && \
    cp /usr/lib64/libkeyutils.so.1.5 ./lib && \
    cp /usr/lib64/libxml2.so.2.9.1 ./lib && \
    cp /usr/lib64/libgcc_s-7-20180712.so.1 ./lib && \
    cp /usr/lib64/libstdc++.so.6.0.24 ./lib && \
    cp /usr/lib64/libhistory.so.6.2 ./lib && \
    cp /usr/lib64/libc-2.26.so ./lib && \
    cp /usr/lib64/libBrokenLocale-2.26.so ./lib && \
    cp /usr/lib64/libreadline.so.6.2 ./lib && \
    cp /usr/lib64/libtic.so.6.0 ./lib && \
    cp /usr/lib64/libdbus-1.so.3.14.14 ./lib && \
    cp /usr/lib64/libicule.so.50.2 ./lib && \
    cp /usr/lib64/libfreebl3.so ./lib && \
    cp /usr/lib64/libutempter.so.1.1.6 ./lib && \
    cp /usr/lib64/librpmio.so.3.2.2 ./lib && \
    cp /usr/lib64/libgmp.so.10 ./lib && \
    cp /usr/lib64/libicule.so.50 ./lib && \
    cp /usr/lib64/libicudata.so.50 ./lib && \
    cp /usr/lib64/liblz4.so.1 ./lib && \
    cp /usr/lib64/libgdbm.so.4 ./lib && \
    cp /usr/lib64/libcidn.so.1 ./lib && \
    cp /usr/lib64/libgio-2.0.so.0 ./lib && \
    cp /usr/lib64/libplds4.so ./lib && \
    cp /usr/lib64/libsystemd-daemon.so.0.0.12 ./lib && \
    cp /usr/lib64/libkeyutils.so.1 ./lib && \
    cp /usr/lib64/libuuid.so.1.3.0 ./lib && \
    cp /usr/lib64/p11-kit-proxy.so ./lib && \
    cp /usr/lib64/libldap_r-2.4.so.2 ./lib && \
    cp /usr/lib64/libgmodule-2.0.so.0.5600.1 ./lib && \
    cp /usr/lib64/libmetalink.so.3 ./lib && \
    cp /usr/lib64/libudev.so.1.6.2 ./lib && \
    cp /usr/lib64/libicutest.so.50 ./lib && \
    cp /usr/lib64/liblzma.so.5.2.2 ./lib && \
    cp /usr/lib64/libpcre.so.1.2.0 ./lib && \
    cp /usr/lib64/libthread_db.so.1 ./lib && \
    cp /usr/lib64/libkmod.so.2 ./lib && \
    cp /usr/lib64/libgmpxx.so.4 ./lib && \
    cp /usr/lib64/libaudit.so.1.0.0 ./lib && \
    cp /usr/lib64/libssl.so.10 ./lib && \
    cp /usr/lib64/libnss_mymachines.so.2 ./lib && \
    cp /usr/lib64/libcap.so.2 ./lib && \
    cp /usr/lib64/librt.so.1 ./lib && \
    cp /usr/lib64/libdb-5.so ./lib && \
    cp /usr/lib64/libcrypto.so.1.0.2k ./lib && \
    cp /usr/lib64/libdb-5.3.so ./lib && \
    cp /usr/lib64/libdl.so.2 ./lib && \
    cp /usr/lib64/libgmp.so.10.2.0 ./lib && \
    cp /usr/lib64/libnssckbi.so ./lib && \
    cp /usr/lib64/libgpgme-pthread.so.11 ./lib && \
    cp /usr/lib64/libpanelw.so.6 ./lib && \
    cp /usr/lib64/libcap.so.2.22 ./lib && \
    cp /usr/lib64/libobjc.so.4 ./lib && \
    cp /usr/lib64/libassuan.so.0 ./lib && \
    cp /usr/lib64/cracklib_dict.pwd ./lib && \
    cp /usr/lib64/libncursesw.so.6.0 ./lib && \
    cp /usr/lib64/libnettle.so.4.7 ./lib && \
    cp /usr/lib64/libldap-2.4.so.2 ./lib && \
    cp /usr/lib64/libidn2.so.0 ./lib && \
    cp /usr/lib64/libbz2.so.1 ./lib && \
    cp /usr/lib64/libqrencode.so.3 ./lib && \
    cp /usr/lib64/libsystemd.so.0 ./lib && \
    cp /usr/lib64/libffi.so.6 ./lib && \
    cp /usr/lib64/libnss_dns-2.26.so ./lib && \
    cp /usr/lib64/libgpg-error.so.0 ./lib && \
    cp /usr/lib64/libhogweed.so.2.5 ./lib && \
    cp /usr/lib64/libqrencode.so.3.4.1 ./lib && \
    cp /usr/lib64/libobjc.so.4.0.0 ./lib && \
    cp /usr/lib64/librpmbuild.so.3.2.2 ./lib && \
    cp /usr/lib64/libpamc.so.0 ./lib && \
    cp /usr/lib64/libpam_misc.so.0 ./lib && \
    cp /usr/lib64/libgpgme.so.11.8.1 ./lib && \
    cp /usr/lib64/libgmodule-2.0.so.0 ./lib && \
    cp /usr/lib64/libsystemd-journal.so.0 ./lib && \
    cp /usr/lib64/libacl.so.1 ./lib && \
    cp /usr/lib64/libavahi-common.so.3 ./lib && \
    cp /usr/lib64/libnspr4.so ./lib && \
    cp /usr/lib64/libformw.so.6.0 ./lib && \
    cp /usr/lib64/libpcre32.so.0.0.0 ./lib && \
    cp /usr/lib64/libssh2.so.1 ./lib && \
    cp /usr/lib64/librt-2.26.so ./lib && \
    cp /usr/lib64/libbz2.so.1.0.6 ./lib && \
    cp /usr/lib64/libicui18n.so.50.2 ./lib && \
    cp /usr/lib64/libtasn1.so.6 ./lib && \
    cp /usr/lib64/libsepol.so.1 ./lib && \
    cp /usr/lib64/libgdbm_compat.so.4.0.0 ./lib && \
    cp /usr/lib64/librpmsign.so.1 ./lib && \
    cp /usr/lib64/libexpat.so.1.6.0 ./lib && \
    cp /usr/lib64/libsmartcols.so.1 ./lib && \
    cp /usr/lib64/libsystemd-daemon.so.0 ./lib && \
    cp /usr/lib64/libsqlite3.so.0.8.6 ./lib && \
    cp /usr/lib64/libBrokenLocale.so.1 ./lib && \
    cp /usr/lib64/libelf-0.176.so ./lib && \
    cp /usr/lib64/libgcrypt.so.11.8.2 ./lib && \
    cp /usr/lib64/libsmartcols.so.1.1.0 ./lib && \
    cp /usr/lib64/libverto.so.1 ./lib && \
    cp /usr/lib64/libdbus-1.so.3 ./lib && \
    cp /usr/lib64/libp11-kit.so.0.3.0 ./lib && \
    cp /usr/lib64/libutil-2.26.so ./lib && \
    cp /usr/lib64/libnss_files-2.26.so ./lib && \
    cp /usr/lib64/libpcprofile.so ./lib && \
    cp /usr/lib64/libhistory.so.6 ./lib && \
    cp /usr/lib64/libsasl2.so.3 ./lib && \
    cp /usr/lib64/libgmpxx.so.4.4.0 ./lib && \
    cp /usr/lib64/libnsl.so.1 ./lib && \
    cp /usr/lib64/libpcreposix.so.0.0.1 ./lib && \
    cp /usr/lib64/libpopt.so.0.0.0 ./lib && \
    cp /usr/lib64/libustr-1.0.so.1.0.4 ./lib && \
    zip -r unrar-lambda-layer.zip ./* && \
    rm -rf lib bin
```

- With unrar-lambda-layer, the binaries should be in ./bin and the libary in ./lib
- The library for unar is libgnustep-base.so.1.24 and libgnustep-base.so.1.24.9
- Now that the Dockerfile is ready, we can build the binaries in the AWS official linux image and zip it locally

```
docker build -t unrar-lambda-layer .
docker run -v "${PWD}/dist":/root/unrar-lambda-layer unrar-lambda-layer:latest
```

The zip file is larger than 50mo so we need to move it to S3

```
aws s3 cp dist/unrar-lambda-layer.zip s3://creditsafedata/LAMBDA_LAYERS/ --profile optimum
```

- Push the layer

```
## if local  #--zip-file "fileb://dist/unrar-lambda-layer.zip" \
aws lambda publish-layer-version \
    --layer-name "unrar-lambda-layer" \
    --description "Lambda Layer to extract unrar" \
    --content S3Bucket=creditsafedata,S3Key=LAMBDA_LAYERS/unrar-lambda-layer.zip \
    --profile optimum
```

At last, we need to add the two layers in the Serverless config file

```
functions:
  etl_data:
    handler: handler.etl_data
    layers:
      - arn:aws:lambda:eu-west-2:869881768412:layer:ftp-connection:1
      - arn:aws:lambda:eu-west-2:869881768412:layer:unrar-lambda-layer:3
```

try it out 

- In lambda:

```
{
  "host": "mft.creditsafe.com",
  "password": "trBjK8XZnA772c",
  "username": "S487438",
  "key": "AKIA4VCH6NHOORJF4DX2",
  "secret": "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",
  "stock": "FALSE"
}
```

- in local

```
serverless invoke local --function etl_data --data '{"queryStringParameters": {"host":"mft.creditsafe.com","password":"trBjK8XZnA772c","username":"S487438","key":"AKIA4VCH6NHOORJF4DX2","secret":"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","stock":"FALSE"}}'


```

- Create a dedicated role https://console.aws.amazon.com/iam/home?region=eu-west-2#/roles/full-access-sagemaker-critical

- - Sagemaker full access
  - S3 bucket access

- Add full access to assumed role https://console.aws.amazon.com/iam/home?region=eu-west-2#/roles/BasicExecuteNotebookRole-eu-west-2

- - Not sure... need to double check why the assumed role work

# 1. Lambda 1 

- How: Time trigger

- Objective: Extract data from FTP

- type: Python script

- US: 

- - [US 1 FTP](https://coda.io/d/OptimumFinance_dirgN8QLrCk/US-1-FTP_suu19)

The lambda function uses the following script [creditsafe_ftp_extraction.py](https://github.com/Optimum-Finance/creditsafePrediction/blob/master/04_lambda_function/01_download_from_FTP/creditsafe_ftp_extraction.py) and is triggered on a time event.

- CreditSafe supplies the data through a FTP. 

- From the current FTP information of Optimum, the data are available every second day of each month.

- The data is be saved in Optimum AWS account, in the following folder:

- - RAW_DATA: [creditsafedata/DATA/RAW_DATA/](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/DATA/RAW_DATA): raw data

  - - Tree:

    - DATA/RAW_DATA

    - - UKLTD_M_YYYYMM01

      - - FILENAME_M_YYYYMM01.rar 

  - UNZIP_DATA: [creditsafedata/DATA/UNZIP_DATA/](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/DATA/UNZIP_DATA): Monthly unzip data

  - - Tree:

    - DATA/UNZIP_DATA

    - - UKLTD_M_YYYYMM01

      - - FILENAME

        - - FILENAME_M_YYYYMM01.txt 

  - UNZIP_DATA_APPEND_ALL: [creditsafedata/DATA/UNZIP_DATA_APPEND_ALL/](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/DATA/UNZIP_DATA_APPEND_ALL): append monthly unzip data to previously loaded data

  - - Tree:

    - DATA/UNZIP_DATA_APPEND_ALL

    - - FILENAME

      - - FILENAME_M_YYYYMM01.txt 

The notebook script [creditsafe_ftp_extraction.py](https://github.com/Optimum-Finance/creditsafePrediction/blob/master/04_lambda_function/01_download_from_FTP/creditsafe_ftp_extraction.py) will save the json parameters file in the folder [creditsafedata/LOGS_FTP_EXECUTION/](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/LOGS_FTP_EXECUTION/?region=eu-west-2&tab=overview)

# 2. Lambda 2 

- How: Event trigger

- Objective: Notebook load and transform tables

- type: Jupyter Notebook

- US: 

- - [US 08 Athena tables Model](https://coda.io/d/OptimumFinance_dirgN8QLrCk/US-08-Athena-tables-Model_suUDh)
  - [US 08 Athena tables Model](https://coda.io/d/OptimumFinance_dirgN8QLrCk/US-08-Athena-tables-Model_suUDh)
  - [US 08 Athena tables Model](https://coda.io/d/OptimumFinance_dirgN8QLrCk/US-08-Athena-tables-Model_suUDh)
  - [US 08 Athena tables Model](https://coda.io/d/OptimumFinance_dirgN8QLrCk/US-08-Athena-tables-Model_suUDh)

The notebook [creditsafedata/NOTEBOOKS_WORKFLOW/00_ETL_invoice_finance.ipynb](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/NOTEBOOKS_WORKFLOW/?region=eu-west-2&tab=overview) is triggered when the file LOG_UKLTD_M_YYYYMM01.json  reaches the the folder [creditsafedata/LOGS_FTP_EXECUTION/](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/LOGS_FTP_EXECUTION/?region=eu-west-2&tab=overview)

During the previous US, we used 7 notebooks to create the final table and the table to return the prediction:

1.  00_lookup_tables  → Import 10 lookup tables
2.  01_main_tables → Import 5 main tables
3.  00_create_users_table  → Create a query to filter the companies that used invoice finance by using the list of companies entitled to issue invoice finance. The query should extract only the first date.
4.  01_CI01_users_not_users_table → Add companies information and the status to all balance sheets available
5.  02_AC01_AC06_users_not_users  → Add lag financial information to the user/not users. A large drop of rows occurs due to the availability of the accounts. Accounts from 2012 onward are available in the FTP
6. 03_financial_information_users_not_users_lookup_tables → Add lookup tables, subset groups of company similar to the users and randomly select 1M rows from the non user to help the algorithm detects a signal.
7. 04_csv_file_reg_to_predict → Replicate step 5/6 using the most recent balance sheets

The notebook 00_ETL_invoice_finance  uses a JSON file to trigger each query. The file is available in GitHub: [parameters_ETL.json](https://github.com/Optimum-Finance/creditsafePrediction/blob/master/03_production/parameters_ETL.json)

The notebook [creditsafedata/NOTEBOOKS_WORKFLOW/00_ETL_invoice_finance.ipynb](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/NOTEBOOKS_WORKFLOW/?region=eu-west-2&tab=overview) will save the json parameters file in the folder [creditsafedata/LOGS_QUERY_EXECUTION/](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/LOGS_QUERY_EXECUTION/?region=eu-west-2&tab=overview)

# 3. Lambda 3

- How: Event trigger

- Objective: Analyse the training table

- type: Jupyter Notebook

- US: 

- - [US 08 Athena tables Model](https://coda.io/d/OptimumFinance_dirgN8QLrCk/US-08-Athena-tables-Model_suUDh)

The notebook [creditsafedata/NOTEBOOKS_WORKFLOW/01_insights_train_table.ipynb](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/NOTEBOOKS_WORKFLOW/?region=eu-west-2&tab=overview) is triggered when the file logs_tables_YYYYMMDD.json  reaches the folder [creditsafedata/LOGS_QUERY_EXECUTION/](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/LOGS_QUERY_EXECUTION/?region=eu-west-2&tab=overview) 

The training table is created in the previous step. A side notebook gives brief analysis to get an idea about the data. It roughly includes:

- - Count observations
  - Count missing observation by variable
  -  Count categorical observations
  -  Distribution continuous variables
  -  Distribution continuous variables, by company type 

The notebook is saved in the folder [creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT/](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT/?region=eu-west-2&tab=overview)

# 4. Lambda 4

- How: Event trigger

- Objective: Train model on randomly selected non user rows and user

- type: Jupyter Notebook

- US:

- - [US 07 Train model monthly data](https://coda.io/d/OptimumFinance_dirgN8QLrCk/US-07-Train-model-monthly-data_su9SN)
  - [US 07 Train model monthly data](https://coda.io/d/OptimumFinance_dirgN8QLrCk/US-07-Train-model-monthly-data_su9SN)

The notebook [creditsafedata/NOTEBOOKS_WORKFLOW/02_train_XGBOOST_algo.ipynb](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/NOTEBOOKS_WORKFLOW/?region=eu-west-2&tab=overview) is triggered when the file logs_tables_YYYYMMDD.json  reaches the folder [creditsafedata/LOGS_QUERY_EXECUTION/](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/LOGS_QUERY_EXECUTION/?region=eu-west-2&tab=overview) 

The training occurs in Sagemaker, where we use our own Docker image from ECR. The training loads the data from Athena, splits the table 90%/10% to have a training and evaluation samples. Two XGBoost are trained on predefined hyperparameters. The first one maximizes the precision and the second one maximizes the recall. 

I found two parameters from the XGBoost algo to tackle imbalance data. One of them tries to improve the prediction when the model makes large mistakes on the positive class (the user). Say differently, the algo attempts to focus on the errors made on the positive class more than the negative one. The second parameters can help to make the update step more conservative, i.e., have an impact on imbalanced data

The models are saved in the S3:

- ALGORITHM/YYYYMMDD/XGBOOST/MODELS/ 

- - [model_precision_1.sav](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/ALGORITHM/20200916/XGBOOST/MODELS/?region=eu-west-2&tab=overview)
  - [model_recall_1.sav](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/ALGORITHM/20200916/XGBOOST/MODELS/?region=eu-west-2&tab=overview)

The notebook is saved in the folder [creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT/](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/NOTEBOOKS_WORKFLOW/OUTPUT/?region=eu-west-2&tab=overview) and the model evaluation is saved in the folder [creditsafedata/ALGORITHM/EVALUATION](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/ALGORITHM/EVALUATION/?region=eu-west-2&tab=overview)

# 5. Lambda 5

- How: Event trigger
- Objective: Predict potential candidates on new balance sheets
- type: Jupyter Notebook
- US:
- [US 01 Users to Predict](https://coda.io/d/OptimumFinance_dirgN8QLrCk/US-01-Users-to-Predict_su7fh)

The notebook [creditsafedata/NOTEBOOKS_WORKFLOW/03_predict_potential_candidates.ipynb](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/NOTEBOOKS_WORKFLOW/?region=eu-west-2&tab=overview) is triggered when the file evaluation_YYYYMMDD.json  reaches the folder [creditsafedata/ALGORITHM/EVALUATION/](https://s3.console.aws.amazon.com/s3/buckets/creditsafedata/ALGORITHM/EVALUATION/?region=eu-west-2&tab=overview) 

At last, we have a final notebook loading the data that contains the latest balance sheet (after 2017), use the two trained models to make a prediction. Three CSV files are generated:

- full_prediction  -> Full dataset with features + predicted probabilities
- full_prediction_proba_75.csv →  Full dataset with features + predicted probabilities above 75% 
- reg_prediction_proba_75.csv → reg number only of predicted probabilities above 75% (same as full_prediction_proba_75 without the features)



| Steps | Name                                                    | Trigger       | time_file_trigger         | Task_type        | S3_key_program                                           | List_source_code                                             | List_US                                                      | List_input                                                   | Input_selection                                              |
| ----- | ------------------------------------------------------- | ------------- | ------------------------- | ---------------- | -------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1     | Extract data from FTP                                   | Time trigger  |                           | Python script    |                                                          | https://github.com/Optimum-Finance/creditsafePrediction/blob/master/04_lambda_function/01_download_from_FTP/handler.py | US 1 FTP                                                     | DATA/UNZIP_DATA_APPEND_ALL/MR01 DATA/UNZIP_DATA_APPEND_ALL/MR02 DATA/UNZIP_DATA_APPEND_ALL/AC01 DATA/UNZIP_DATA_APPEND_ALL/AC06 DATA/UNZIP_DATA_APPEND_ALL/CI01 | DATA/UNZIP_DATA_APPEND_ALL/MR01,DATA/UNZIP_DATA_APPEND_ALL/MR02,DATA/UNZIP_DATA_APPEND_ALL/AC01,DATA/UNZIP_DATA_APPEND_ALL/AC06,DATA/UNZIP_DATA_APPEND_ALL/CI01 |
| 2     | Notebook load and transform tables                      | Event trigger | LOG_UKLTD_M_YYYYMM01.json | Jupyter Notebook | NOTEBOOKS_WORKFLOW/00_ETL_invoice_finance.ipynb          | https://github.com/Optimum-Finance/creditsafePrediction/blob/master/03_production/00_ETL_invoice_finance.md | US 08 Athena tables Model US 08 Athena tables Model US 08 Athena tables Model US 08 Athena tables Model | LUAT01 LUDF01 LUCT01 LUFA01 LUFT01 LULQ01 LUSC01 LUSC02 other_company_types person_entitled_adjusted MR01 AC01 AC06 CI01 MR01 MR02 | LUAT01,LUDF01,LUCT01,LUFA01,LUFT01,LULQ01,LUSC01,LUSC02,other_company_types,person_entitled_adjusted,MR01,AC01,AC06,CI01,MR01,MR02 |
| 3     | Analyse the training table                              | Event trigger | logs_tables_YYYYMMDD.json | Jupyter Notebook | NOTEBOOKS_WORKFLOW/01_insights_train_table.ipynb         | https://github.com/Optimum-Finance/creditsafePrediction/blob/master/03_production/01_insights_train_table.md | US 08 Athena tables Model                                    | financial_information_user_not_user_to_train                 | financial_information_user_not_user_to_train                 |
| 4     | Train model on randomly selected non user rows and user | Event trigger | logs_tables_YYYYMMDD.json | Jupyter Notebook | NOTEBOOKS_WORKFLOW/02_train_XGBOOST_algo.ipynb           | https://github.com/Optimum-Finance/creditsafePrediction/blob/master/03_production/02_train_XGBOOST_algo.md | US 07 Train model monthly data US 07 Train model monthly data | financial_information_user_not_user_to_train                 | financial_information_user_not_user_to_train                 |
| 5     | Predict potential candidates on new balance sheets      | Event trigger | evaluation_YYYYMMDD.json  | Jupyter Notebook | NOTEBOOKS_WORKFLOW/03_predict_potential_candidates.ipynb | https://github.com/Optimum-Finance/creditsafePrediction/blob/master/03_production/03_predict_potential_candidates.md | US 01 Users to Predict                                       | financial_information_reg_to_predict                         | financial_information_reg_to_predict                         |



# Source

-  https://aws.amazon.com/blogs/compute/reducing-custom-code-by-using-advanced-rules-in-amazon-eventbridge/
- https://awscli.amazonaws.com/v2/documentation/api/latest/reference/cloudtrail/create-trail.html
- https://docs.aws.amazon.com/eventbridge/latest/userguide/log-s3-data-events.html
- https://docs.aws.amazon.com/awscloudtrail/latest/userguide/create-s3-bucket-policy-for-cloudtrail.html
- https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-additional-cli-commands.html
- https://docs.aws.amazon.com/awscloudtrail/latest/APIReference/API_DataResource.html