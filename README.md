# waveform_streaming
streaming data feed QC and wfdb conversion from Phillips Capsule

# Running:
1. convert HL7 files in a 1 hour folder into parquet files
```
   python convert_hl7aecg.py --rootdir {parent folder} --folders {list of child folders, space separated.}
```
2. qc the parquet files
3. convert the parquet files to wfdb files.

# Profiling:
to generate profile, add the ```-m cProfile -o {output}.prof``` flags immediately after ```python```

to visualize profile, use ```snakeviz {output}.prof```

# Dockerization
Reason for creating a docker image instead of going with python code with layers is because of size of layers. 
Layer sizes are limited to 50MB compressed and 250MB uncompressed.

Dockerfile simply does pip install with requirements.txt and invokes lambda handler function.

Building the actual docker image and pushing to ECR is done by the following commands:

1. Creating ECR repository
   ```aws ecr create-repository --repository-name hl7-lambda --region us-east-1```
   ```
   Sample output: 
   
   {
   "repository": {
   "repositoryArn": "arn:aws:ecr:us-east-1:471112573534:repository/hl7-lambda",
   "registryId": "471112573534",
   "repositoryName": "hl7-lambda",
   "repositoryUri": "471112573534.dkr.ecr.us-east-1.amazonaws.com/hl7-lambda",
   "createdAt": "2025-03-25T19:41:57.352000-04:00",
   "imageTagMutability": "MUTABLE",
   "imageScanningConfiguration": {
   "scanOnPush": false
   },
   "encryptionConfiguration": {
   "encryptionType": "AES256"
   }
   }
   }
   ```
2. Building the docker image
   ```docker build -t hl7-lambda .```
3. Tag the image
   ```docker tag hl7-lambda:latest 471112573534.dkr.ecr.us-east-1.amazonaws.com/hl7-lambda:latest```
4. Login to ECR
   ```aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 471112573534.dkr.ecr.us-east-1.amazonaws.com```
5. Push the image to registry
   ```docker push 471112573534.dkr.ecr.us-east-1.amazonaws.com/hl7-lambda:latest```


# TODO
here are some tasks that need to be done to improve performance
1. Generate minute parquets.  This is meant for stream processing only so is separate from the hourly parquets.  The minute parquets will need to be deleted 
2. Parse and store Alarms
3. Parse and store vitals.
4. Collect patient information from vitals internal or PID, alarms PID, and any other source.
5. collect info about bed presence for each hl7 file.
6. downed connection - need to check it too.