# AWS Lambda Java File Processor

This is project is to process large text files in s3. This is done by a two stage lambda process.

## fileSplitter
This lambda function is triggered on S3 createoObject event. This will check the size of the file and generate split info. It then invokes fileProcessor lambda functions to process each split.

## fileProcessor
This lambda function received a  split info, downloads the byte range of the s3 object and process them.

