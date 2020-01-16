# create the new Serverless package named 'first_sls_app'
sam init --runtime python3.7 --name first_sls_app

cd first_sls_app
ls

# make a new directory
# put the python code in the directory
# update the template.yaml file, as demo'd here: https://www.youtube.com/watch?v=bih5b3C1nqc

# in the first_sls_app directory run the following commands to test the cloud formation function locally
sudo service docker start
sam local invoke <function_name> --no-event

#once happy with the test output, package the function for deployment
sam package --template-file template.yaml --output-template-file deploy.yaml --s3-bucket <s3_bucket_eg._bshelt-web-scraping>

#once packages, deploy:
sam deploy --template-file deploy.yaml --stack-name MyPrintNameLambdaStack --capabilities CAPABILITY_IAM
