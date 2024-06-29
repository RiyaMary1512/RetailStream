import boto3
from pathlib import Path

# Initialize S3 client
s3 = boto3.client('s3')

# Define the local directory and S3 bucket
local_directory = Path('D:\Data Set\Sales_Dataset')
s3_bucket = 'org-sales-data'

# Function to upload files to S3
def upload_files(local_directory, s3_bucket):
    # Iterate over all CSV files in the directory
    for file_path in local_directory.glob('*.csv'):
        # Print the upload process
        print(f'Uploading {file_path} to s3://{s3_bucket}/{file_path.name}')
        
        # Upload the file to S3
        s3.upload_file(str(file_path), s3_bucket, file_path.name)
        print(f'{file_path.name} has been uploaded.')

# Call the function to upload files
upload_files(local_directory, s3_bucket)
