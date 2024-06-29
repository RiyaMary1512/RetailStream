import json
import boto3
import pymysql

# S3 client
s3_client = boto3.client('s3')

# RDS MySQL database connection details
db_host = 'salesdatadb.c3a8osasabdn.ap-south-1.rds.amazonaws.com'
db_port = 3306  # Default MySQL port
db_user = 'admin'
db_password = 'OpenAI123!456'
db_name = 'salesdatadb'

# Initialize connection variables
connection = None

def get_db_connection():
    global connection
    if connection and connection.open:
        return connection
    connection = pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name,
        port=db_port,
        connect_timeout=5
    )
    return connection

def lambda_handler(event, context):
    # Get the S3 bucket and object key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Download the file from S3
    download_path = f'/tmp/{key}'
    s3_client.download_file(bucket, key, download_path)
    
    # Get the database connection
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cursor:
            # Read the file and insert the data into the MySQL database
            with open(download_path, 'r') as file:
                for line in file:
                    # Assuming CSV format, adjust as necessary
                    data = line.strip().split(',')
                    
                    # Construct the SQL query
                    sql = "INSERT INTO your_table_name (column1, column2, column3, ...) VALUES (%s, %s, %s, ...)"
                    cursor.execute(sql, tuple(data))
        
        # Commit the transaction
        conn.commit()
        
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {e}")
        }
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data inserted successfully!')
    }
