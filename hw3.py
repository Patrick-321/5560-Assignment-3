# assignment3
import boto3
from botocore.exceptions import ClientError

# Initialize boto3 clients
s3_client = boto3.client('s3')
dynamodb_resource = boto3.resource('dynamodb')

def create_s3_bucket(bucket_name):
    """
    Create an S3 bucket in the current AWS region.

    Parameters:
        bucket_name (str): The name of the bucket to create.
    """
    try:
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': boto3.session.Session().region_name
            }
        )
        print(f"S3 bucket '{bucket_name}' created successfully.")
    except ClientError as e:
        print(f"Error creating S3 bucket: {e}")

def create_dynamodb_table(table_name):
    """
    Create a DynamoDB table with a composite primary key.

    Parameters:
        table_name (str): The name of the table to create.
    """
    try:
        table = dynamodb_resource.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'BucketName', 'KeyType': 'HASH'},  # Partition key
                {'AttributeName': 'Timestamp', 'KeyType': 'RANGE'}  # Sort key
            ],
            AttributeDefinitions=[
                {'AttributeName': 'BucketName', 'AttributeType': 'S'},
                {'AttributeName': 'Timestamp', 'AttributeType': 'N'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print(f"DynamoDB table '{table_name}' created successfully.")
    except ClientError as e:
        print(f"Error creating DynamoDB table: {e}")

def main():
    """
    Main function to create S3 bucket and DynamoDB table.
    """
    bucket_name = 'ziweizhou_hw3'
    table_name = 'S3_object_size_history'

    # Create S3 bucket
    create_s3_bucket(bucket_name)

    # Create DynamoDB table
    create_dynamodb_table(table_name)

if __name__ == "__main__":
    main()


# size_function
import boto3
from datetime import datetime

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Configuration
BUCKET_NAME = 'ziweizhou_hw3'
TABLE_NAME = 'S3_object_size_history'

def calculate_bucket_size():
    """
    Calculate the total size and object count of the specified S3 bucket.

    Returns:
        tuple: Total size of the bucket (int) and object count (int).
    """
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    total_size = 0
    object_count = 0

    if 'Contents' in response:
        # Iterate through objects to calculate total size and count
        for obj in response['Contents']:
            total_size += obj['Size']
            object_count += 1

    return total_size, object_count

def write_size_to_dynamodb(total_size, object_count):
    """
    Write the bucket size and object count to the DynamoDB table.

    Parameters:
        total_size (int): The total size of the bucket in bytes.
        object_count (int): The number of objects in the bucket.
    """
    table = dynamodb.Table(TABLE_NAME)
    timestamp = int(datetime.utcnow().timestamp())
    timestamp_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Put item into DynamoDB
    table.put_item(
        Item={
            'BucketName': BUCKET_NAME,
            'Timestamp': timestamp,
            'TimestampStr': timestamp_str,
            'TotalSize': total_size,
            'ObjectCount': object_count
        }
    )
    print(f"Data written to DynamoDB: BucketName={BUCKET_NAME}, TotalSize={total_size}, ObjectCount={object_count}")

def main():
    """
    Main function to calculate bucket size and write results to DynamoDB.
    """
    total_size, object_count = calculate_bucket_size()
    write_size_to_dynamodb(total_size, object_count)
    print("Bucket size and object count updated in DynamoDB.")

if __name__ == "__main__":
    main()


# driver function
import boto3
import time
import urllib3

# Initialize boto3 clients
s3_client = boto3.client('s3')

# Configuration
BUCKET_NAME = 'ziweizhou_hw3'
PLOTTING_API_URL = 'https://mldfcj19u3.execute-api.us-west-1.amazonaws.com/default/plotting_function'

def create_object(object_name, content):
    """
    Create an object in the specified S3 bucket.

    Parameters:
        object_name (str): The name of the object to create.
        content (str): The content to store in the object.
    """
    s3_client.put_object(Bucket=BUCKET_NAME, Key=object_name, Body=content)
    print(f"Object '{object_name}' created with content: {content}")

def update_object(object_name, content):
    """
    Update an object in the specified S3 bucket.

    Parameters:
        object_name (str): The name of the object to update.
        content (str): The new content for the object.
    """
    s3_client.put_object(Bucket=BUCKET_NAME, Key=object_name, Body=content)
    print(f"Object '{object_name}' updated with content: {content}")

def delete_object(object_name):
    """
    Delete an object from the specified S3 bucket.

    Parameters:
        object_name (str): The name of the object to delete.
    """
    s3_client.delete_object(Bucket=BUCKET_NAME, Key=object_name)
    print(f"Object '{object_name}' deleted.")

def call_plotting_api():
    """
    Invoke the plotting API to trigger a Lambda function.
    """
    http = urllib3.PoolManager()
    response = http.request('POST', PLOTTING_API_URL)
    print(f"Plotting API invoked with status: {response.status}")

def lambda_handler(event, context):
    """
    Main handler for the Driver Lambda function.

    Parameters:
        event (dict): Event data passed to the Lambda function.
        context (object): Lambda context object.

    Returns:
        dict: Response indicating the status of the Lambda execution.
    """
    create_object('assignment1.txt', 'Empty Assignment 1')
    time.sleep(2)

    update_object('assignment1.txt', 'Empty Assignment 2')
    time.sleep(2)

    delete_object('assignment1.txt')
    time.sleep(2)

    create_object('assignment2.txt', '21')
    time.sleep(2)

    call_plotting_api()

    return {
        'statusCode': 200,
        'body': 'Driver Lambda executed successfully and invoked Plotting API.'
    }

# plotting function
import boto3
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# Configuration
TABLE_NAME = 'S3_object_size_history'
BUCKET_NAME = 'ziweizhou_hw3'
PLOT_BUCKET = 'plotbucket1'

def query_size_history():
    """
    Query the DynamoDB table for bucket size data in the last 10 seconds.

    Returns:
        list: List of size data entries from DynamoDB.
    """
    table = dynamodb.Table(TABLE_NAME)
    now = datetime.utcnow()
    ten_seconds_ago = int((now - timedelta(seconds=10)).timestamp())

    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('BucketName').eq(BUCKET_NAME) &
                               boto3.dynamodb.conditions.Key('Timestamp').between(ten_seconds_ago, int(now.timestamp()))
    )
    return response.get('Items', [])

def get_max_size():
    """
    Retrieve the maximum size recorded in the DynamoDB table for the bucket.

    Returns:
        int: The maximum size of the bucket, or 0 if no data is available.
    """
    table = dynamodb.Table(TABLE_NAME)
    response = table.scan(
        ProjectionExpression='TotalSize',
        FilterExpression=boto3.dynamodb.conditions.Key('BucketName').eq(BUCKET_NAME)
    )
    if not response.get('Items'):
        return 0

    return max(item['TotalSize'] for item in response['Items'])

def plot_size_history(size_data, max_size):
    """
    Create a plot of bucket size history with the maximum size indicated.

    Parameters:
        size_data (list): List of size data entries from DynamoDB.
        max_size (int): The maximum size recorded.
    """
    timestamps = [item['TimestampStr'] for item in size_data]
    sizes = [item['TotalSize'] for item in size_data]

    plt.figure(figsize=(10, 6))
    plt.plot(timestamps, sizes, label='Bucket Size', marker='o')
    plt.axhline(y=max_size, color='r', linestyle='--', label='Max Size')
    plt.xlabel('Timestamp')
    plt.ylabel('Size (Bytes)')
    plt.xticks(rotation=45)
    plt.title('Bucket Size Over Time')
    plt.tight_layout()
    plt.legend()

    plt.savefig('/tmp/plot.png')

def upload_plot_to_s3():
    """
    Upload the generated plot to an S3 bucket.
    """
    with open('/tmp/plot.png', 'rb') as plot_file:
        s3_client.put_object(Bucket=PLOT_BUCKET, Key='plot.png', Body=plot_file)
    print(f"Plot uploaded to S3 bucket '{PLOT_BUCKET}' as 'plot.png'.")

def lambda_handler(event, context):
    """
    AWS Lambda function handler for querying bucket size data, generating a plot, and uploading it to S3.

    Parameters:
        event (dict): Event data passed to the Lambda function.
        context (object): Lambda context object.

    Returns:
        dict: Response indicating the status of the Lambda execution.
    """
    size_data = query_size_history()
    max_size = get_max_size()

    plot_size_history(size_data, max_size)
    upload_plot_to_s3()

    return {
        'statusCode': 200,
        'body': 'Plotting Lambda executed successfully, plot uploaded to S3.'
    }
