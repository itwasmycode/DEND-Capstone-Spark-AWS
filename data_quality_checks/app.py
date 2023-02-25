import boto3

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

def lambda_handler(event, context):
    # Replace the values below with your own table and S3 bucket names
    table_name = 'orders'
    bucket_name = 'mysparkawsbucket'
    file_name = 'row_count.txt'
    
    # Get the row count for the table
    response = glue_client.get_table(
        DatabaseName='your_database_name',
        Name=table_name
    )
    row_count = response['Table']['Parameters']['numRows']
    
    # Log the row count to S3
    s3_client.put_object(
        Body=row_count.encode('utf-8'),
        Bucket=bucket_name,
        Key=file_name
    )
    
    print(f"Row count for table {table_name}: {row_count}")