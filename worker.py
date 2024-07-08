import boto3
import os
from datetime import datetime, timedelta

sqs = boto3.client('sqs')
cloudwatch = boto3.client('cloudwatch')

# Environment variables
QUEUE_URL = os.environ['QUEUE_URL']
MAX_MESSAGES_PER_HOUR = int(os.environ['MAX_MESSAGES_PER_HOUR'])

def lambda_handler(event, context):
    now = datetime.utcnow()
    start_time = now - timedelta(hours=1)

    # Get the number of messages processed in the last hour from CloudWatch
    response = cloudwatch.get_metric_statistics(
        Namespace='Custom/Lambda',
        MetricName='MessagesProcessed',
        Dimensions=[
            {
                'Name': 'FunctionName',
                'Value': context.function_name
            },
        ],
        StartTime=start_time,
        EndTime=now,
        Period=3600,
        Statistics=['Sum']
    )
    
    messages_processed_last_hour = response['Datapoints'][0]['Sum'] if response['Datapoints'] else 0

    if messages_processed_last_hour >= MAX_MESSAGES_PER_HOUR:
        print("Max messages processed in the last hour. Exiting.")
        return

    # Calculate how many messages to process
    messages_to_process = min(MAX_MESSAGES_PER_HOUR - messages_processed_last_hour, 10)

    # Receive messages from the SQS queue
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=messages_to_process,
        WaitTimeSeconds=10
    )
    
    if 'Messages' in response:
        for message in response['Messages']:
            # Process the message
            print(f"Processing message: {message['Body']}")
            # Delete the message after processing
            sqs.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=message['ReceiptHandle']
            )
        
        # Update the custom metric in CloudWatch
        cloudwatch.put_metric_data(
            Namespace='Custom/Lambda',
            MetricName='MessagesProcessed',
            Dimensions=[
                {
                    'Name': 'FunctionName',
                    'Value': context.function_name
                },
            ],
            Unit='Count',
            Value=len(response['Messages'])
        )
    else:
        print("No messages to process.")
