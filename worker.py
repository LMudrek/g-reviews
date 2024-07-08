import json
import boto3
import os
from datetime import datetime, timedelta

sqs = boto3.client('sqs')
lambda_client = boto3.client('lambda')
cloudwatch = boto3.client('cloudwatch')

# Environment variables
QUEUE_URL = os.environ['QUEUE_URL']
MAPPING_UUID = os.environ['MAPPING_UUID']
MAX_MESSAGES_PER_HOUR = int(os.environ['MAX_MESSAGES_PER_HOUR'])

def lambda_handler(event, context):
    if 'httpMethod' in event:
        # Event from API Gateway
        enable = json.loads(event['body']).get('enable')
        if enable is None:
            return {
                'statusCode': 400,
                'body': json.dumps('Request body must contain "enable" field')
            }
        return update_batch(enable)
        
    elif 'source' in event and event['source'] == 'aws.events':
        # Event from EventBridge
        return update_batch(True)
    elif 'Records' in event:
        # Event from SQS
        return handle_sqs(event, context)
    else:
        return {
            'statusCode': 400,
            'body': json.dumps('Unsupported event type')
        }

def get_messages_processed(function_name):
    now = datetime.utcnow()
    start_time = now - timedelta(hours=1)
    response = cloudwatch.get_metric_statistics(
        Namespace='Custom/Lambda',
        MetricName='MessagesProcessed',
        Dimensions=[
            {
                'Name': 'FunctionName',
                'Value': function_name
            },
        ],
        StartTime=start_time,
        EndTime=now,
        Period=3600,
        Statistics=['Sum']
    )
    messages_processed_last_hour = response['Datapoints'][0]['Sum'] if response['Datapoints'] else 0
    return messages_processed_last_hour

def put_messages_processed(function_name, value):
    # Update the custom metric in CloudWatch
    cloudwatch.put_metric_data(
        Namespace='Custom/Lambda',
        MetricName='MessagesProcessed',
        Dimensions=[
            {
                'Name': 'FunctionName',
                'Value': function_name
            },
        ],
        Unit='Count',
        Value=value
    )

def rate_limit_achieved(value):    
    return value >= MAX_MESSAGES_PER_HOUR

def disable_batch():
    print("Max messages processed in the last hour. Disabling event source mapping.")
    update_batch(False)
    return {
        'statusCode': 200,
        'body': 'Max messages processed. Event source mapping disabled.'
    }

def enable_batch():
    print("Enabling event source mapping.")
    update_batch(True)
    return {
        'statusCode': 200,
        'body': 'Event source mapping enabled.'
    }

def update_batch(enable):
    # Update the event source mapping to enable or disable it
    action = "enabled" if enable else "disabled"
    response = lambda_client.update_event_source_mapping(
        UUID=MAPPING_UUID,
        Enabled=enable
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Event source mapping {action}')
    }

def handle_sqs(event, context):
    messages_processed_last_hour = get_messages_processed(context.function_name)

    # Calculate how many messages to process
    messages_to_process = min(MAX_MESSAGES_PER_HOUR - messages_processed_last_hour, 10)

    if messages_to_process == 0:
        # If there are no messages to process, disable the batch
        return disable_batch()
    
    # Receive messages from the SQS queue
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=messages_to_process,
        WaitTimeSeconds=10
    )
    
    messages_processed = 0

    if 'Messages' in response:
        for message in response['Messages']:
            # Process the message
            print(f"Processing message: {message['Body']}")
            # Delete the message after processing
            sqs.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=message['ReceiptHandle']
            )

            messages_processed += 1

        # Update the custom metric in CloudWatch
        put_messages_processed(context.function_name, messages_processed)
        
        # Check if we need to disable the batch
        if rate_limit_achieved(get_messages_processed(context.function_name)):
            return disable_batch()
    else:
        print("No messages to process.")
        # If there are no messages and the batch is enabled, disable the batch
        disable_batch()
    
    return {
        'statusCode': 200,
        'body': 'Processed messages from SQS.'
    }
