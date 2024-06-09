import json
import boto3

sns_client = boto3.client('sns')
TARGET_ARN = 'arn:aws:sns:us-east-1:211125329152:ingestionsns'

def lambda_handler(event, context):
    try:
        job_name = event['detail']['jobName']
        state = event['detail']['state']
        error_message = event['detail'].get('errorMessage', 'No error message provided')

        message = (
            f"Glue Job Failure Notification\n"
            f"Job Name: {job_name}\n"
            f"State: {state}\n"
            f"Error Message: {error_message}\n"
            f"Details: {json.dumps(event, indent=2)}"
        )

        # Send the message to SNS
        response = sns_client.publish(
            TopicArn=TARGET_ARN,
            Message=message,
            Subject='CRITICAL!!! Glue Job Failed'
        )
        print(f"Message sent to SNS topic {TARGET_ARN}: {response['MessageId']}")

    except Exception as e:
        print(f"Error processing Glue job failure event: {str(e)}")
        raise e
