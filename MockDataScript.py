import json
import random
import string
import boto3
from datetime import datetime, date

sns_client = boto3.client('sns')
target_arn = 'arn:aws:sns:us-east-1:211125329152:ingestionsns'
ETL_FAILURE_MESSAGE = 'ETL LOAD HAS FAILED, PLEASE CHECK CLOUDWATCH LOGS'
s3_directory= "bankingrawtransactions"

def lambda_handler(event, context):
    try:
        def generate_transaction_id():
            return 'T' + ''.join(random.choices(string.digits, k=6))

        def generate_account_id():
            return 'A' + ''.join(random.choices(string.digits, k=5))

        def generate_branch_id():
            return 'B' + str(random.randint(1, 10))

        def generate_transaction_date():
            start_date = datetime(2020, 1, 1)
            end_date = datetime.now()
            random_date = start_date + (end_date - start_date) * random.random()
            return random_date.strftime('%Y-%m-%d')

        def generate_transaction_type():
            return random.choice(['debit', 'credit'])

        def generate_description(transaction_type):
            if transaction_type == 'debit':
                return random.choice(['ATM Withdrawal', 'Online Purchase', 'Store Purchase'])
            else:
                return random.choice(['Salary Deposit', 'Bank Transfer', 'Interest Payment'])

        num_records = random.randint(1000, 2000)  # Adjust as needed

        def generate_mock_data(total_records):
            transactions = []
            for _ in range(total_records):
                transaction_type = generate_transaction_type()
                transaction = {
                    "transaction_id": generate_transaction_id(),
                    "account_id": generate_account_id(),
                    "transaction_date": generate_transaction_date(),
                    "amount": round(random.uniform(10.0, 1000.0), 2),
                    "transaction_type": transaction_type,
                    "branch_id": generate_branch_id(),
                    "description": generate_description(transaction_type)
                }
                transactions.append(transaction)
            return transactions

        mock_data = generate_mock_data(num_records)

        file_content = '\n'.join(json.dumps(transaction) for transaction in mock_data)
        print(file_content)
        current_date_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        current_day= date.today()
        bucket_name = 'telecomglue'
        file_name = f'{s3_directory}/{current_day}/banking_transactions_{current_date_time}.json'

        s3_client = boto3.client('s3')
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=file_content)
        print(f'{s3_directory}/{current_day}/Uploaded {file_name} to S3 bucket {bucket_name}')

        # Verify that the object was written to S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        if response['Body'].read().decode('utf-8') == file_content:
            etl_message = 'INFO-INCREMENTAL LOAD HAS BEEN COMPLETED IN PRODUCTION'
            sns_client.publish(
                TopicArn=target_arn,
                Message=etl_message,
                Subject='INFO!!! INCREMENTAL LOAD HAS BEEN COMPLETED IN PRODUCTION'
            )
        else:
            raise Exception('Verification of uploaded file content failed.')

    except Exception as e:
        print(f'Lambda Function Failed: {str(e)}')
        sns_client.publish(
            TopicArn=target_arn,
            Message=ETL_FAILURE_MESSAGE,
            Subject='CRITICAL!!! INCREMENTAL LOAD HAS FAILED IN PRODUCTION'
        )


if __name__ == '__main__':
    lambda_handler({}, {})
