import boto3
import time

TARGET_ARN = 'arn:aws:sns:us-east-1:211125329152:ingestionsns'
failure_message = 'CRITICAL!!! ETL JOB FAILED'
success_message = 'SUCCESS!!! ETL JOB COMPLETE'
sns_client = boto3.client('sns')


def lambda_handler(event, context):
    try:
        glue_client = boto3.client('glue')
        response = glue_client.start_job_run(
            JobName="ETL-PYSPARK-JOB",
            WorkerType="G.1X",
            NumberOfWorkers=2,
            ExecutionClass="STANDARD",
        )
        job_run_id = response['JobRunId']
        print(f'Job run started. JobRunId: {job_run_id}')

        # Wait for the job to complete
        while True:
            job_status = glue_client.get_job_run(JobName="ETL-PYSPARK-JOB", RunId=job_run_id)
            state = job_status['JobRun']['JobRunState']
            print(f'Current job state: {state}')

            if state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                break

            time.sleep(30)  # Wait for 30 seconds before checking again

        # Publish SNS message based on the job status
        if state == 'SUCCEEDED':
            sns_client.publish(
                TopicArn=TARGET_ARN,
                Message=success_message,
                Subject='INFO: Glue Job Succeeded'
            )
        else:
            sns_client.publish(
                TopicArn=TARGET_ARN,
                Message=failure_message,
                Subject='CRITICAL!!! Glue Job Failed'
            )

    except Exception as e:
        print(e)
        print('Job run failed.')

        sns_client.publish(
            TopicArn=TARGET_ARN,
            Message=failure_message,
            Subject='CRITICAL!!! Glue Job Failed'
        )


if __name__ == '__main__':
    lambda_handler({}, {})
