import json
import io
import csv
import boto3

def lambda_handler(event, context):
    # TODO implement
    eventclient = boto3.client('events')
    eventclient.enable_rule(
        Name='MDM_automation_user_access_managment_3_strike_rule'
        )
    #eventbridgeAutomation(eventclient)
    #eventclient.disable_rule(
    #    Name='MDM_automation_user_access_managment_3_strike_rule'
    #    )

def eventbridgeAutomation(eventclient):
    response = eventclient.put_rule(
        Name='MDM_automation_user_access_managment_3_strike_rule',
        ScheduleExpression='cron(0 0 ? * MON-FRI/4 *)',
        State='ENABLED',
        Description='schedule lambda to run the 3-strike rule'
    )
    
    response = eventclient.put_targets(
        Rule='MDM_automation_user_access_managment_3_strike_rule',
        Targets=[
            {
                'Id': 'StartInstance',
                'Arn': 'arn:aws:lambda:us-east-1:266994203134:function:glue_stats',
                'Input': '{"test_key": "test_value"}'
            }
        ]
    )
    
    eventclient.enable_rule(
        Name='MDM_automation_user_access_managment_3_strike_rule'
        )
    
    return {
    'statusCode': 200,
    'body': json.dumps('Hello from Lambda!')
    }
    

def s3_automation():
    data = [{"param1": 1, "param2": 2}, {"param1": 1, "param2": 3}]

    stream = io.StringIO()
    headers = list(data[0].keys())
    writer = csv.DictWriter(stream, fieldnames=headers)
    writer.writeheader()
    writer.writerows(data)
    
    csv_string_object = stream.getvalue()
    
    s3 = boto3.resource("s3")
    s3.Object('automationtest2023', 'test.csv').put(Body=csv_string_object)
    print("write as csv done")
    obj = s3.meta.client.get_object(Bucket = 'automationtest2023', Key = 'test.csv')
    lines = obj['Body'].read().decode("utf-8")
    print(lines)
    
    buf = io.StringIO(lines)
    reader = csv.DictReader(buf)
    rows = list(reader)
    print("response: ")
    print(rows)
    

    

