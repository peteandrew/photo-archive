import boto3
import botocore
import sqlite3
import sys
import uuid

if len(sys.argv) < 2:
    print('DB filename required')
    sys.exit(1)

db_filename = sys.argv[1]
db = sqlite3.connect(db_filename)

client = boto3.client('dynamodb')

cur = db.execute('SELECT filename, time_created, time_processed, original_directory FROM images')

for row in cur:
    print(row)

    filename = row[0]
    time_created = row[1]

    items = []
    last_evaluated_key = None
    continue_scan = True

    while continue_scan:
        try:
            scan_params = {
                'TableName': 'IA_Images',
                'FilterExpression': "CurrentFilename = :CurrentFilename AND TimeCreated = :TimeCreated",
                'ExpressionAttributeValues': {
                    ":CurrentFilename": { 'S': filename },
                    ":TimeCreated": { 'S': time_created }
                },
            }

            if last_evaluated_key:
                scan_params['ExclusiveStartKey'] = last_evaluated_key

            response = client.scan(**scan_params)
            items += response['Items']

            if 'LastEvaluatedKey' in response:
                last_evaluated_key = response['LastEvaluatedKey']
            else:
                continue_scan = False

        except botocore.exceptions.ClientError as e:
            print(e)

    if len(items) > 1:
        print(len(items))
        for item in items:
            if 'GlacierState' not in item:
                client.delete_item(
                    TableName='IA_Images',
                    Key={
                        'ImageID': item['ImageID']
                    }
                )

cur.close()
