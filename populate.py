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

    image_id = str(uuid.uuid4())
    created_year_month = row[1][:7]

    try:
        response = client.put_item(
            TableName='IA_Images',
            Item={
                'ImageID': {
                    'S': image_id
                },
                'CreatedYearMonth': {
                    'S': created_year_month
                },
                'TimeCreated': {
                    'S': row[1]
                },
                'CurrentFilename': {
                    'S': row[0]
                },
                'TimeProcessed': {
                    'S': row[2]
                },
                'OriginalDirectory': {
                    'S': row[3]
                }
            }
        )
        print(response)
    except botocore.exceptions.ClientError as e:
        print(e)

cur.close()
