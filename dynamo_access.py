from dateutil.rrule import rrule, MONTHLY
import boto3

def create_tables():
    client = boto3.client('dynamodb')

    client.create_table(
        TableName="IA_Images",
        BillingMode="PAY_PER_REQUEST",
        KeySchema=[
            {
                "AttributeName": "ImageID",
                "KeyType": "HASH"
            }
        ],
        AttributeDefinitions=[
            {
                "AttributeName": "ImageID",
                "AttributeType": "S"
            },
            {
                "AttributeName": "CreatedYearMonth",
                "AttributeType": "S"
            },
            {
                "AttributeName": "TimeCreated",
                "AttributeType": "S"
            }
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "ImagesTimeCreated",
                "KeySchema": [
                    {
                        "AttributeName": "CreatedYearMonth",
                        "KeyType": "HASH"
                    },
                    {
                        "AttributeName": "TimeCreated",
                        "KeyType": "RANGE"
                    }
                ],
                "Projection": {
                    "ProjectionType": "INCLUDE",
                    "NonKeyAttributes": [
                        "ImageID"
                    ]
                }
            }
        ]
    )

    client.create_table(
        TableName="IA_TagsImages",
        BillingMode="PAY_PER_REQUEST",
        KeySchema=[
            {
                "AttributeName": "Tag",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "ImageID",
                "KeyType": "RANGE"
            }
        ],
        AttributeDefinitions=[
            {
                "AttributeName": "Tag",
                "AttributeType": "S"
            },
            {
                "AttributeName": "ImageID",
                "AttributeType": "S"
            }
        ]
    )


def find_images(from_datetime, to_datetime):
    dt_recurrences = rrule(MONTHLY, dtstart=from_datetime, until=to_datetime)

    client = boto3.client('dynamodb')

    images = []

    for dt in dt_recurrences:
        created_year_month = str(dt)[:7]
        response = client.query(
            TableName='IA_Images',
            IndexName='ImagesTimeCreated',
            KeyConditionExpression='CreatedYearMonth = :CreatedYearMonth AND TimeCreated BETWEEN :FromDateTime AND :ToDateTime',
            ExpressionAttributeValues={
                ':CreatedYearMonth': {
                    'S': created_year_month
                },
                ':FromDateTime': {
                    'S': str(from_datetime)
                },
                ':ToDateTime': {
                    'S': str(to_datetime)
                }
            }
        )
        images += [{'image_id': image['ImageID']['S'], 'created_datetime': image['TimeCreated']['S']} for image in response['Items']]

    return images


def image_details(image_id):
    client = boto3.client('dynamodb')

    response = client.query(
        TableName='IA_Images',
        KeyConditionExpression='ImageID = :ImageID',
        ExpressionAttributeValues={
            ':ImageID': {
                'S': image_id
            }
        }
    )

    if len(response['Items']) == 0:
        return None

    image = response['Items'][0]
    image_details = {
        'image_id': image['ImageID']['S'],
        'processed_datetime': image['TimeProcessed']['S'],
        'created_datetime': image['TimeCreated']['S']
    }
    if 'CurrentFilename' in image:
        image_details['current_filename'] = image['CurrentFilename']['S']
    if 'OriginalDirectory' in image:
        image_details['original_directory'] = image['OriginalDirectory']['S']
    if 'GlacierState' in image:
        image_details['glacier_state'] = image['GlacierState']['S']

    return image_details


def update_image_glacier_state(image_id, new_glacier_state):
    client = boto3.client('dynamodb')

    client.update_item(
        TableName='IA_Images',
        UpdateExpression='SET GlacierState = :NewState',
        ExpressionAttributeValues={
            ':NewState': {
                'S': new_glacier_state
            }
        },
        Key={
            'ImageID': {
                'S': image_id
            }
        }
    )
