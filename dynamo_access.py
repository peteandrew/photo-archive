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

