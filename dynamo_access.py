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


def get_images_for_month(year, month):
    created_year_month = "{year}-{month}".format(year=year, month=month)
    client = boto3.client('dynamodb')
    response = client.query(
        TableName='IA_Images',
        IndexName='ImagesTimeCreated',
        KeyConditionExpression='CreatedYearMonth = :CreatedYearMonth',
        ExpressionAttributeValues={
            ':CreatedYearMonth': {
                'S': created_year_month
            }
        }
    )
    return response['Items']

