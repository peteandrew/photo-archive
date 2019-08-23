import boto3
from moto import mock_dynamodb2
from dynamo_access import get_images_for_month


@mock_dynamodb2
def test_get_images_for_month():
    client = boto3.client('dynamodb')
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



        conn = boto3.resource('s3', region_name='us-east-1')
            # We need to create the bucket since this is all in Moto's 'virtual' AWS account
                conn.create_bucket(Bucket='mybucket')

                    model_instance = MyModel('steve', 'is awesome')
                        model_instance.save()

                            body = conn.Object('mybucket', 'steve').get()['Body'].read().decode("utf-8")

                                assert body == 'is awesome'


def get_images_for_month(year, month):

