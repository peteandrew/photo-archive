import unittest
import boto3
from moto import mock_dynamodb2

from dynamo_access import create_tables, get_images_for_month


@mock_dynamodb2
class TestGetImagesForMonth(unittest.TestCase):
    def setUp(self):
        create_tables()

        client = boto3.client('dynamodb')
        client.put_item(
            TableName='IA_Images',
            Item={
                'ImageID': {
                    'S': 'testid'
                },
                'CreatedYearMonth': {
                    'S': '2019-08'
                },
                'TimeCreated': {
                    'S': '2019-08-23 23:17:00'
                },
                'CurrentFilename': {
                    'S': 'testfile.jpg'
                },
            }
        )

    def test_get_images_for_month(self):
        images = get_images_for_month('2019', '08')
        self.assertEqual(len(images), 1)


if __name__ == '__main__':
    unittest.main()
