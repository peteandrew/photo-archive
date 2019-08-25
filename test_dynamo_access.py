import unittest
from datetime import datetime

import boto3
from moto import mock_dynamodb2

from dynamo_access import create_tables, find_images


@mock_dynamodb2
class TestFindImages(unittest.TestCase):
    def setUp(self):
        create_tables()

        client = boto3.client('dynamodb')
        client.put_item(
            TableName='IA_Images',
            Item={
                'ImageID': {
                    'S': 'image1'
                },
                'CreatedYearMonth': {
                    'S': '2019-07'
                },
                'TimeCreated': {
                    'S': '2019-07-01 10:10:00'
                },
                'CurrentFilename': {
                    'S': 'image1.jpg'
                },
            }
        )
        client.put_item(
            TableName='IA_Images',
            Item={
                'ImageID': {
                    'S': 'image2'
                },
                'CreatedYearMonth': {
                    'S': '2019-08'
                },
                'TimeCreated': {
                    'S': '2019-08-01 15:00:00'
                },
                'CurrentFilename': {
                    'S': 'image2.jpg'
                },
            }
        )
        client.put_item(
            TableName='IA_Images',
            Item={
                'ImageID': {
                    'S': 'image3'
                },
                'CreatedYearMonth': {
                    'S': '2019-08'
                },
                'TimeCreated': {
                    'S': '2019-08-10 20:00:00'
                },
                'CurrentFilename': {
                    'S': 'image3.jpg'
                },
            }
        )

    def tearDown(self):
        client = boto3.client('dynamodb')
        client.delete_table(TableName="IA_Images")
        client.delete_table(TableName="IA_TagsImages")

    def test_find_images_for_month(self):
        images = find_images(datetime(2019, 8, 1), datetime(2019, 8, 31))
        self.assertEqual(len(images), 2)
        image_ids = [image['ImageID']['S'] for image in images]
        self.assertTrue(set(image_ids) == set(['image2', 'image3']))

    def test_find_images_for_multiple_months(self):
        images = find_images(datetime(2019, 7, 1), datetime(2019, 8, 31))
        self.assertEqual(len(images), 3)
        image_ids = [image['ImageID']['S'] for image in images]
        self.assertTrue(set(image_ids) == set(['image1', 'image2', 'image3']))

    def test_find_no_images_for_month(self):
        images = find_images(datetime(2019, 1, 1), datetime(2019, 1, 31))
        self.assertEqual(len(images), 0)

    def test_find_images_for_day(self):
        images = find_images(datetime(2019, 8, 1, 0, 0, 1), datetime(2019, 8, 1, 23, 59, 59))
        self.assertEqual(len(images), 1)
        self.assertEqual(images[0]['ImageID']['S'], 'image2')

if __name__ == '__main__':
    unittest.main()
