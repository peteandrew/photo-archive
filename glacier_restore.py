from datetime import datetime

import boto3
import botocore

from dynamo_access import find_images, image_details, update_image_glacier_state

from_datetime = datetime(2000, 1, 1, 0, 0, 0)
to_datetime = datetime(2019, 12, 31, 23, 59, 59)

s3_client = boto3.client('s3')


def copy_image(old_key, image, curr_image_details):
    new_key = 'originals/{year}/{month}/{imageid}.jpg'.format(
        year=curr_image_details['created_datetime'][:4],
        month=curr_image_details['created_datetime'][5:7],
        imageid=curr_image_details['image_id']
    )
        
    s3_client.copy_object(
        Bucket='peteandrew-photoarchive',
        Key=new_key,
        CopySource='{bucket}/{key}'.format(bucket='peteandrew-photobackups', key=key)
    )

    update_image_glacier_state(image['image_id'], 'restored')


images = find_images(from_datetime, to_datetime)
for image in images:
    curr_image_details = image_details(image['image_id'])
    print(curr_image_details)

    key = '{year}/{month}/{filename}'.format(
        year=curr_image_details['created_datetime'][:4],
        month=curr_image_details['created_datetime'][5:7],
        filename=curr_image_details['current_filename']
    )

    if 'glacier_state' not in curr_image_details:
        try:
            s3_client.restore_object(
                Bucket='peteandrew-photobackups',
                Key=key,
                RestoreRequest={
                    'Days': 21,
                    'GlacierJobParameters': {
                        'Tier': 'Standard'
                    }
                }
            )
        except botocore.exceptions.ClientError as e:
            if 'RestoreAlreadyInProgress' in str(e):
                pass

        update_image_glacier_state(image['image_id'], 'restoring')

    else:
        print(curr_image_details['image_id'], curr_image_details['glacier_state'])

        if curr_image_details['glacier_state'] == 'restoring':
            response = s3_client.head_object(
                Bucket='peteandrew-photobackups',
                Key=key
            )

            if 'StorageClass' not in response and 'Restore' not in response:
                copy_image(key, image, curr_image_details)
                continue

            if response['Restore'].startswith('ongoing-request="false"'):
                copy_image(key, image, curr_image_details)
