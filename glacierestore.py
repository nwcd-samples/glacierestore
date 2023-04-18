import boto3
import click
import time


def list_objects(s3o, bucket_name, prefix):
    u = dict()
    continuationToken = None
    while True:
        # 列出对象
        if continuationToken:
            response = s3o.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                ContinuationToken=continuationToken
            )
        else:
            response = s3o.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )

        # 如果存在对象，则打印对象的名称
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Size'] == 0:
                    continue
                if obj['StorageClass'] != 'GLACIER':
                    continue
                u[obj['Key']] = True

        # 如果存储桶中的对象未被全部列出，则继续列出对象
        if not response['IsTruncated']:
            break

        continuationToken = response['NextContinuationToken']

    return u.keys()


def restore_object(s3o, bucket_name, object_key, available_days=100):
    # initiate restore request for a single object
    restore_request = {
        # The number of days that you want the restored object to be available
        'Days': available_days,
        'GlacierJobParameters': {
            'Tier': 'Standard'  # The retrieval option for the restored object
        }
    }
    t = s3o.restore_object(
        Bucket=bucket_name,
        Key=object_key,
        RestoreRequest=restore_request
    )


def check_restore_progress(s3o, bucket_name, key):
    response = s3o.head_object(Bucket=bucket_name, Key=key)
    if 'Restore' in response:
        restore_status = response['Restore']
        if restore_status.startswith('ongoing-request'):
            print(f'{key}: Restore in progress, please wait...')
        elif restore_status.startswith('completed'):
            print('Restore already completed:', restore_status)
    else:
        print('Object is not in Glacier storage class.')


@click.command()
@click.option('--bucket_name', prompt='S3 桶的名称', help='S3 桶的名称')
@click.option('--prefix', prompt='文件夹路径,例如：a/b/c ,如果是根目录，则无需输入', help='件夹路径,例如：a/b/c ,如果是根目录，则无需输入', default='')
@click.option('--region', prompt='S3 桶所属的AWS 区域，默认:', default='cn-northwest-1',  help='S3 桶所属的AWS 区域, 默认 cn-northwest-1')
@click.option('--command', prompt='要执行的操作[restore, check],默认 restore, 即提交恢复申请', default='restore',  help='要执行的操作, restore将发起把对象从Glacier恢复至S3的请求，check 将检查对象是否已经被恢复')
def main(bucket_name, prefix, region, command):
    s3 = boto3.client('s3', region_name=region)
    for item in list_objects(s3, bucket_name, prefix):
        if command == 'restore':
            response = s3.head_object(Bucket=bucket_name, Key=item)
            if 'Restore' in response:
                restore_status = response['Restore']
                if restore_status.startswith('ongoing-request'):
                    print(f'{item}: Restore in progress, please wait...')
            else:
                restore_object(s3, bucket_name, item)
                print(f'{item}: Restore have been submitted')
        else:
            check_restore_progress(s3, bucket_name, item)
