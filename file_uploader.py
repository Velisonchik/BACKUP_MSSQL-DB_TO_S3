from datetime import datetime
from minio import Minio
from DB_req import *


def connect_minio():
    return Minio(
        minio_server,
        access_key=access_key_s3,
        secret_key=secret_key_s3,
    )


def write_log(text):
    with open('log.log', 'a') as log:
        current_datetime = datetime.now()
        log.write(f'{current_datetime} - {text}\n')


def upload_bak(bak_file_name, bak_file_path):
    try:
        client = connect_minio()
        client.fput_object("rdm-database", bak_file_name, bak_file_path)
        write_log('Backup upload to S3 successfully')
    except Exception as e:
        write_log(e)
    finally:
        client.close()


def del_obj(bucket_name, obj_name):
    try:
        client = connect_minio()
        client.remove_object(bucket_name, obj_name)
        client.close()
    except Exception as e:
        write_log(e)
    finally:
        client.close()
